// Copyright 2025 RISC Zero, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use crate::order_monitor::OrderMonitorErr;
use alloy::{
    network::Ethereum,
    primitives::{Address, B256},
    providers::Provider,
    sol,
    sol_types::SolCall,
};

use alloy::rpc::types::Transaction;
use alloy::consensus::Transaction as _;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use boundless_market::{
    contracts::{
        boundless_market::BoundlessMarketService, IBoundlessMarket,
    },
};
use futures_util::{StreamExt, SinkExt};
use tokio_util::sync::CancellationToken;
use crate::{chain_monitor::ChainMonitorService, db::DbObj, errors::{impl_coded_debug, CodedError}, task::{RetryRes, RetryTask, SupervisorErr},
            FulfillmentType, OrderRequest, storage::{upload_image_uri, upload_input_uri}};

use thiserror::Error;
use crate::config::ConfigLock;
use crate::provers::ProverObj;

use serde::{Deserialize, Serialize};

const BLOCK_TIME_SAMPLE_SIZE: u64 = 10;

#[derive(Error)]
pub enum MarketMonitorErr {
    #[error("{code} Mempool polling failed: {0:?}", code = self.code())]
    MempoolPollingErr(anyhow::Error),

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedErr(#[from] anyhow::Error),
}

impl CodedError for MarketMonitorErr {
    fn code(&self) -> &str {
        match self {
            MarketMonitorErr::MempoolPollingErr(_) => "[B-MM-501]",
            MarketMonitorErr::UnexpectedErr(_) => "[B-MM-500]",
        }
    }
}

impl_coded_debug!(MarketMonitorErr);

#[derive(Deserialize, Serialize)]
struct LockTransactionRequest {
    tx_hash: String,
    lock_block: u64,
    input_hex: String,
}

#[derive(Serialize)]
struct ApiResponse {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct CommittedOrdersResponse {
    count: usize,
}

pub struct MarketMonitor<P> {
    market_addr: Address,
    provider: Arc<P>,
    config: ConfigLock,
    db_obj: DbObj,
    prover_addr: Address,
    boundless_service: BoundlessMarketService<Arc<P>>,
    prover: ProverObj
}

impl<P> MarketMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone + Send + Sync,
{
    pub fn new(
        market_addr: Address,
        provider: Arc<P>,
        config: ConfigLock,
        db_obj: DbObj,
        prover_addr: Address,
        prover: ProverObj
    ) -> Self {
        let boundless_service = BoundlessMarketService::new(market_addr, provider.clone(), prover_addr);
        Self {
            market_addr,
            provider,
            config,
            db_obj,
            prover_addr,
            boundless_service,
            prover
        }
    }

    // Basit HTTP server - axum kullanmak yerine manuel TCP
    async fn run_simple_http_server(
        market_addr: Address,
        provider: Arc<P>,
        config: ConfigLock,
        db_obj: DbObj,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        use std::net::SocketAddr;
        use tokio::net::TcpListener;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        tracing::info!("🌐 Starting simple HTTP server on port 3001");

        let addr: SocketAddr = "0.0.0.0:3001".parse().unwrap();
        let listener = TcpListener::bind(addr).await?;

        tracing::info!("✅ HTTP server listening on http://{}", addr);
        tracing::info!("📝 Available endpoints:");
        tracing::info!("   POST /api/lock-transaction - Process lock transaction");
        tracing::info!("   GET  /api/committed-orders-count - Get orders count");
        tracing::info!("   GET  /health - Health check");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((mut socket, _)) => {
                            let market_addr = market_addr;
                            let provider = provider.clone();
                            let config = config.clone();
                            let db_obj = db_obj.clone();

                            tokio::spawn(async move {
                                let mut buffer = [0; 4096];
                                match socket.read(&mut buffer).await {
                                    Ok(n) if n > 0 => {
                                        let request = String::from_utf8_lossy(&buffer[..n]);
                                        let response = Self::handle_http_request(
                                            &request,
                                            market_addr,
                                            provider,
                                            config,
                                            db_obj
                                        ).await;

                                        if let Err(e) = socket.write_all(response.as_bytes()).await {
                                            tracing::warn!("Failed to write response: {}", e);
                                        }
                                    }
                                    Ok(_) => {
                                        tracing::debug!("Empty request received");
                                    }
                                    Err(e) => {
                                        tracing::warn!("Failed to read from socket: {}", e);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            tracing::warn!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    tracing::info!("HTTP server cancelled");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_http_request(
        request: &str,
        market_addr: Address,
        provider: Arc<P>,
        config: ConfigLock,
        db_obj: DbObj,
    ) -> String {
        let lines: Vec<&str> = request.lines().collect();
        if lines.is_empty() {
            return Self::http_response(400, "Bad Request", "");
        }

        let request_line = lines[0];
        let parts: Vec<&str> = request_line.split_whitespace().collect();
        if parts.len() < 2 {
            return Self::http_response(400, "Bad Request", "");
        }

        let method = parts[0];
        let path = parts[1];

        match (method, path) {
            ("GET", "/health") => {
                let body = serde_json::json!({
                    "status": "ok",
                    "service": "market-monitor",
                    "timestamp": chrono::Utc::now().to_rfc3339()
                });
                Self::http_response(200, "OK", &body.to_string())
            }
            ("GET", "/api/committed-orders-count") => {
                match db_obj.get_committed_orders().await {
                    Ok(orders) => {
                        let body = CommittedOrdersResponse { count: orders.len() };
                        Self::http_response(200, "OK", &serde_json::to_string(&body).unwrap_or_default())
                    }
                    Err(e) => {
                        let body = ApiResponse {
                            success: false,
                            message: None,
                            error: Some(format!("Database error: {}", e)),
                        };
                        Self::http_response(500, "Internal Server Error", &serde_json::to_string(&body).unwrap_or_default())
                    }
                }
            }
            ("POST", "/api/lock-transaction") => {
                // JSON body'yi bul
                let mut body_start = false;
                let mut json_body = String::new();

                for line in lines.iter() {
                    if body_start {
                        json_body.push_str(line);
                        json_body.push('\n');
                    } else if line.is_empty() {
                        body_start = true;
                    }
                }

                match serde_json::from_str::<LockTransactionRequest>(&json_body.trim()) {
                    Ok(req) => {
                        match Self::handle_lock_transaction(req, market_addr, provider, config, db_obj).await {
                            Ok(_) => {
                                let body = ApiResponse {
                                    success: true,
                                    message: Some("Lock transaction processed successfully".to_string()),
                                    error: None,
                                };
                                Self::http_response(200, "OK", &serde_json::to_string(&body).unwrap_or_default())
                            }
                            Err(e) => {
                                let body = ApiResponse {
                                    success: false,
                                    message: None,
                                    error: Some(format!("Processing failed: {}", e)),
                                };
                                Self::http_response(500, "Internal Server Error", &serde_json::to_string(&body).unwrap_or_default())
                            }
                        }
                    }
                    Err(e) => {
                        let body = ApiResponse {
                            success: false,
                            message: None,
                            error: Some(format!("Invalid JSON: {}", e)),
                        };
                        Self::http_response(400, "Bad Request", &serde_json::to_string(&body).unwrap_or_default())
                    }
                }
            }
            ("OPTIONS", _) => {
                // CORS preflight
                format!(
                    "HTTP/1.1 200 OK\r\n\
                     Access-Control-Allow-Origin: *\r\n\
                     Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n\
                     Access-Control-Allow-Headers: Content-Type\r\n\
                     Content-Length: 0\r\n\r\n"
                )
            }
            _ => Self::http_response(404, "Not Found", ""),
        }
    }

    fn http_response(status_code: u16, status_text: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {} {}\r\n\
             Content-Type: application/json\r\n\
             Access-Control-Allow-Origin: *\r\n\
             Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n\
             Access-Control-Allow-Headers: Content-Type\r\n\
             Content-Length: {}\r\n\r\n{}",
            status_code,
            status_text,
            body.len(),
            body
        )
    }

    // Node.js'den gelen lock transaction'ları işle
    async fn handle_lock_transaction(
        req: LockTransactionRequest,
        market_addr: Address,
        provider: Arc<P>,
        config: ConfigLock,
        db_obj: DbObj,
    ) -> Result<()> {
        tracing::info!("🦀 Received lock transaction from Node.js:");
        tracing::info!("   - TX Hash: {}", req.tx_hash);
        tracing::info!("   - Lock Block: {}", req.lock_block);

        // Parse transaction hash
        let _tx_hash_bytes = req.tx_hash.parse::<alloy::primitives::TxHash>()
            .map_err(|e| anyhow::anyhow!("Invalid tx hash: {}", e))?;

        // Input'u decode et
        let input_bytes = hex::decode(&req.input_hex[2..])
            .context("Failed to decode input hex")?;

        let decoded = match IBoundlessMarket::submitRequestCall::abi_decode(&input_bytes) {
            Ok(call) => call,
            Err(_) => {
                return Err(anyhow::anyhow!("Transaction is not submitRequest"));
            }
        };

        let client_addr = decoded.request.client_address();
        let request_id = decoded.request.id;

        tracing::info!("📋 Processing submitRequest from lock:");
        tracing::info!("   - Request ID: 0x{:x}", request_id);
        tracing::info!("   - Client: 0x{:x}", client_addr);

        // Check capacity constraints
        let committed_orders = db_obj.get_committed_orders().await
            .map_err(|e| anyhow::anyhow!("Failed to get committed orders: {}", e))?;

        let committed_count = committed_orders.len();
        let max_capacity = Some(1); // Could be from configuration

        if let Some(max_capacity) = max_capacity {
            if committed_count as u32 >= max_capacity {
                tracing::info!("Committed orders count ({}) reached max concurrency limit ({}), skipping order {:?}",
                    committed_count,
                    max_capacity,
                    request_id
                );
                return Ok(()); // Don't process this order
            }
        }

        // Check if client is allowed
        let allowed_requestors_opt = {
            let locked_conf = config.lock_all().context("Failed to read config")?;
            locked_conf.market.allow_requestor_addresses.clone()
        };

        if let Some(allow_addresses) = allowed_requestors_opt {
            if !allow_addresses.contains(&client_addr) {
                tracing::debug!("🚫 Client not in allowed requestors, skipping");
                return Ok(());
            }
        }

        tracing::info!("✅ Processing allowed request from: 0x{:x}", client_addr);

        // Get chain ID and create order
        let chain_id = provider.get_chain_id().await.context("Failed to get chain id")?;

        let mut new_order = OrderRequest::new(
            decoded.request.clone(),
            decoded.clientSignature.clone(),
            FulfillmentType::LockAndFulfill,
            market_addr,
            chain_id,
        );

        tracing::info!("✅ Successfully received lock for request: 0x{:x} at block {}", request_id, req.lock_block);

        // Calculate lock price and save to DB
        let lock_timestamp = provider
            .get_block_by_number(req.lock_block.into())
            .await
            .context("Failed to get lock block")?
            .context("Lock block not found")?
            .header
            .timestamp;

        let lock_price = new_order
            .request
            .offer
            .price_at(lock_timestamp)
            .context("Failed to calculate lock price")?;

        // Try to get confirmed transaction data
        let final_order = match Self::fetch_confirmed_transaction_data_by_input(provider.clone(), &req.input_hex).await {
            Ok(confirmed_request) => {
                tracing::info!("✅ Got CONFIRMED data, creating updated order: 0x{:x}", request_id);

                let mut updated_order = OrderRequest::new(
                    confirmed_request,
                    decoded.clientSignature.clone(),
                    FulfillmentType::LockAndFulfill,
                    market_addr,
                    chain_id,
                );
                updated_order.target_timestamp = Some(updated_order.request.lock_expires_at());
                updated_order.expire_timestamp = Some(updated_order.request.expires_at());

                updated_order
            }
            Err(e) => {
                tracing::info!("⚠️ Confirmed data fetch failed: {} - using provided data", e);
                new_order
            }
        };

        // Insert into database
        if let Err(e) = db_obj.insert_accepted_request(&final_order, lock_price).await {
            tracing::error!("Failed to insert accepted request: {:?}", e);
            return Err(anyhow::anyhow!("Database insertion failed: {}", e));
        }

        tracing::info!("✅ Order successfully inserted into database: 0x{:x}", request_id);

        Ok(())
    }

    // Helper function to fetch confirmed transaction data
    async fn fetch_confirmed_transaction_data_by_input(
        provider: Arc<P>,
        input_hex: &str,
    ) -> Result<boundless_market::contracts::ProofRequest> {
        // Input'u decode et
        let input_bytes = hex::decode(&input_hex[2..])
            .context("Failed to decode input hex")?;

        let decoded_call = IBoundlessMarket::submitRequestCall::abi_decode(&input_bytes)
            .context("Failed to decode transaction input")?;

        tracing::debug!("✅ Input data decoded successfully");

        Ok(decoded_call.request)
    }

    fn format_time(dt: DateTime<Utc>) -> String {
        dt.format("%H:%M:%S%.3f").to_string()
    }

    // Her zaman 0x ile başlayan format kullan
    fn normalize_hex_data(data: &str) -> String {
        if data.starts_with("0x") {
            data.to_string()
        } else {
            format!("0x{}", data)
        }
    }
}

impl<P> RetryTask for MarketMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone + Send + Sync,
{
    type Error = MarketMonitorErr;

    fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        let market_addr = self.market_addr;
        let provider = self.provider.clone();
        let db = self.db_obj.clone();
        let config = self.config.clone();

        // Basit approach - HTTP server'ı direkt çalıştır
        Box::pin(async move {
            tracing::info!("🚀 Starting market monitor with simple HTTP server");

            match Self::run_simple_http_server(
                market_addr,
                provider,
                config,
                db,
                cancel_token,
            ).await {
                Ok(()) => {
                    tracing::info!("✅ HTTP server completed successfully");
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("❌ HTTP server failed: {:?}", e);
                    Err(SupervisorErr::Fault(MarketMonitorErr::UnexpectedErr(e)))
                }
            }
        })
    }
}