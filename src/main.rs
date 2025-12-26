use anyhow::{anyhow, Context, Result};
use chrono::Local;
use cocoon::Cocoon;
use ethers::{
    prelude::*,
    types::{Address, U256},
    utils::{format_units, parse_ether},
};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::{self, File, OpenOptions},
    io::Write,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::{error, info, warn};

// --- Config Structs ---
#[derive(Serialize, Deserialize, Debug, Clone)]
struct AppConfig {
    private_key: String,
    ipc_path: String,
    contract_address: String,
    smtp_username: String,
    smtp_password: String,
    my_email: String,
}

#[derive(Debug, Deserialize, Clone)]
struct JsonPoolInput {
    name: String,
    token_a: String,
    token_b: String,
    router: String,
    quoter: Option<String>,
    pool: Option<String>,
    fee: u32,
    protocol: Option<String>,
}

#[derive(Clone, Debug)]
struct PoolConfig {
    name: String,
    router: Address,
    quoter: Option<Address>,
    pool: Option<Address>,
    fee: u32,
    token_a: Address,
    token_b: Address,
    protocol: u8, // 0=V3, 1=V2, 2=CL
}

// --- ABI Definitions ---
abigen!(
    IQuoterV2,
    r#"[
        struct QuoteParams { address tokenIn; address tokenOut; uint256 amountIn; uint24 fee; uint160 sqrtPriceLimitX96; }
        function quoteExactInputSingle(QuoteParams params) external returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)
    ]"#;

    IMixedRouteQuoterV1,
    r#"[
        struct QuoteParams { address tokenIn; address tokenOut; uint256 amountIn; uint24 fee; uint160 sqrtPriceLimitX96; }
        function quoteExactInputSingleV2(QuoteParams params) external returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)
    ]"#;

    ICLPool,
    r#"[
        function slot0() external view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, bool unlocked)
        function token0() external view returns (address)
    ]"#;

    IAerodromePair,
    r#"[
        function getReserves() external view returns (uint256 reserve0, uint256 reserve1, uint256 blockTimestampLast)
        function token0() external view returns (address)
        function token1() external view returns (address)
        function stable() external view returns (bool)
        function getAmountOut(uint256 amountIn, address tokenIn) external view returns (uint256 amountOut)
    ]"#
);

const WETH_ADDR: &str = "0x4200000000000000000000000000000000000006";
const MAX_DAILY_GAS_LOSS_WEI: u128 = 20_000_000_000_000_000;
const UNISWAP_QUOTER: &str = "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a";

// --- Helpers ---
#[derive(Serialize, Deserialize, Debug, Default)]
struct GasState {
    date: String,
    accumulated_loss: u128,
}

struct SharedGasManager {
    file_path: String,
    accumulated_loss: Mutex<u128>,
}

impl SharedGasManager {
    fn new(path: String) -> Self {
        let loaded = Self::load_gas_state(&path);
        Self {
            file_path: path,
            accumulated_loss: Mutex::new(loaded.accumulated_loss),
        }
    }

    fn load_gas_state(path: &str) -> GasState {
        let today = Local::now().format("%Y-%m-%d").to_string();
        if let Ok(c) = std::fs::read_to_string(path) {
            if let Ok(s) = serde_json::from_str::<GasState>(&c) {
                if s.date == today {
                    return s;
                }
            }
        }
        GasState {
            date: today,
            accumulated_loss: 0,
        }
    }

    fn get_loss(&self) -> u128 {
        *self.accumulated_loss.lock().unwrap()
    }

    // 恢复：写入 Gas 状态到文件
    fn add_loss(&self, loss: u128) {
        let mut lock = self.accumulated_loss.lock().unwrap();
        *lock += loss;

        let state = GasState {
            date: Local::now().format("%Y-%m-%d").to_string(),
            accumulated_loss: *lock,
        };

        if let Ok(json) = serde_json::to_string_pretty(&state) {
            let _ = std::fs::write(&self.file_path, json);
        }
    }
}

// 恢复：追加日志到本地文件
fn append_log_to_file(msg: &str) {
    let file_path = "opportunities.log";
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(file_path) {
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
        let _ = writeln!(file, "[{}] {}", timestamp, msg);
    }
}

fn calculate_v3_amount_out(
    amount_in: U256,
    sqrt_price_x96: U256,
    token_in: Address,
    token0: Address,
) -> U256 {
    let q96 = U256::from(2).pow(U256::from(96));
    if token_in == token0 {
        let numerator = amount_in
            .saturating_mul(sqrt_price_x96)
            .saturating_mul(sqrt_price_x96);
        let denominator = q96.saturating_mul(q96);
        numerator.checked_div(denominator).unwrap_or_default()
    } else {
        let numerator = amount_in.saturating_mul(q96).saturating_mul(q96);
        let denominator = sqrt_price_x96.saturating_mul(sqrt_price_x96);
        numerator.checked_div(denominator).unwrap_or_default()
    }
}

async fn validate_cl_pool(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
) -> bool {
    if let Some(pool_addr) = pool.pool {
        let contract = ICLPool::new(pool_addr, client);
        match contract.slot_0().call().await {
            Ok(_) => true,
            Err(_) => false,
        }
    } else {
        false
    }
}

async fn validate_v2_pool(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
) -> bool {
    if let Some(pair_addr) = pool.quoter {
        let pair = IAerodromePair::new(pair_addr, client);
        let t0 = match pair.token_0().call().await {
            Ok(t) => t,
            Err(_) => return false,
        };
        let t1 = match pair.token_1().call().await {
            Ok(t) => t,
            Err(_) => return false,
        };
        let config_match = (t0 == pool.token_a && t1 == pool.token_b)
            || (t0 == pool.token_b && t1 == pool.token_a);
        if !config_match {
            warn!(
                "⚠️ V2 Token Mismatch for {}: Config({:?}, {:?}) vs Chain({:?}, {:?})",
                pool.name, pool.token_a, pool.token_b, t0, t1
            );
            return false;
        }
        pair.get_reserves().call().await.is_ok() && pair.stable().call().await.is_ok()
    } else {
        false
    }
}

async fn get_amount_out(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
) -> Result<U256> {
    if pool.protocol == 1 {
        let pair_addr = pool.quoter.ok_or(anyhow!("V2 missing pair address"))?;
        let pair = IAerodromePair::new(pair_addr, client.clone());
        match pair.get_amount_out(amount_in, token_in).call().await {
            Ok(out) => Ok(out),
            Err(e) => Err(anyhow!("V2 on-chain quote failed: {}", e)),
        }
    } else if pool.protocol == 2 {
        if let Some(quoter_addr) = pool.quoter {
            let quoter = IMixedRouteQuoterV1::new(quoter_addr, client.clone());
            let params = i_mixed_route_quoter_v1::QuoteParams {
                token_in,
                token_out,
                amount_in,
                fee: pool.fee,
                sqrt_price_limit_x96: U256::zero(),
            };
            if let Ok((amount_out, _, _, _)) =
                quoter.quote_exact_input_single_v2(params).call().await
            {
                return Ok(amount_out);
            }
        }
        let pool_addr = pool.pool.ok_or(anyhow!("CL missing pool address"))?;
        let pool_contract = ICLPool::new(pool_addr, client.clone());
        let (sqrt_price, _, _, _, _, _) = pool_contract
            .slot_0()
            .call()
            .await
            .map_err(|e| anyhow!("CL Slot0: {}", e))?;
        let token0 = pool_contract
            .token_0()
            .call()
            .await
            .map_err(|e| anyhow!("CL Token0: {}", e))?;
        let raw_out = calculate_v3_amount_out(amount_in, U256::from(sqrt_price), token_in, token0);
        let fee_ppm = U256::from(pool.fee);
        let out_after_fee = raw_out * (U256::from(1000000) - fee_ppm) / U256::from(1000000);
        Ok(out_after_fee)
    } else {
        let quoter_addr = pool.quoter.ok_or(anyhow!("V3 missing quoter"))?;
        let quoter = IQuoterV2::new(quoter_addr, client);
        let params = i_quoter_v2::QuoteParams {
            token_in,
            token_out,
            amount_in,
            fee: pool.fee,
            sqrt_price_limit_x96: U256::zero(),
        };
        let (amount_out, _, _, _) = quoter.quote_exact_input_single(params).call().await?;
        Ok(amount_out)
    }
}

#[derive(Clone)]
struct ArbPath {
    pools: Vec<PoolConfig>,
    tokens: Vec<Address>,
    is_triangle: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("🚀 System Starting: Base Bot V5.8 (File Logging Restored)");

    let config = load_encrypted_config()?;
    let provider = Arc::new(Provider::<Ipc>::connect_ipc(&config.ipc_path).await?);
    let wallet = LocalWallet::from_str(&config.private_key)?.with_chain_id(8453u64);
    let client = Arc::new(SignerMiddleware::new(provider.clone(), wallet.clone()));
    let gas_manager = Arc::new(SharedGasManager::new("gas_state.json".to_string()));

    let config_content = fs::read_to_string("pools.json").context("Failed to read pools.json")?;
    let json_configs: Vec<JsonPoolInput> = serde_json::from_str(&config_content)?;
    let weth = Address::from_str(WETH_ADDR)?;
    let uniswap_quoter_addr = Address::from_str(UNISWAP_QUOTER)?;

    let mut pools = Vec::new();

    info!("🔍 Validating pools before startup...");
    for cfg in json_configs {
        let token_a = Address::from_str(&cfg.token_a)?;
        let token_b = Address::from_str(&cfg.token_b)?;
        let quoter_addr = cfg.quoter.as_ref().map(|s| Address::from_str(s).unwrap());
        let pool_addr = cfg.pool.as_ref().map(|s| Address::from_str(s).unwrap());

        let proto_str = cfg.protocol.unwrap_or("v3".to_string()).to_lowercase();
        let proto_code = if proto_str == "v2" {
            1
        } else if proto_str == "cl" {
            2
        } else {
            0
        };

        if proto_code == 2 && quoter_addr == Some(uniswap_quoter_addr) {
            warn!("⚠️ Skipping [{}]: CL pool using Uniswap Quoter.", cfg.name);
            continue;
        }

        let p_config = PoolConfig {
            name: cfg.name.clone(),
            router: Address::from_str(&cfg.router)?,
            quoter: quoter_addr,
            pool: pool_addr,
            fee: cfg.fee,
            token_a,
            token_b,
            protocol: proto_code,
        };

        let is_valid = match proto_code {
            1 => validate_v2_pool(client.clone(), &p_config).await,
            2 => validate_cl_pool(client.clone(), &p_config).await,
            _ => true,
        };

        if !is_valid {
            warn!(
                "❌ Removing invalid pool [{}]: Validation failed.",
                cfg.name
            );
            continue;
        }

        pools.push(p_config);
    }
    info!("✅ Active Pools: {}", pools.len());

    let mut stream = client.subscribe_blocks().await?;
    info!("Waiting for blocks...");

    loop {
        let block = match tokio::time::timeout(Duration::from_secs(15), stream.next()).await {
            Ok(Some(b)) => b,
            _ => {
                warn!("Timeout");
                continue;
            }
        };
        let current_bn = block.number.unwrap();

        if gas_manager.get_loss() >= MAX_DAILY_GAS_LOSS_WEI {
            error!("💀 Daily Gas Limit Reached.");
            break;
        }

        let borrow_amount = parse_ether("0.001").unwrap();
        let client_ref = &client;
        let weth_addr_parsed: Address = WETH_ADDR.parse().unwrap();
        let gas_price = provider
            .get_gas_price()
            .await
            .unwrap_or(parse_ether("0.0000000001").unwrap());
        let gas_cost_2hop = (gas_price * U256::from(300_000)).as_u128();
        let gas_cost_3hop = (gas_price * U256::from(450_000)).as_u128();

        let mut candidates = Vec::new();

        // 2-Hop
        for i in 0..pools.len() {
            for j in 0..pools.len() {
                if i == j {
                    continue;
                }
                let pa = &pools[i];
                let pb = &pools[j];
                let start_token = weth_addr_parsed;
                if pa.token_a != start_token && pa.token_b != start_token {
                    continue;
                }
                let token_mid = if pa.token_a == start_token {
                    pa.token_b
                } else {
                    pa.token_a
                };

                let pb_has_mid = pb.token_a == token_mid || pb.token_b == token_mid;
                let pb_has_end = pb.token_a == start_token || pb.token_b == start_token;

                if pb_has_mid && pb_has_end {
                    candidates.push(ArbPath {
                        pools: vec![pa.clone(), pb.clone()],
                        tokens: vec![start_token, token_mid, start_token],
                        is_triangle: false,
                    });
                }
            }
        }

        // 3-Hop
        for i in 0..pools.len() {
            let pa = &pools[i];
            if pa.token_a != weth_addr_parsed && pa.token_b != weth_addr_parsed {
                continue;
            }
            let token_1 = if pa.token_a == weth_addr_parsed {
                pa.token_b
            } else {
                pa.token_a
            };

            for j in 0..pools.len() {
                if i == j {
                    continue;
                }
                let pb = &pools[j];
                let pb_has_weth = pb.token_a == weth_addr_parsed || pb.token_b == weth_addr_parsed;
                if pb_has_weth {
                    continue;
                }

                if pb.token_a != token_1 && pb.token_b != token_1 {
                    continue;
                }
                let token_2 = if pb.token_a == token_1 {
                    pb.token_b
                } else {
                    pb.token_a
                };

                for k in 0..pools.len() {
                    if k == i || k == j {
                        continue;
                    }
                    let pc = &pools[k];
                    let pc_has_token2 = pc.token_a == token_2 || pc.token_b == token_2;
                    let pc_has_weth =
                        pc.token_a == weth_addr_parsed || pc.token_b == weth_addr_parsed;

                    if pc_has_token2 && pc_has_weth {
                        candidates.push(ArbPath {
                            pools: vec![pa.clone(), pb.clone(), pc.clone()],
                            tokens: vec![weth_addr_parsed, token_1, token_2, weth_addr_parsed],
                            is_triangle: true,
                        });
                    }
                }
            }
        }

        let total_candidates = candidates.len();
        let results = stream::iter(candidates)
            .map(|path| async move {
                let mut amt = borrow_amount;
                // Step 1
                amt = match get_amount_out(
                    client_ref.clone(),
                    &path.pools[0],
                    path.tokens[0],
                    path.tokens[1],
                    amt,
                )
                .await
                {
                    Ok(a) => a,
                    Err(e) => {
                        warn!("❌ S1 Fail [{}]: {:?}", path.pools[0].name, e);
                        return None;
                    }
                };
                // Step 2
                amt = match get_amount_out(
                    client_ref.clone(),
                    &path.pools[1],
                    path.tokens[1],
                    path.tokens[2],
                    amt,
                )
                .await
                {
                    Ok(a) => a,
                    Err(e) => {
                        warn!("❌ S2 Fail [{}]: {:?}", path.pools[1].name, e);
                        return None;
                    }
                };
                // Step 3
                if path.is_triangle {
                    amt = match get_amount_out(
                        client_ref.clone(),
                        &path.pools[2],
                        path.tokens[2],
                        path.tokens[3],
                        amt,
                    )
                    .await
                    {
                        Ok(a) => a,
                        Err(e) => {
                            warn!("❌ S3 Fail [{}]: {:?}", path.pools[2].name, e);
                            return None;
                        }
                    };
                }
                Some((path, amt))
            })
            .buffer_unordered(20)
            .collect::<Vec<_>>()
            .await;

        let ok_paths = results.iter().flatten().count();
        let gas_gwei = format_units(gas_price, "gwei").unwrap_or_else(|_| "0.0".to_string());

        // info!(
        //     "--- Block {} | Gas: {} gwei | Cands: {} -> OkPaths: {} ---",
        //     current_bn, gas_gwei, total_candidates, ok_paths
        // );

        for (path, out_eth) in results.into_iter().flatten() {
            let gross_profit = if out_eth > borrow_amount {
                I256::from((out_eth - borrow_amount).as_u128())
            } else {
                -I256::from((borrow_amount - out_eth).as_u128())
            };

            let gas_cost = if path.is_triangle {
                gas_cost_3hop
            } else {
                gas_cost_2hop
            };
            let net_profit = gross_profit - I256::from(gas_cost);

            let route_name = if path.is_triangle {
                format!(
                    "{}->{}->{}",
                    path.pools[0].name, path.pools[1].name, path.pools[2].name
                )
            } else {
                format!("{}->{}", path.pools[0].name, path.pools[1].name)
            };

            // Build log message
            let log_msg = if net_profit > I256::from(0) {
                format!(
                    "💰 NET PROFIT: {} | Gross: {} | Net: {} WEI",
                    route_name, gross_profit, net_profit
                )
            } else if gross_profit > I256::from(-50000000000000i64) {
                // format!(
                //     "🧊 WATCH: {} | Gross: {} | Net: {} (Gas: {})",
                //     route_name, gross_profit, net_profit, gas_cost
                // )
                String::new()
            } else {
                String::new()
            };

            if !log_msg.is_empty() {
                info!("{}", log_msg);
                append_log_to_file(&log_msg); // 🔥 写入本地文件
            }
        }
        info!("-----------------------");
    }
    Ok(())
}

fn load_encrypted_config() -> Result<AppConfig> {
    let password = env::var("CONFIG_PASS").unwrap_or_else(|_| "password".to_string());
    let mut file = File::open("mev_bot.secure")?;
    let cocoon = Cocoon::new(password.as_bytes());
    let decrypted_bytes = cocoon.parse(&mut file).map_err(|e| anyhow!("{:?}", e))?;
    Ok(serde_json::from_slice(&decrypted_bytes)?)
}
