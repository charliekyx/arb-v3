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
    fs::{self, File},
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
    quoter: Option<String>, // Optional: Contract Address
    pool: Option<String>,   // Optional: Pool Address (for Slot0)
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

    // Aerodrome CL MixedRouteQuoterV1
    IMixedRouteQuoterV1,
    r#"[
        struct QuoteExactInputSingleV2Params { address tokenIn; address tokenOut; uint256 amountIn; uint24 fee; uint160 sqrtPriceLimitX96; }
        function quoteExactInputSingleV2(QuoteExactInputSingleV2Params params) external returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)
    ]"#;

    ICLPool,
    r#"[
        function slot0() external view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, bool unlocked)
        function token0() external view returns (address)
    ]"#;

    IAerodromePair,
    r#"[
        function reserve0() external view returns (uint256)
        function reserve1() external view returns (uint256)
        function token0() external view returns (address)
    ]"#
);

const WETH_ADDR: &str = "0x4200000000000000000000000000000000000006";
const MAX_DAILY_GAS_LOSS_WEI: u128 = 20_000_000_000_000_000;

// --- Helpers ---
#[derive(Serialize, Deserialize, Debug, Default)]
struct GasState {
    date: String,
    accumulated_loss: u128,
}

struct SharedGasManager {
    accumulated_loss: Mutex<u128>,
}

impl SharedGasManager {
    fn new(path: String) -> Self {
        let loaded = Self::load_gas_state(&path);
        Self {
            accumulated_loss: Mutex::new(loaded.accumulated_loss),
        }
    }
    fn load_gas_state(path: &str) -> GasState {
        let today = Local::now().format("%Y-%m-%d").to_string();
        if let Ok(c) = fs::read_to_string(path) {
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
}

// Local Calc V3 Price
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

// Core Quote Function (Smart Fallback)
async fn get_amount_out(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
) -> Result<U256> {
    if pool.protocol == 1 {
        // --- V2 Logic (Pair Address stored in 'quoter' field historically, fix below) ---
        // 为了兼容旧逻辑，V2 的 Pair 地址我们约定存在 config.pool 中更合理，或者 fallback 到 quoter
        // 这里假设 V2 的 Pair 地址存在 pool.quoter 中 (根据之前的 pools.json)
        let pair_addr = pool.quoter.ok_or(anyhow!("V2 missing pair address"))?;
        let pair = IAerodromePair::new(pair_addr, client.clone());
        let r0 = pair
            .reserve_0()
            .call()
            .await
            .map_err(|e| anyhow!("V2 r0: {}", e))?;
        let r1 = pair
            .reserve_1()
            .call()
            .await
            .map_err(|e| anyhow!("V2 r1: {}", e))?;
        let t0 = pair
            .token_0()
            .call()
            .await
            .map_err(|e| anyhow!("V2 t0: {}", e))?;

        let (reserve_in, reserve_out) = if t0 == token_in { (r0, r1) } else { (r1, r0) };
        if reserve_in.is_zero() || reserve_out.is_zero() {
            return Err(anyhow!("Empty V2 reserves"));
        }

        let fee_bps = U256::from(pool.fee);
        let amount_in_with_fee = amount_in * (U256::from(1000000) - fee_bps);
        let numerator = amount_in_with_fee * reserve_out;
        let denominator = (reserve_in * U256::from(1000000)) + amount_in_with_fee;
        Ok(numerator / denominator)
    } else if pool.protocol == 2 {
        // --- CL Logic (Smart Fallback) ---
        // 1. Try Quoter (if configured)
        if let Some(quoter_addr) = pool.quoter {
            let quoter = IMixedRouteQuoterV1::new(quoter_addr, client.clone());
            let params = QuoteExactInputSingleV2Params {
                token_in,
                token_out,
                amount_in,
                fee: pool.fee,
                sqrt_price_limit_x96: U256::zero(),
            };

            // 尝试调用 Quoter
            match quoter.quote_exact_input_single_v2(params).call().await {
                Ok((amount_out, _, _, _)) => return Ok(amount_out),
                Err(e) => {
                    // 仅打印一次 Warn，避免刷屏 (或者包含 debug 信息)
                    // warn!("⚠️ CL Quoter failed for {}, fallback to Slot0. Err: {:?}", pool.name, e);
                }
            }
        }

        // 2. Fallback to Local Slot0 Calc
        let pool_addr = pool
            .pool
            .ok_or(anyhow!("CL missing pool address for slot0"))?;
        let pool_contract = ICLPool::new(pool_addr, client.clone());
        let (sqrt_price, _, _, _, _, _) = pool_contract
            .slot_0()
            .call()
            .await
            .map_err(|e| anyhow!("CL Slot0 fail: {}", e))?;
        let token0 = pool_contract
            .token_0()
            .call()
            .await
            .map_err(|e| anyhow!("CL Token0 fail: {}", e))?;

        let raw_out = calculate_v3_amount_out(amount_in, U256::from(sqrt_price), token_in, token0);
        let fee_ppm = U256::from(pool.fee);
        let out_after_fee = raw_out * (U256::from(1000000) - fee_ppm) / U256::from(1000000);
        Ok(out_after_fee)
    } else {
        // --- V3 Logic (Uniswap Quoter) ---
        let quoter_addr = pool.quoter.ok_or(anyhow!("V3 missing quoter"))?;
        let quoter = IQuoterV2::new(quoter_addr, client);
        let params = QuoteParams {
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

// --- Main ---

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("🚀 System Starting: Base Bot V5.4 (Smart Fallback)");

    let config = load_encrypted_config()?;
    let provider = Arc::new(Provider::<Ipc>::connect_ipc(&config.ipc_path).await?);
    let wallet = LocalWallet::from_str(&config.private_key)?.with_chain_id(8453u64);
    let client = Arc::new(SignerMiddleware::new(provider.clone(), wallet.clone()));
    let gas_manager = Arc::new(SharedGasManager::new("gas_state.json".to_string()));

    let config_content = fs::read_to_string("pools.json").context("Failed to read pools.json")?;
    let json_configs: Vec<JsonPoolInput> = serde_json::from_str(&config_content)?;
    let weth = Address::from_str(WETH_ADDR)?;

    let mut pools = Vec::new();
    for cfg in json_configs {
        let token_a = Address::from_str(&cfg.token_a)?;
        let token_b = Address::from_str(&cfg.token_b)?;

        // Parse Addresses: Some might be None depending on JSON
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

        pools.push(PoolConfig {
            name: cfg.name,
            router: Address::from_str(&cfg.router)?,
            quoter: quoter_addr,
            pool: pool_addr,
            fee: cfg.fee,
            token_a,
            token_b,
            protocol: proto_code,
        });
    }
    info!("✅ Loaded {} Pools.", pools.len());

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

        let gas_gwei = format_units(gas_price, "gwei").unwrap_or_else(|_| "0.0".to_string());
        info!(
            "--- Block {} | Gas: {} gwei | Cands: {} -> Paths: {} ---",
            current_bn,
            gas_gwei,
            total_candidates,
            results.len()
        );

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

            if net_profit > I256::from(0) {
                info!(
                    "💰 NET PROFIT: {} | Gross: {} | Net: {} WEI",
                    route_name, gross_profit, net_profit
                );
            } else {
                if gross_profit > I256::from(-50000000000000i64) {
                    info!(
                        "🧊 WATCH: {} | Gross: {} | Net: {} (Gas: {})",
                        route_name, gross_profit, net_profit, gas_cost
                    );
                }
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
