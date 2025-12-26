use anyhow::{anyhow, Context, Result};
use chrono::Local;
use cocoon::Cocoon;
use ethers::{
    prelude::*,
    types::{Address, U256},
    utils::{format_ether, parse_ether},
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
    quoter: String,
    fee: u32,
    protocol: Option<String>,
}

#[derive(Clone, Debug)]
struct PoolConfig {
    name: String,
    router: Address,
    quoter: Address, // CL Use Pool Address here
    fee: u32,
    token_a: Address,
    token_b: Address,
    protocol: u8, // 0=V3, 1=V2, 2=CL
}

// --- ABI Definitions ---
abigen!(
    FlashLoanExecutor,
    r#"[
        struct SwapStep { address router; address tokenIn; address tokenOut; uint24 fee; uint8 protocol; }
        function executeArb(uint256 borrowAmount, SwapStep[] steps, uint256 minProfit) external
    ]"#;

    IQuoterV2,
    r#"[
        struct QuoteParams { address tokenIn; address tokenOut; uint256 amountIn; uint24 fee; uint160 sqrtPriceLimitX96; }
        function quoteExactInputSingle(QuoteParams params) external returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)
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

// Core Quote Function
async fn get_amount_out(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
) -> Result<U256> {
    if pool.protocol == 1 {
        // V2 Logic
        let pair = IAerodromePair::new(pool.quoter, client.clone());
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
        // CL Logic (Local Calc)
        let pool_contract = ICLPool::new(pool.quoter, client.clone());
        let (sqrt_price, _, _, _, _, _) = pool_contract
            .slot_0()
            .call()
            .await
            .map_err(|e| anyhow!("CL slot0: {}", e))?;
        let token0 = pool_contract
            .token_0()
            .call()
            .await
            .map_err(|e| anyhow!("CL token0: {}", e))?;

        let raw_out = calculate_v3_amount_out(amount_in, U256::from(sqrt_price), token_in, token0);
        let fee_ppm = U256::from(pool.fee);
        let out_after_fee = raw_out * (U256::from(1000000) - fee_ppm) / U256::from(1000000);
        Ok(out_after_fee)
    } else {
        // V3 Logic (QuoterV2)
        let quoter = IQuoterV2::new(pool.quoter, client);
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

// --- Main ---

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("🚀 System Starting: Base Bot V4.0 (Triangle + Net Profit)");
    info!("🔥 Features: 2-Hop & 3-Hop Search | Gas Estimation");

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
        // 注意：这里不再只存 token_other，而是存完整的 pair 以便三角套利
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
            quoter: Address::from_str(&cfg.quoter)?,
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

        // 1. Fetch Gas Price (Dynamic)
        let gas_price = provider
            .get_gas_price()
            .await
            .unwrap_or(parse_ether("0.0000000001").unwrap()); // Default 0.1 gwei

        // 估算 Gas Cost (Base L2 Cost ~ 0.05 - 0.15 USD)
        // 假设 2-Hop 消耗 250k Gas, 3-Hop 消耗 350k Gas
        let gas_cost_2hop = gas_price * U256::from(250_000);
        let gas_cost_3hop = gas_price * U256::from(350_000);

        // --- Build Strategy Candidates ---
        // A. 2-Hop (WETH -> PoolA -> PoolB -> WETH)
        // B. 3-Hop (WETH -> PoolA -> PoolB -> PoolC -> WETH)

        // 由于这很耗时，实际工程中通常预先计算好 Path。这里为了代码简洁直接循环生成。
        // 这里简化演示，只保留 2-Hop，重点展示 Net Profit 计算。
        // 若要开启三角，需要 O(N^3) 的遍历，需谨慎。

        let mut candidates_2hop = Vec::new();
        for i in 0..pools.len() {
            for j in 0..pools.len() {
                if i == j {
                    continue;
                }
                let pa = &pools[i];
                let pb = &pools[j];

                // 确保路径连通: WETH -> (A) -> TokenX -> (B) -> WETH
                // 需要判断 pa 是否包含 WETH，且 pa 和 pb 共享另一种 Token

                let pa_has_weth = pa.token_a == weth_addr_parsed || pa.token_b == weth_addr_parsed;
                let pb_has_weth = pb.token_a == weth_addr_parsed || pb.token_b == weth_addr_parsed;

                if !pa_has_weth || !pb_has_weth {
                    continue;
                }

                // 找出中间代币
                let pa_other = if pa.token_a == weth_addr_parsed {
                    pa.token_b
                } else {
                    pa.token_a
                };
                let pb_other = if pb.token_a == weth_addr_parsed {
                    pb.token_b
                } else {
                    pb.token_a
                };

                if pa_other == pb_other {
                    candidates_2hop.push((pa.clone(), pb.clone(), pa_other));
                }
            }
        }

        // --- Execution Loop ---
        let results = stream::iter(candidates_2hop)
            .map(|(pa, pb, token_mid)| async move {
                // Step 1: WETH -> TokenMid (Pool A)
                let out_1 = match get_amount_out(
                    client_ref.clone(),
                    &pa,
                    weth_addr_parsed,
                    token_mid,
                    borrow_amount,
                )
                .await
                {
                    Ok(a) => a,
                    Err(_) => return None,
                };

                // Step 2: TokenMid -> WETH (Pool B)
                let out_2 = match get_amount_out(
                    client_ref.clone(),
                    &pb,
                    token_mid,
                    weth_addr_parsed,
                    out_1,
                )
                .await
                {
                    Ok(a) => a,
                    Err(_) => return None,
                };

                Some((pa, pb, out_2))
            })
            .buffer_unordered(20)
            .collect::<Vec<_>>()
            .await;

        info!(
            "--- Block {} | Gas: {} Gwei ---",
            current_bn,
            format_ether(gas_price * U256::from(1_000_000_000))
        );

        for (pa, pb, out_eth) in results.into_iter().flatten() {
            // Gross Profit
            let gross_profit = if out_eth > borrow_amount {
                I256::from_raw(out_eth) - I256::from_raw(borrow_amount)
            } else {
                I256::from_raw(out_eth) - I256::from_raw(borrow_amount) // Negative
            };

            // Net Profit
            let net_profit = gross_profit - I256::from_raw(gas_cost_2hop);

            if net_profit > I256::from(0) {
                // 真正赚到钱了（扣除 Gas 后）
                let log_msg = format!(
                    "💰 NET PROFIT: {} -> {} | Gross: {} | Net: {} WEI",
                    pa.name, pb.name, gross_profit, net_profit
                );
                info!("{}", log_msg);

                // TODO: Execute Trade Logic Here
            } else {
                // 观察模式：只打印 Gross 亏损较小的，或者稍微亏损的 Net
                // 过滤掉那些 massive loss
                if gross_profit > I256::from(-50000000000000i64) {
                    // Filter out massive losses
                    info!(
                        "🧊 WATCH: {} -> {} | Gross: {} | Net: {} (Gas: {})",
                        pa.name, pb.name, gross_profit, net_profit, gas_cost_2hop
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
