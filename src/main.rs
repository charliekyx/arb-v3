use anyhow::{anyhow, Context, Result};
use chrono::Local;
use cocoon::Cocoon;
use ethers::{
    prelude::*,
    types::{Address, U256},
    utils::{format_units, parse_ether}, // Added format_units
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
    quoter: Address, // Now strictly a Quoter Contract Address
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

// Core Quote Function
async fn get_amount_out(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
) -> Result<U256> {
    if pool.protocol == 1 {
        // --- V2 Logic ---
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
    } else {
        // --- V3 & CL Logic (Unified via QuoterV2) ---
        // 现在 CL 和 V3 都使用相同的 QuoterV2 接口和地址
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

// Struct to hold a trade path (2-hop or 3-hop)
#[derive(Clone)]
struct ArbPath {
    pools: Vec<PoolConfig>,
    tokens: Vec<Address>, // sequence of tokens: [In, Mid1, (Mid2), Out]
    is_triangle: bool,
}

// --- Main ---

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("🚀 System Starting: Base Bot V5.0 (True Triangle & Real Quoter)");
    info!("🔥 Features: V2/V3/CL Unified Quoting + 2-Hop & 3-Hop Search");

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

        // 1. Fetch Gas Price
        let gas_price = provider
            .get_gas_price()
            .await
            .unwrap_or(parse_ether("0.0000000001").unwrap());
        let gas_cost_2hop = (gas_price * U256::from(250_000)).as_u128();
        let gas_cost_3hop = (gas_price * U256::from(350_000)).as_u128();

        // 2. Build Candidates (Dynamic 2-Hop & 3-Hop)
        let mut candidates = Vec::new();

        // --- 2-Hop Generation ---
        for i in 0..pools.len() {
            for j in 0..pools.len() {
                if i == j {
                    continue;
                }
                let pa = &pools[i];
                let pb = &pools[j];

                // Route: WETH -> (Pool A) -> TokenMid -> (Pool B) -> WETH
                // Check if PA contains WETH
                let start_token = weth_addr_parsed;
                if pa.token_a != start_token && pa.token_b != start_token {
                    continue;
                }
                let token_mid = if pa.token_a == start_token {
                    pa.token_b
                } else {
                    pa.token_a
                };

                // Check if PB connects TokenMid back to WETH
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

        // --- 3-Hop (Triangle) Generation ---
        // WETH -> (Pool A) -> TokenX -> (Pool B) -> TokenY -> (Pool C) -> WETH
        // Only run this if you have pools connecting two non-WETH tokens (e.g. USDC-AERO)
        // With current 14 pools (mostly WETH pairs), this loop might not find many, but logic is ready.
        for i in 0..pools.len() {
            let pa = &pools[i];
            if pa.token_a != weth_addr_parsed && pa.token_b != weth_addr_parsed {
                continue;
            }
            let token_1 = if pa.token_a == weth_addr_parsed {
                pa.token_b
            } else {
                pa.token_a
            }; // Token X

            for j in 0..pools.len() {
                if i == j {
                    continue;
                }
                let pb = &pools[j];
                // PB must connect Token X -> Token Y
                if pb.token_a != token_1 && pb.token_b != token_1 {
                    continue;
                }
                let token_2 = if pb.token_a == token_1 {
                    pb.token_b
                } else {
                    pb.token_a
                }; // Token Y

                if token_2 == weth_addr_parsed {
                    continue;
                } // Back to WETH is 2-hop, skip

                for k in 0..pools.len() {
                    if k == i || k == j {
                        continue;
                    }
                    let pc = &pools[k];
                    // PC must connect Token Y -> WETH
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

        let results = stream::iter(candidates)
            .map(|path| async move {
                let mut current_amount = borrow_amount;

                // Step 1
                current_amount = match get_amount_out(
                    client_ref.clone(),
                    &path.pools[0],
                    path.tokens[0],
                    path.tokens[1],
                    current_amount,
                )
                .await
                {
                    Ok(a) => a,
                    Err(_) => return None,
                };
                // Step 2
                current_amount = match get_amount_out(
                    client_ref.clone(),
                    &path.pools[1],
                    path.tokens[1],
                    path.tokens[2],
                    current_amount,
                )
                .await
                {
                    Ok(a) => a,
                    Err(_) => return None,
                };
                // Step 3 (if triangle)
                if path.is_triangle {
                    current_amount = match get_amount_out(
                        client_ref.clone(),
                        &path.pools[2],
                        path.tokens[2],
                        path.tokens[3],
                        current_amount,
                    )
                    .await
                    {
                        Ok(a) => a,
                        Err(_) => return None,
                    };
                }

                Some((path, current_amount))
            })
            .buffer_unordered(20)
            .collect::<Vec<_>>()
            .await;

        let gas_gwei = format_units(gas_price, "gwei").unwrap_or_else(|_| "0.0".to_string());
        info!(
            "--- Block {} | Gas: {} gwei | Paths: {} ---",
            current_bn,
            gas_gwei,
            results.len()
        );

        for (path, out_eth) in results.into_iter().flatten() {
            // Profit Calc
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

            // Logging
            let route_name = if path.is_triangle {
                format!(
                    "{}->{}->{}",
                    path.pools[0].name, path.pools[1].name, path.pools[2].name
                )
            } else {
                format!("{}->{}", path.pools[0].name, path.pools[1].name)
            };

            if net_profit > I256::from(0) {
                let log_msg = format!(
                    "💰 NET PROFIT: {} | Gross: {} | Net: {} WEI",
                    route_name, gross_profit, net_profit
                );
                info!("{}", log_msg);
                // TODO: Execute Logic Here
            } else {
                // Only show relevant losses to avoid spam
                if gross_profit > I256::from(-1000000000000000i64) {
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
