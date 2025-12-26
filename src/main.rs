use anyhow::{anyhow, Context, Result};
use chrono::Local;
use cocoon::Cocoon;
use ethers::{
    prelude::*,
    types::{Address, I256, U256},
    utils::{format_ether, format_units, parse_ether},
};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::{self, File, OpenOptions},
    io::Write,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
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
    let file_path = "opportunities.txt";
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
        let pair = IAerodromePair::new(pool.quoter.unwrap(), client);
        // 只信链上给的结果，不信本地算的
        return pair
            .get_amount_out(amount_in, token_in)
            .call()
            .await
            .map_err(|e| anyhow!("V2 On-chain Quote Fail: {}", e));
    } else if pool.protocol == 2 {
        if let Some(q) = pool.quoter {
            let quoter = IMixedRouteQuoterV1::new(q, client.clone());
            let params = i_mixed_route_quoter_v1::QuoteParams {
                token_in,
                token_out,
                amount_in,
                fee: pool.fee,
                sqrt_price_limit_x96: U256::zero(),
            };
            if let Ok((out, _, _, _)) = quoter.quote_exact_input_single_v2(params).call().await {
                return Ok(out); // ✅ 只有 Quoter 给出的价格在大额时才可信
            }
        }

        // 🔥 限制 Fallback 条件：仅当金额极小 (如 < 0.005 ETH) 且无 Quoter 时作为调试参考
        if amount_in > parse_ether("0.005").unwrap() {
            return Err(anyhow!("CL Quoter failed for large size: {}", pool.name));
        }

        // 原有的 slot0 fallback 仅用于小额“存活探测”
        let pc = ICLPool::new(pool.pool.unwrap(), client);
        let (sqrt, _, _, _, _, _) = pc.slot_0().call().await?;
        let t0 = pc.token_0().call().await?;
        let raw = calculate_v3_amount_out(amount_in, U256::from(sqrt), token_in, t0);
        Ok(raw * (1000000 - pool.fee) / 1000000)
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

    let test_sizes = [
        parse_ether("0.1").unwrap(),
        parse_ether("0.5").unwrap(),
        parse_ether("1.0").unwrap(),
        parse_ether("2.0").unwrap(),
        parse_ether("5.0").unwrap(),
    ];

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

        let _borrow_amount = parse_ether("0.001").unwrap();
        let client_ref = &client;
        let weth_addr_parsed: Address = WETH_ADDR.parse().unwrap();
        let gas_price = provider
            .get_gas_price()
            .await
            .unwrap_or(parse_ether("0.0000000001").unwrap());
        let _gas_cost_2hop = (gas_price * U256::from(300_000)).as_u128();
        let _gas_cost_3hop = (gas_price * U256::from(450_000)).as_u128();

        let mut candidates = Vec::new();

        // 2-Hop
        // 2-Hop 逻辑中，确保不只是 weth <-> mid <-> weth
        // 而是可以在不同的池子间跳
        for i in 0..pools.len() {
            for j in 0..pools.len() {
                if i == j {
                    continue;
                }
                // 池子 A 包含 WETH，池子 B 也包含 WETH
                if (pools[i].token_a == weth || pools[i].token_b == weth)
                    && (pools[j].token_a == weth || pools[j].token_b == weth)
                {
                    let mid_i = if pools[i].token_a == weth {
                        pools[i].token_b
                    } else {
                        pools[i].token_a
                    };
                    let mid_j = if pools[j].token_a == weth {
                        pools[j].token_b
                    } else {
                        pools[j].token_a
                    };

                    // 如果两个池子的中间币是同一种（比如都是 cbETH）
                    if mid_i == mid_j {
                        candidates.push(ArbPath {
                            pools: vec![pools[i].clone(), pools[j].clone()],
                            tokens: vec![weth, mid_i, weth],
                            is_triangle: false,
                        });
                    }
                }
            }
        }
        // --- 3-Hop 路径生成器优化 ---
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

                // 修正：如果 token_1 是 A，这个池子是 A/B，那么 token_2 应该拿到 B
                if pb.token_a != token_1 && pb.token_b != token_1 {
                    continue;
                }
                let token_2 = if pb.token_a == token_1 {
                    pb.token_b
                } else {
                    pb.token_a
                };

                // 重点：只有当第 2 跳又回到了 WETH 时才跳过（因为那是 2-hop 的事）
                if token_2 == weth_addr_parsed {
                    continue;
                }

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
        let ok_paths = Arc::new(AtomicUsize::new(0));
        let ok_paths_ref = ok_paths.clone();

        // 核心逻辑：遍历 candidates 路径
        stream::iter(candidates)
            .for_each_concurrent(10, |path| {
                let ok_paths = ok_paths_ref.clone();
                async move {
                    let mut path_profitable = false;
                    let mut best_gross = I256::from(i64::MIN);
                    let mut best_report = String::new();
                    let mut found_any = false;

                    // 🔥 局部改动：针对每条路径，跑遍所有资金档位
                    for size in test_sizes {
                        let mut current_amt = size;
                        let mut step_results = Vec::new();
                        let mut failed = false;

                        // 逐跳获取报价
                        for i in 0..path.pools.len() {
                            match get_amount_out(
                                client_ref.clone(),
                                &path.pools[i],
                                path.tokens[i],
                                path.tokens[i + 1],
                                current_amt,
                            )
                            .await
                            {
                                Ok(out) => {
                                    step_results.push((
                                        current_amt,
                                        out,
                                        path.pools[i].name.clone(),
                                    ));
                                    current_amt = out;
                                }
                                Err(_) => {
                                    failed = true;
                                    break;
                                }
                            }
                        }

                        if !failed {
                            // 🔥 修改：只要路径没有 revert (failed == false)，就视为“有效路径”并计数
                            // 这样 OkPaths 就会显示所有能跑通的路径数量，而不仅仅是盈利的
                            if !path_profitable {
                                ok_paths.fetch_add(1, Ordering::Relaxed);
                                path_profitable = true;
                            }

                            // 计算毛利
                            let gross = if current_amt > size {
                                I256::from((current_amt - size).as_u128())
                            } else {
                                -I256::from((size - current_amt).as_u128())
                            };

                            // 估算 Gas (2-hop 用 160k, 3-hop 用 280k)
                            let gas_used = if path.is_triangle { 280_000 } else { 160_000 };
                            let gas_cost = I256::from((gas_price * U256::from(gas_used)).as_u128());
                            let net = gross - gas_cost;

                            found_any = true;
                            if gross > best_gross {
                                best_gross = gross;
                                let route_name = if path.is_triangle {
                                    format!(
                                        "{}->{}->{}",
                                        path.pools[0].name, path.pools[1].name, path.pools[2].name
                                    )
                                } else {
                                    format!("{}->{}", path.pools[0].name, path.pools[1].name)
                                };
                                best_report = format!(
                                    "🧊 WATCH: {} | Best Size: {} | Gross: {} | Net: {} (Gas: {})",
                                    route_name,
                                    format_ether(size),
                                    gross,
                                    net,
                                    gas_cost
                                );
                            }

                            // 只要毛利为正，就记录“证据”
                            if gross > I256::zero() {
                                let mut report = format!(
                                    "--- Opportunity (Size: {} ETH) ---\n",
                                    format_ether(size)
                                );
                                for (idx, (inp, outp, p_name)) in step_results.iter().enumerate() {
                                    report.push_str(&format!(
                                        "  Step {}: {} -> {} via {}\n",
                                        idx + 1,
                                        format_ether(*inp),
                                        format_ether(*outp),
                                        p_name
                                    ));
                                }
                                report
                                    .push_str(&format!("  Gross: {} | Net: {} WEI\n", gross, net));

                                info!("{}", report);
                                append_log_to_file(&report);
                            }
                        }
                    }

                    if found_any {
                        // 仅当最佳档位的毛利 > -0.001 ETH 时才显示（过滤掉必死路径）
                        if best_gross > I256::from(-1000000000000000i128) {
                            info!("{}", best_report);
                        }
                    }
                }
            })
            .await;

        let gas_gwei = format_units(gas_price, "gwei").unwrap_or_else(|_| "0.0".to_string());
        let ok_paths_count = ok_paths.load(Ordering::Relaxed);
        info!(
            "--- Block {} | Gas: {} gwei | Cands: {} -> OkPaths: {} ---",
            current_bn, gas_gwei, total_candidates, ok_paths_count
        );
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
