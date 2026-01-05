use anyhow::Result;
use ethers::prelude::*;
use ethers::utils::parse_units;
use std::sync::Arc;
use tracing::{info, warn};

// 定义合约接口 (ABI)
abigen!(
    ArbEngine,
    r#"[
        struct ArbStep { address router; address tokenIn; address tokenOut; uint24 fee; uint8 protocol; }
        function executeArb(uint256 amountIn, ArbStep[] calldata steps, uint256 minProfit) external returns (uint256 amountOut)
    ]"#
);

pub async fn execute_transaction(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    contract_address: Address,
    amount_in: U256,
    _expected_gross_profit: U256,
    // pool_data tuple: (router, token_in, token_out, fee, protocol)
    pools_data: Vec<(Address, Address, Address, u32, u8)>,
    provider: Arc<Provider<Ipc>>,
) -> Result<TxHash> {
    // 1. 构建合约调用参数
    let steps: Vec<ArbStep> = pools_data
        .into_iter()
        .map(|(router, token_in, token_out, fee, protocol)| ArbStep {
            router,
            token_in,
            token_out,
            fee: fee as u32,
            protocol,
        })
        .collect();

    let contract = ArbEngine::new(contract_address, client.clone());

    // 建议：min_profit 不要写死为 0，最好作为参数传入。
    // 这里暂时保持为 0 以确保先跑通，后续可以在 main.rs 里计算。
    let min_profit = U256::zero();

    // 2. 构建合约调用 (FunctionCall)
    // [FIXED] 修正调用顺序：amount -> steps -> min_profit
    let call = contract.execute_arb(amount_in, steps, min_profit);

    // 3. 获取当前 BaseFee 并计算 EIP-1559 费用
    let _block = client
        .provider()
        .get_block(BlockNumber::Latest)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Failed to get latest block"))?;

    // 1. Get current network gas status
    let (max_fee, priority_fee) = match provider.estimate_eip1559_fees(None).await {
        Ok((base, prio)) => {
            // Aggressive Strategy:
            // Base Fee + 20% buffer
            // Priority Fee: Use network average or at least 0.1 gwei (Base chain default)
            let aggressive_prio: U256 = if prio < parse_units("0.1", "gwei").unwrap().into() {
                parse_units("0.1", "gwei").unwrap().into()
            } else {
                prio
            };
            (base * 120 / 100 + aggressive_prio, aggressive_prio)
        }
        Err(_) => (
            parse_units("0.1", "gwei")?.into(),
            parse_units("0.05", "gwei")?.into(),
        ), // Fallback
    };

    // 4. 模拟执行 (Estimate Gas)
    let estimated_gas: U256 = match call.estimate_gas().await {
        Ok(gas) => {
            // 使用 explicit 的 from 转换，避免直接运算导致的类型歧义
            let multiplier = U256::from(150);
            let divisor = U256::from(100);
            (gas * multiplier) / divisor
        }
        Err(e) => {
            warn!("[EXECUTION SKIP] Simulation failed: {:?}", e);
            // 确保这里返回的是 Result::Err，这会中断整个函数执行
            return Err(anyhow::anyhow!("Simulation failed: {:?}", e));
        }
    };

    info!(
        "Simulation OK. Gas: {}, MaxFee: {} gwei",
        estimated_gas,
        ethers::utils::format_units(max_fee, "gwei").unwrap_or_default()
    );

    // 5. 显式构建 EIP-1559 交易请求
    let mut tx_req = Eip1559TransactionRequest::new()
        .data(call.tx.data().cloned().unwrap_or_default())
        .gas(estimated_gas)
        .max_fee_per_gas(max_fee)
        .max_priority_fee_per_gas(priority_fee);

    if let Some(to_addr) = call.tx.to() {
        tx_req = tx_req.to(to_addr.clone());
    }

    // 6. 发送交易
    let pending_tx = client.send_transaction(tx_req, None).await?;

    info!("Tx Broadcasted: {:?}", pending_tx.tx_hash());
    Ok(pending_tx.tx_hash())
}
