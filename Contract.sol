// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "openzeppelin-contracts/contracts/access/Ownable.sol";
import "openzeppelin-contracts/contracts/token/ERC20/IERC20.sol";
import "openzeppelin-contracts/contracts/token/ERC20/utils/SafeERC20.sol";

// --- Uniswap V3 Router 接口 ---
interface ISwapRouter {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    function exactInputSingle(
        ExactInputSingleParams calldata params
    ) external payable returns (uint256 amountOut);
}

// --- Balancer 接口 ---
interface IFlashLoanRecipient {
    function receiveFlashLoan(
        IERC20[] memory tokens,
        uint256[] memory amounts,
        uint256[] memory feeAmounts,
        bytes memory userData
    ) external;
}

interface IVault {
    function flashLoan(
        IFlashLoanRecipient recipient,
        IERC20[] memory tokens,
        uint256[] memory amounts,
        bytes memory userData
    ) external;
}

contract FlashLoanExecutor is IFlashLoanRecipient, Ownable {
    using SafeERC20 for IERC20;

    // Base 链 Balancer Vault
    address private constant BALANCER_VAULT =
        0xBA12222222228d8Ba445958a75a0704d566BF2C8;
    // Base 链 WETH
    address private constant WETH = 0x4200000000000000000000000000000000000006;

    address public executor;

    struct SwapStep {
        address router; // V3 Router 地址
        address tokenIn;
        address tokenOut;
        uint24 fee; // 500, 3000, 10000
    }

    struct ArbParams {
        uint256 borrowAmount;
        SwapStep[] steps;
        uint256 minProfit;
    }

    error InsufficientProfit(uint256 balanceAfter, uint256 required);
    error NotBalancer();
    error OnlyExecutorOrOwner();

    constructor() Ownable(msg.sender) {}

    function setExecutor(address _executor) external onlyOwner {
        executor = _executor;
    }

    modifier onlyExecutorOrOwner() {
        if (msg.sender != executor && msg.sender != owner()) {
            revert OnlyExecutorOrOwner();
        }
        _;
    }

    // 运维函数：必须给 Router 授权
    function approveToken(
        address token,
        address spender,
        uint256 amount
    ) external onlyOwner {
        IERC20(token).approve(spender, amount);
    }

    function withdraw(address token) external onlyOwner {
        if (token == address(0)) {
            payable(msg.sender).transfer(address(this).balance);
        } else {
            IERC20(token).safeTransfer(
                msg.sender,
                IERC20(token).balanceOf(address(this))
            );
        }
    }

    receive() external payable {}

    // --- 核心入口 ---
    function executeArb(
        uint256 borrowAmount,
        SwapStep[] calldata steps,
        uint256 minProfit
    ) external onlyExecutorOrOwner {
        bytes memory userData = abi.encode(
            ArbParams({
                borrowAmount: borrowAmount,
                steps: steps,
                minProfit: minProfit
            })
        );

        IERC20[] memory tokens = new IERC20[](1);
        tokens[0] = IERC20(WETH);
        uint256[] memory amounts = new uint256[](1);
        amounts[0] = borrowAmount;

        IVault(BALANCER_VAULT).flashLoan(
            IFlashLoanRecipient(address(this)),
            tokens,
            amounts,
            userData
        );
    }

    // --- 闪电贷回调 ---
    function receiveFlashLoan(
        IERC20[] memory tokens,
        uint256[] memory amounts,
        uint256[] memory feeAmounts,
        bytes memory userData
    ) external override {
        if (msg.sender != BALANCER_VAULT) revert NotBalancer();

        ArbParams memory params = abi.decode(userData, (ArbParams));

        uint256 repayAmount = amounts[0] + feeAmounts[0];
        uint256 balanceBefore = IERC20(WETH).balanceOf(address(this));
        uint256 currentAmount = amounts[0];

        // 循环执行 V3 交易
        for (uint256 i = 0; i < params.steps.length; i++) {
            SwapStep memory step = params.steps[i];

            ISwapRouter.ExactInputSingleParams memory swapParams = ISwapRouter
                .ExactInputSingleParams({
                    tokenIn: step.tokenIn,
                    tokenOut: step.tokenOut,
                    fee: step.fee,
                    recipient: address(this),
                    deadline: block.timestamp,
                    amountIn: currentAmount,
                    amountOutMinimum: 0,
                    sqrtPriceLimitX96: 0
                });

            currentAmount = ISwapRouter(step.router).exactInputSingle(
                swapParams
            );
        }

        uint256 balanceAfter = IERC20(WETH).balanceOf(address(this));
        uint256 required = balanceBefore + feeAmounts[0] + params.minProfit;

        if (balanceAfter < required) {
            revert InsufficientProfit(balanceAfter, required);
        }

        IERC20(WETH).safeTransfer(BALANCER_VAULT, repayAmount);

        uint256 profit = IERC20(WETH).balanceOf(address(this));
        if (profit > 0) {
            IERC20(WETH).safeTransfer(owner(), profit);
        }
    }
}
