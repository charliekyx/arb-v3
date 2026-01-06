// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";

// --- 接口定义 ---
interface ISwapRouter {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    function exactInputSingle(
        ExactInputSingleParams calldata params
    ) external payable returns (uint256 amountOut);
}

interface IUniswapV2Router {
    function swapExactTokensForTokens(
        uint amountIn,
        uint amountOutMin,
        address[] calldata path,
        address to,
        uint deadline
    ) external returns (uint[] memory amounts);
}

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

    address private constant BALANCER_VAULT =
        0xBA12222222228d8Ba445958a75a0704d566BF2C8;

    address public executor;

    struct SwapStep {
        address router;
        address tokenIn;
        address tokenOut;
        uint24 fee; // V3用
        uint8 protocol; // 0 = V3, 1 = V2, 2 = CL (Same as V3)
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

    function executeArb(
        uint256 borrowAmount,
        SwapStep[] calldata steps,
        uint256 minProfit
    ) external onlyExecutorOrOwner returns (uint256) {
        // Added return type logic for ABI match
        bytes memory userData = abi.encode(
            ArbParams({
                borrowAmount: borrowAmount,
                steps: steps,
                minProfit: minProfit
            })
        );

        // 借款代币 = 第一步交易的输入代币
        address borrowToken = steps[0].tokenIn;

        IERC20[] memory tokens = new IERC20[](1);
        tokens[0] = IERC20(borrowToken);
        uint256[] memory amounts = new uint256[](1);
        amounts[0] = borrowAmount;

        IVault(BALANCER_VAULT).flashLoan(
            IFlashLoanRecipient(address(this)),
            tokens,
            amounts,
            userData
        );

        return 0; // Return dummy value to satisfy ABI
    }

    function receiveFlashLoan(
        IERC20[] memory tokens,
        uint256[] memory amounts,
        uint256[] memory feeAmounts,
        bytes memory userData
    ) external override {
        if (msg.sender != BALANCER_VAULT) revert NotBalancer();

        ArbParams memory params = abi.decode(userData, (ArbParams));

        //  获取借入的代币地址
        IERC20 borrowedToken = tokens[0];

        uint256 repayAmount = amounts[0] + feeAmounts[0];
        uint256 balanceBefore = borrowedToken.balanceOf(address(this));
        uint256 currentAmount = amounts[0];

        // 循环执行交易
        for (uint256 i = 0; i < params.steps.length; i++) {
            SwapStep memory step = params.steps[i];

            uint256 currentAllowance = IERC20(step.tokenIn).allowance(
                address(this),
                step.router
            );

            if (currentAllowance < currentAmount) {
                if (currentAllowance > 0) {
                    IERC20(step.tokenIn).approve(step.router, 0);
                }
                IERC20(step.tokenIn).approve(step.router, type(uint256).max);
            }

            if (step.protocol == 1) {
                // --- V2 (Aerodrome Classic) ---
                address[] memory path = new address[](2);
                path[0] = step.tokenIn;
                path[1] = step.tokenOut;

                uint[] memory amountsOut = IUniswapV2Router(step.router)
                    .swapExactTokensForTokens(
                        currentAmount,
                        0,
                        path,
                        address(this),
                        block.timestamp
                    );
                currentAmount = amountsOut[amountsOut.length - 1];
            } else {
                bool zeroForOne = step.tokenIn < step.tokenOut;
                uint160 sqrtPriceLimitX96 = 0;

                if (zeroForOne) {
                    sqrtPriceLimitX96 = 4295128740;
                } else {
                    sqrtPriceLimitX96 = 1461446703485210103287273052203988822378723970341; // TickMath.MAX_SQRT_RATIO - 1
                }

                ISwapRouter.ExactInputSingleParams memory swapParams = ISwapRouter
                    .ExactInputSingleParams({
                        tokenIn: step.tokenIn,
                        tokenOut: step.tokenOut,
                        fee: step.fee,
                        recipient: address(this),
                        amountIn: currentAmount,
                        amountOutMinimum: 0,
                        sqrtPriceLimitX96: sqrtPriceLimitX96 // [修复] 传入计算好的有效值
                    });
                currentAmount = ISwapRouter(step.router).exactInputSingle(
                    swapParams
                );
            }
        }

        // 检查借入代币的余额是否足够还款 + 利润
        uint256 balanceAfter = borrowedToken.balanceOf(address(this));
        uint256 required = balanceBefore + feeAmounts[0] + params.minProfit;

        if (balanceAfter < required) {
            revert InsufficientProfit(balanceAfter, required);
        }

        // 还款给 Balancer
        borrowedToken.safeTransfer(BALANCER_VAULT, repayAmount);

        // 提取利润给 Owner
        // 注意：这里只提取了借入代币的利润。如果中间产生了其他代币的粉尘，需另行提取。
        uint256 profit = borrowedToken.balanceOf(address(this));
        if (profit > 0) {
            borrowedToken.safeTransfer(owner(), profit);
        }
    }
}
