# rust-arbitrage-searcher
## 部署流程
```bash
准备账户:
    Account A (Owner): 主钱包，有钱。
    Account B (Bot): 新生成的私钥，没钱（或者只有一点点 Gas）。
部署合约:
    使用 Account A 部署上述合约。
    此时 owner 是 Account A。
授权 Bot:
    使用 Account A 调用合约的 setExecutor 函数。
    参数填入 Account B 的地址。
运行 Bot:
    在服务器上，.env 文件里填 Account B 的私钥。
    Bot 启动后，会使用 Account B 签名交易并发送。
    合约检查 msg.sender == executor，检查通过，交易执行。
```
