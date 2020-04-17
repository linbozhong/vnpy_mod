### 实现功能
1. 策略初始化的时候从tqdata获取历史数据，可以获取可交易数据、指数数据、主力连续合约数据。数据频率以vnpy为准，分钟线只有1分钟的设置参数，要使用多分钟，可用vnpy的BarGenerator类合成。
2. 策略启动后，可接收tqdata的数据推送。

### 文件结构

* tqdata：客户端程序接口，做成符合vnpy范例的自定义gateway
* run_demo.py：客户端入口主程序演示文件
* rpc.py：修改自vnpy官方的rpc模块，将pub-socket的调用放置在一个线程，防止出现潜在的crash
* tqdata_server.py：服务端程序
* TqdataStrategy.py：策略文件模板，把自己的策略类继承自此类

### 使用方式

1. 下载vnpy代码文件，把tqdata目录放置在vnpy/gateway目录下。
2. 在vnpy/example目录下，复制一个可以运行vnpy程序的目录，然后把run_demo.py放进去。
3. 在本机或者服务器运行tqdata_server.py，tcp地址根据自己的需要更改。
4. 用run_demo.py运行客户端，然后连接ctp和tqdata接口，正常运行cta策略程序。

### 其他

* 只经过简单测试，未全方位测试
* 盘中是否能实时添加并运行新的策略，暂时未测试，若要盘中运行新策略，建议同时重新启动服务端和客户端程序。