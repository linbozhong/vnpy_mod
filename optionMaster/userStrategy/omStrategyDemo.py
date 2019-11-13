# encoding: UTF-8

# from vnpy.trader.app.optionMaster.omStrategy import OmStrategyTemplate
from vnpy.trader.vtUtility import ArrayManager, BarGenerator

# 改成载入本地目录的omStrategy模块，否则会引用安装目录的模块，从而引发动态引用找不到模块的错误
from omStrategy import OmStrategyTemplate


#######################################################################
class DemoStrategy(OmStrategyTemplate):
    """演示策略"""
    className = 'DemoStrategy'
    author = u'本地的用Python的交易员'

    temp = 123

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbols',
                 'temp',
                 'chainSymbol']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading']

    # ----------------------------------------------------------------------
    def __init__(self, engine, setting):
        """Constructor"""
        self.chainSymbol = ''

        super(DemoStrategy, self).__init__(engine, setting)

        self.timeInterval = 10
        self.timeCount = 0

    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化"""
        self.writeLog(u'%s策略初始化' % self.name)
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStart(self):
        """启动"""
        self.writeLog(u'%s策略启动' % self.name)
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStop(self):
        """停止"""
        self.writeLog(u'%s策略停止' % self.name)
        self.putEvent()

    # ----------------------------------------------------------------------
    def onTick(self, tick):
        """行情推送"""
        # self.writeLog(u'%s策略收到行情推送' % self.name)
        self.writeLog(u'%s：最新价：%s, 涨跌停价：%s %s' % (tick.symbol, tick.lastPrice, tick.upperLimit, tick.lowerLimit))
        self.putEvent()

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        """成交推送"""
        self.writeLog(u'%s策略收到成交推送' % self.name)
        self.putEvent()

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """委托推送"""
        self.writeLog(u'%s策略收到委托推送' % self.name)
        self.putEvent()

    # ----------------------------------------------------------------------
    def onTimer(self):
        """定时推送"""
        self.timeCount += 1
        if self.timeCount > self.timeInterval:
            self.writeLog(u'%s策略收到定时推送，自定义参数%s' % (self.name, self.temp))
            call, put = self.getAtmContract(self.chainSymbol)
            self.writeLog(u'平值购:%s, 平值沽:%s' % (call, put))
            self.timeCount = 0

    def onVixTick(self, vixTick):
        print('Strategy onVixTick:', vixTick.lastPrice)


    def onVixBar(self, vixBar):
        pass
        # print('Strategy onVixTick:', vixTick.lastPrice)