# encoding: UTF-8

from vnpy.trader.vtConstant import (DIRECTION_LONG, DIRECTION_SHORT,
                                    OFFSET_OPEN, OFFSET_CLOSE,
                                    EMPTY_UNICODE)

TICK_DB_NAME = 'VnTrader_Tick_Db'
MINUTE_DB_NAME = 'VnTrader_1Min_Db'

########################################################################
class OmStrategyTemplate(object):
    """策略模板"""
    className = 'OmStrategyTemplate'
    author = EMPTY_UNICODE
    name = EMPTY_UNICODE  # 策略实例名称
    vtSymbols = []  # 交易的合约vt系统代码
    chainSymbol = EMPTY_UNICODE

    tickDbName = TICK_DB_NAME
    barDbName = MINUTE_DB_NAME

    inited = False  # 是否进行了初始化
    trading = False  # 是否启动交易，由引擎管理

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbols']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading']

    # ----------------------------------------------------------------------
    def __init__(self, engine, setting):
        """Constructor"""
        self.engine = engine

        self.vtSymbols = []

        # 设置策略的参数
        for key in self.paramList:
            if key in setting:
                self.__setattr__(key, setting[key])

    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def onStart(self):
        """启动"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def onStop(self):
        """停止"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def onTick(self, tick):
        """行情推送"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        """成交推送"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """委托推送"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def onTimer(self):
        """定时推送"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def onVixTick(self, vixTick):
        """vixTick推送"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def onVixBar(self, vixBar):
        """vixBar推送"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def loadVixTick(self, days):
        """读取tick数据"""
        return self.engine.loadVixTick(self.tickDbName, self.chainSymbol, days)

    # ----------------------------------------------------------------------
    def loadVixBar(self, days):
        """读取bar数据"""
        return self.engine.loadVixBar(self.barDbName, self.chainSymbol, days)

    # ----------------------------------------------------------------------
    def cancelOrder(self, vtOrderID):
        """撤单"""
        self.engine.cancelOrder(vtOrderID)

    # ----------------------------------------------------------------------
    def buy(self, vtSymbol, price, volume):
        """开多"""
        if self.trading:
            return self.engine.buy(vtSymbol, price, volume)
        else:
            return []

    # ----------------------------------------------------------------------
    def short(self, vtSymbol, price, volume):
        """开空"""
        return self.engine.short(vtSymbol, price, volume)

    # ----------------------------------------------------------------------

    def sell(self, vtSymbol, price, volume):
        """平多"""
        return self.engine.sell(vtSymbol, price, volume)

    # ----------------------------------------------------------------------

    def cover(self, vtSymbol, price, volume):
        """平空"""
        return self.engine.cover(vtSymbol, price, volume)

    # ----------------------------------------------------------------------

    def dbQuery(self, flt):
        """查询数据"""
        return self.engine.dbQuery(self.name, flt)

    # ----------------------------------------------------------------------
    def dbUpdate(self, d, flt):
        """更新数据"""
        return self.engine.dbUpdate(self.name, d, flt)

    # ----------------------------------------------------------------------
    def getOption(self, vtSymbol):
        """查询期权数据"""
        return self.engine.getOption(vtSymbol)

    # ----------------------------------------------------------------------
    def getUnderlying(self, vtSymbol):
        """获取标的信息"""
        return self.engine.getUnderlying(vtSymbol)

    # ----------------------------------------------------------------------
    def getChain(self, symbol):
        """获取期权链信息"""
        return self.engine.getChain(symbol)

    # ----------------------------------------------------------------------
    def getPortfolio(self):
        """获取持仓组合信息"""
        return self.engine.getPortfolio()

    # ----------------------------------------------------------------------
    def putEvent(self):
        """发出GUI更新通知"""
        self.engine.putStrategyEvent(self.name)

    # ----------------------------------------------------------------------
    def writeLog(self, content):
        """记录日志"""
        content = '%s:%s' % (self.name, content)
        self.engine.writeLog(content)

    # ----------------------------------------------------------------------
    def addTickEvent(self, vtSymbol):
        """把合约加入symbolStrategy列表，使得可以用onTick推送对应的行情"""
        strategyList = self.engine.symbolStrategyDict.get(vtSymbol)
        if strategyList is None:
            self.engine.symbolStrategyDict[vtSymbol] = [self]
        else:
            if self not in strategyList:
                strategyList.append(self)
        # print(strategyList)

    # ----------------------------------------------------------------------
    def getAtmContract(self, chainSymbol):
        """通过gamma值找到平值购和平值沽的合约代码"""
        chain = self.getChain(chainSymbol)
        callMaxGamma = 0
        atmCallOptionSymbol = None
        putMaxGamma = 0
        atmPutOptionSymbol = None
        for option in chain.callDict.values():
            if option.theoGamma >= callMaxGamma:
                callMaxGamma = option.theoGamma
                atmCallOptionSymbol = option.vtSymbol
            else:
                break
        for option in chain.putDict.values():
            if option.theoGamma >= putMaxGamma:
                putMaxGamma = option.theoGamma
                atmPutOptionSymbol = option.vtSymbol
            else:
                break

        self.addTickEvent(atmCallOptionSymbol)
        self.addTickEvent(atmPutOptionSymbol)
        return atmCallOptionSymbol, atmPutOptionSymbol
