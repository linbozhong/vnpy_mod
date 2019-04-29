# encoding: UTF-8

from __future__ import division

import math
import pandas as pd

from copy import copy
from collections import OrderedDict
from math import log1p
from datetime import datetime, timedelta

from vnpy.trader.vtConstant import *
from vnpy.trader.vtObject import VtTickData

from .omDate import getTimeToMaturity

# 常量定义
CALL = 1
PUT = -1

OM_DB_NAME = 'VnTrader_OptionMaster_Db'

# 事件定义
EVENT_OM_LOG = 'eOmLog'
EVENT_OM_STRATEGY = 'eOmStrategy.'
EVENT_OM_STRATEGYLOG = 'eOmStrategyLog'


########################################################################
class OmInstrument(VtTickData):
    """交易合约对象"""

    # ----------------------------------------------------------------------
    def __init__(self, contract, detail):
        """Constructor"""
        super(OmInstrument, self).__init__()

        self.tickInited = False

        # 初始化合约信息
        self.symbol = contract.symbol
        self.exchange = contract.exchange
        self.vtSymbol = contract.vtSymbol

        self.size = contract.size
        self.priceTick = contract.priceTick
        self.gatewayName = contract.gatewayName

        # 中间价
        self.midPrice = EMPTY_FLOAT

        # 持仓数据
        self.longPos = 0
        self.shortPos = 0
        self.netPos = 0

        if detail:
            self.longPos = detail.longPos
            self.shortPos = detail.shortPos
            self.netPos = self.longPos - self.shortPos

    # ----------------------------------------------------------------------
    def newTick(self, tick):
        """行情更新"""
        if not self.tickInited:
            self.date = tick.date
            self.openPrice = tick.openPrice
            self.upperLimit = tick.upperLimit
            self.lowerLimit = tick.lowerLimit
            self.tickInited = True

        self.lastPrice = tick.lastPrice
        self.volume = tick.volume
        self.openInterest = tick.openInterest
        self.time = tick.time

        self.bidPrice1 = tick.bidPrice1
        self.askPrice1 = tick.askPrice1
        self.bidVolume1 = tick.bidVolume1
        self.askVolume1 = tick.askVolume1
        self.midPrice = (self.bidPrice1 + self.askPrice1) / 2

    # ----------------------------------------------------------------------
    def newTrade(self, trade):
        """成交更新"""
        if trade.direction is DIRECTION_LONG:
            if trade.offset is OFFSET_OPEN:
                self.longPos += trade.volume
            else:
                self.shortPos -= trade.volume
        else:
            if trade.offset is OFFSET_OPEN:
                self.shortPos += trade.volume
            else:
                self.longPos -= trade.volume

        self.calculateNetPos()

    # ----------------------------------------------------------------------
    def calculateNetPos(self):
        """计算净持仓"""
        newNetPos = self.longPos - self.shortPos

        # 检查净持仓是否发生变化
        if newNetPos != self.netPos:
            netPosChanged = True
            self.netPos = newNetPos
        else:
            netPosChanged = False

        return netPosChanged


########################################################################
class OmUnderlying(OmInstrument):
    """标的物"""

    # ----------------------------------------------------------------------
    def __init__(self, contract, detail, chainList=None):
        """Constructor"""
        super(OmUnderlying, self).__init__(contract, detail)

        # 标的类型
        self.productClass = contract.productClass

        # 以该合约为标的物的期权链字典
        self.chainDict = OrderedDict()

        # 希腊值
        self.theoDelta = EMPTY_FLOAT  # 理论delta值
        self.posDelta = EMPTY_FLOAT  # 持仓delta值

    # ----------------------------------------------------------------------
    def addChain(self, chain):
        """添加以该合约为标的的期权链"""
        self.chainDict[chain.symbol] = chain

    # ----------------------------------------------------------------------
    def newTick(self, tick):
        """行情更新"""
        super(OmUnderlying, self).newTick(tick)

        self.theoDelta = self.size * self.midPrice / 100

        # 遍历推送自己的行情到期权链中
        for chain in self.chainDict.values():
            chain.newUnderlyingTick()

    # ----------------------------------------------------------------------
    def newTrade(self, trade):
        """成交更新"""
        super(OmUnderlying, self).newTrade(trade)
        self.calculatePosGreeks()

    # ----------------------------------------------------------------------
    def calculatePosGreeks(self):
        """计算持仓希腊值"""
        self.posDelta = self.theoDelta * self.netPos


########################################################################
class OmOption(OmInstrument):
    """期权"""

    # ----------------------------------------------------------------------
    def __init__(self, contract, detail, underlying, model, r):
        """Constructor"""
        super(OmOption, self).__init__(contract, detail)

        # 期权属性
        self.underlying = underlying  # 标的物对象
        self.k = contract.strikePrice  # 行权价
        self.r = r  # 利率

        if contract.optionType == OPTION_CALL:
            self.cp = CALL  # 期权类型
        else:
            self.cp = PUT

        self.expiryDate = contract.expiryDate  # 到期日（字符串）
        self.t = getTimeToMaturity(self.expiryDate)  # 剩余时间

        # 波动率属性
        self.bidImpv = EMPTY_FLOAT
        self.askImpv = EMPTY_FLOAT
        self.midImpv = EMPTY_FLOAT

        # 定价公式
        self.calculatePrice = model.calculatePrice
        self.calculateGreeks = model.calculateGreeks
        self.calculateImpv = model.calculateImpv

        # 模型定价
        self.pricingImpv = EMPTY_FLOAT

        self.theoPrice = EMPTY_FLOAT  # 理论价
        self.theoDelta = EMPTY_FLOAT  # 合约的希腊值（乘以了合约大小）
        self.theoGamma = EMPTY_FLOAT
        self.theoTheta = EMPTY_FLOAT
        self.theoVega = EMPTY_FLOAT

        self.posValue = EMPTY_FLOAT  # 持仓市值
        self.posDelta = EMPTY_FLOAT  # 持仓的希腊值（乘以了持仓）
        self.posGamma = EMPTY_FLOAT
        self.posTheta = EMPTY_FLOAT
        self.posVega = EMPTY_FLOAT

        # 期权链
        self.chain = None

    # ----------------------------------------------------------------------
    def calculateOptionImpv(self):
        """计算隐含波动率"""
        underlyingPrice = self.underlying.midPrice
        if not underlyingPrice or not self.t:
            return

        self.askImpv = self.calculateImpv(self.askPrice1, underlyingPrice, self.k,
                                          self.r, self.t, self.cp)
        if self.askImpv > 1:  # 正常情况下波动率不应该超过100%
            self.askImpv = 0.01  # 若超过则大概率为溢出，调整为1%

        self.bidImpv = self.calculateImpv(self.bidPrice1, underlyingPrice, self.k,
                                          self.r, self.t, self.cp)
        if self.bidImpv > 1:
            self.bidImpv = 0.01
        self.midImpv = (self.askImpv + self.bidImpv) / 2

    # ----------------------------------------------------------------------
    def calculateTheoGreeks(self):
        """计算理论希腊值"""
        underlyingPrice = self.underlying.midPrice
        if not underlyingPrice or not self.pricingImpv:
            return

        self.theoPrice, delta, gamma, theta, vega = self.calculateGreeks(underlyingPrice,
                                                                         self.k,
                                                                         self.r,
                                                                         self.t,
                                                                         self.pricingImpv,
                                                                         self.cp)

        self.theoDelta = delta * self.size
        self.theoGamma = gamma * self.size
        self.theoTheta = theta * self.size
        self.theoVega = vega * self.size

    # ----------------------------------------------------------------------
    def calculatePosGreeks(self):
        """计算持仓希腊值"""
        self.posValue = self.theoPrice * self.netPos * self.size
        self.posDelta = self.theoDelta * self.netPos
        self.posGamma = self.theoGamma * self.netPos
        self.posTheta = self.theoTheta * self.netPos
        self.posVega = self.theoVega * self.netPos

    # ----------------------------------------------------------------------
    def newTick(self, tick):
        """行情更新"""
        super(OmOption, self).newTick(tick)
        self.calculateOptionImpv()

    # ----------------------------------------------------------------------
    def newUnderlyingTick(self):
        """标的行情更新"""
        self.calculateOptionImpv()
        self.calculateTheoGreeks()
        self.calculatePosGreeks()

    # ----------------------------------------------------------------------
    def newTrade(self, trade):
        """成交更新"""
        super(OmOption, self).newTrade(trade)
        self.calculatePosGreeks()

    # ----------------------------------------------------------------------
    def setUnderlying(self, underlying):
        """设置标的物对象"""
        self.underlying = underlying

    # ----------------------------------------------------------------------
    def setR(self, r):
        """设置折现率"""
        self.r = r


########################################################################
class OmChain(object):
    """期权链"""

    # ----------------------------------------------------------------------
    def __init__(self, symbol, callList, putList):
        """Constructor"""
        self.symbol = symbol

        # 原始容器
        self.callDict = OrderedDict()
        self.putDict = OrderedDict()
        self.optionDict = OrderedDict()

        for option in callList:
            option.chain = self
            self.callDict[option.symbol] = option
            self.optionDict[option.symbol] = option

        for option in putList:
            option.chain = self
            self.putDict[option.symbol] = option
            self.optionDict[option.symbol] = option

        # 持仓数据
        self.longPos = EMPTY_INT
        self.shortPos = EMPTY_INT
        self.netPos = EMPTY_INT

        self.posValue = EMPTY_FLOAT
        self.posDelta = EMPTY_FLOAT
        self.posGamma = EMPTY_FLOAT
        self.posTheta = EMPTY_FLOAT
        self.posVega = EMPTY_FLOAT

    # ----------------------------------------------------------------------
    def calculatePosGreeks(self):
        """计算持仓希腊值"""
        # 清空数据
        self.longPos = 0
        self.shortPos = 0
        self.netPos = 0
        self.posDelta = 0
        self.posGamma = 0
        self.posTheta = 0
        self.posVega = 0

        # 遍历汇总
        for option in self.optionDict.values():
            self.longPos += option.longPos
            self.shortPos += option.shortPos

            self.posValue += option.posValue
            self.posDelta += option.posDelta
            self.posGamma += option.posGamma
            self.posTheta += option.posTheta
            self.posVega += option.posVega

        self.netPos = self.longPos - self.shortPos

    # ----------------------------------------------------------------------
    def newTick(self, tick):
        """期权行情更新"""
        option = self.optionDict[tick.symbol]
        option.newTick(tick)

    # ----------------------------------------------------------------------
    def newUnderlyingTick(self):
        """期货行情更新"""
        for option in self.optionDict.values():
            option.newUnderlyingTick()

        self.calculatePosGreeks()

    # ----------------------------------------------------------------------
    def newTrade(self, trade):
        """期权成交更新"""
        option = self.optionDict[trade.symbol]

        # 缓存旧数据
        oldLongPos = option.longPos
        oldShortPos = option.shortPos

        oldPosValue = option.posValue
        oldPosDelta = option.posDelta
        oldPosGamma = option.posGamma
        oldPosTheta = option.posTheta
        oldPosVega = option.posVega

        # 更新到期权s中
        option.newTrade(trade)

        # 计算持仓希腊值
        self.longPos = self.longPos - oldLongPos + option.longPos
        self.shortPos = self.shortPos - oldShortPos + option.shortPos
        self.netPos = self.longPos - self.shortPos

        self.posValue = self.posValue - oldPosValue + option.posValue
        self.posDelta = self.posDelta - oldPosDelta + option.posDelta
        self.posGamma = self.posGamma - oldPosGamma + option.posGamma
        self.posTheta = self.posTheta - oldPosTheta + option.posTheta
        self.posVega = self.posVega - oldPosVega + option.posVega

    # ----------------------------------------------------------------------
    def adjustR(self):
        """调整折现率（r）"""
        l = []
        callList = self.callDict.values()
        putList = self.putDict.values()

        # 通过计算期权链所有PCP的平价利率
        for n, call in enumerate(callList):
            put = putList[n]

            # 如果标的为期货，则不进行调整
            if call.underlying.productClass == PRODUCT_FUTURES:
                return

            # 如果有任意中间价为0，则忽略该PCP
            if (not call.underlying.midPrice or
                    not put.midPrice or
                    not call.midPrice or
                    not call.k or
                    not call.t):
                continue

            temp = (call.underlying.midPrice + put.midPrice - call.midPrice) / call.k
            r = log1p(temp - 1) / (-call.t)
            l.append(r)

        # 求平均值来计算拟合折现率
        if not l:
            return

        self.r = sum(l) / len(l)
        for option in self.optionDict.values():
            option.setR(self.r)


########################################################################
class OmPortfolio(object):
    """持仓组合"""

    # ----------------------------------------------------------------------
    def __init__(self, name, model, underlyingList, chainList):
        """Constructor"""
        self.name = name
        self.model = model

        # 原始容器
        self.underlyingDict = OrderedDict()
        self.chainDict = OrderedDict()
        self.optionDict = {}
        self.instrumentDict = {}

        for underlying in underlyingList:
            self.underlyingDict[underlying.symbol] = underlying

        for chain in chainList:
            self.chainDict[chain.symbol] = chain
            self.optionDict.update(chain.callDict)
            self.optionDict.update(chain.putDict)

        self.instrumentDict.update(self.underlyingDict)
        self.instrumentDict.update(self.optionDict)

        # 持仓数据
        self.longPos = EMPTY_INT
        self.shortPos = EMPTY_INT
        self.netPos = EMPTY_INT

        self.posValue = EMPTY_FLOAT
        self.posDelta = EMPTY_FLOAT
        self.posGamma = EMPTY_FLOAT
        self.posTheta = EMPTY_FLOAT
        self.posVega = EMPTY_FLOAT

        self.vixDict = OrderedDict()
        for chain in chainList:
            self.vixDict[chain.symbol] = OmVixCalculator(chain)

    # ----------------------------------------------------------------------
    def calculatePosGreeks(self):
        """计算持仓希腊值"""
        self.longPos = 0
        self.shortPos = 0
        self.netPos = 0

        self.posValue = 0
        self.posDelta = 0
        self.posGamma = 0
        self.posTheta = 0
        self.posVega = 0

        for underlying in self.underlyingDict.values():
            self.posDelta += underlying.posDelta

        for chain in self.chainDict.values():
            self.longPos += chain.longPos
            self.shortPos += chain.shortPos

            self.posValue += chain.posValue
            self.posDelta += chain.posDelta
            self.posGamma += chain.posGamma
            self.posTheta += chain.posTheta
            self.posVega += chain.posVega

        self.netPos = self.longPos - self.shortPos

    # ----------------------------------------------------------------------
    def newTick(self, tick):
        """行情推送"""
        symbol = tick.symbol

        if symbol in self.optionDict:
            chain = self.optionDict[symbol].chain
            chain.newTick(tick)
            self.calculatePosGreeks()
        elif symbol in self.underlyingDict:
            underlying = self.underlyingDict[symbol]
            underlying.newTick(tick)
            self.calculatePosGreeks()

    # ----------------------------------------------------------------------
    def newTrade(self, trade):
        """成交推送"""
        symbol = trade.symbol

        if symbol in self.optionDict:
            chain = self.optionDict[symbol].chain
            chain.newTrade(trade)
            self.calculatePosGreeks()
        elif symbol in self.underlyingDict:
            underlying = self.underlyingDict[symbol]
            underlying.newTrade(trade)
            self.calculatePosGreeks()

    # ----------------------------------------------------------------------
    def adjustR(self):
        """调整折现率"""
        for chain in self.chainDict.values():
            chain.adjustR()

    # 自定义功能
    # ----------------------------------------------------------------------
    def onTimer(self):
        """定时器回调函数"""
        for calculator in self.vixDict.values():
            calculator.onTimer()

    def calcVix(self):
        """计算波动率指数"""
        for calculator in self.vixDict.values():
            calculator.start()


########################################################################
class OmVixCalculator(object):
    def __init__(self, chain):
        self.chain = chain
        self.calls = self.chain.callDict.values()
        self.puts = self.chain.putDict.values()

        self.active = False

        # 计算波动率相关
        self.r = self.chain.optionDict.values()[0].r
        self.ks = [option.k for option in self.calls]

        # 这两个参数定时计算即可，所以可以缓存起来复用
        self.fIdx = EMPTY_INT
        self.t = EMPTY_FLOAT

        # 避免重复计算
        self.f = EMPTY_FLOAT
        self.k0 = EMPTY_FLOAT

        # 定时器容器
        self.timerDict = dict()

    def start(self):
        """开始计算波动率指数"""

        # 先计算剩余时间、轮询option，直到成功计算t
        # while not self.t:
        #     for option in self.calls:
        #         self.calcT(option)
        #         if self.t:
        #             break

        # 启动onTimer
        self.active = True
        print('Start calculate vix..')

    def stop(self):
        """停止计算波动率指数"""
        if self.active:
            self.active = False

    def calcT(self, option):
        """计算期权合约剩余到期时间（以分钟计并且年化）"""
        if option.time and option.date:
            print(option.time, option.date)
            hour = int(option.time[0:2])
            minute = int(option.time[3:5])
            m1 = (24 - hour) * 60 - minute  # 当前时间距离当日24点的剩余分钟

            nowDate = datetime.strptime(option.date, '%Y%m%d').date()
            expiryDate = datetime.strptime(option.expiryDate, '%Y%m%d').date()
            m2 = ((expiryDate - nowDate).days - 1) * 24 * 60  # 中间天数的剩余分钟

            m3 = 9.5 * 60  # 到期日当天0点到9点30的剩余分钟
            self.t = float(m1 + m2 + m3) / float(365 * 24 * 60)  # 所有剩余分钟的年化
            # print('VixT:', self.t)

    def calcFIdx(self):
        """计算远期指数F对应的期权行权价所在的index"""
        calls = self.calls
        puts = self.puts

        l = []
        for i in range(0, len(calls)):
            d = dict()
            d['strikePrice'] = calls[i].k
            d['callPrice'] = calls[i].lastPrice
            d['putPrice'] = puts[i].lastPrice
            l.append(d)
        df = pd.DataFrame(l)
        spread = abs(df['callPrice'] - df['putPrice'])
        self.fIdx = spread.idxmin()
        # print('vixFIdx:{} Strike:{}'.format(self.fIdx, calls[self.fIdx].k))

    def calcF(self):
        """计算远期指数F"""
        if self.fIdx and self.t:
            calls = self.calls
            puts = self.puts

            fK = calls[self.fIdx].k
            spread = abs(calls[self.fIdx].lastPrice - puts[self.fIdx].lastPrice)
            f = fK + spread * math.exp(self.r * self.t)
            # print('vixF:', f)
            self.f = f
            return f

    def calcK0(self):
        """计算K0行权价"""
        if self.f:
            ks = self.ks
            f = self.f
            i = 0
            while ks[i] < f:
                i += 1
            k0 = ks[i - 1]
            # print('vixK0:', k0)
            self.k0 = k0
            return k0

    def calcDeltaK(self):
        """计算行权价档差"""
        deltaK = {}
        ks = self.ks
        deltaK[ks[0]] = ks[1] - ks[0]
        deltaK[ks[-1]] = ks[-1] - ks[-2]
        for i in range(1, len(ks) - 1):
            deltaK[ks[i]] = (ks[i + 1] - ks[i - 1]) / 2
        return deltaK

    def calcQK(self):
        """计算波动率指数各档行权价期权的价格序列字典"""
        if self.k0:
            qK = {}
            calls = self.calls
            puts = self.puts
            k0 = self.k0
            for i, k in enumerate(self.ks):
                if k < k0:
                    qK[k] = puts[i].midPrice
                elif k > k0:
                    qK[k] = calls[i].midPrice
                else:
                    qK[k] = (calls[i].midPrice + puts[i].midPrice) / 2
            return qK

    def calSigma2(self):
        """计算某一个时刻，某一到期期限的波动率"""
        if self.t and self.fIdx:
            # 不需要计算
            r = self.r
            ks = self.ks

            # 无需实时计算
            t = self.t
            deltaK = self.calcDeltaK()

            # 不要重复计算
            f = self.calcF()
            k0 = self.calcK0()

            # 需要实时计算
            qK = self.calcQK()

            sum_K = sum(deltaK[k] / k ** 2 * qK[k] * math.exp(r * t) for k in ks)
            sigma2 = 2 * sum_K / t - (f / k0 - 1) ** 2 / t
            print('{}-Vix:{}'.format(self.chain.symbol, 100 * math.sqrt(sigma2)))
            return sigma2

    def onTimer(self):
        """定时事件回调函数"""
        if self.active:
            option = self.chain.optionDict.values()[0]
            if not self.t:
                self.timer('vixT', 1, self.calcT, option)
            else:
                self.timer('vixT', 60, self.calcT, option)

            if not self.fIdx:
                self.timer('vixFIdx', 1, self.calcFIdx)
            else:
                self.timer('vixFIdx', 12, self.calcFIdx)

            self.timer('vix', 3, self.calSigma2)

    def timer(self, name, interval, func, *args, **kwargs):
        """自定义定时器"""
        if self.timerDict.get(name) is None:
            self.timerDict[name] = 0

        if self.timerDict[name] > interval:
            func(*args, **kwargs)
            self.timerDict[name] = 0

        self.timerDict[name] += 1
