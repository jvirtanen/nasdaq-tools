package org.jvirtanen.nasdaq.taq

import com.paritytrading.juncture.nasdaq.itch50.ITCH50
import com.paritytrading.juncture.nasdaq.itch50.ITCH50Listener
import com.paritytrading.parity.top.Market
import com.paritytrading.parity.top.Order
import com.paritytrading.parity.top.Side

class ITCH50Source(val market: Market, val instrument: Long, val sink: TAQSink): ITCH50Listener {

    override fun systemEvent(message: ITCH50.SystemEvent?) {
    }

    override fun stockDirectory(message: ITCH50.StockDirectory?) {
    }

    override fun stockTradingAction(message: ITCH50.StockTradingAction?) {
    }

    override fun regSHORestriction(message: ITCH50.RegSHORestriction?) {
    }

    override fun marketParticipantPosition(message: ITCH50.MarketParticipantPosition?) {
    }

    override fun mwcbDeclineLevel(message: ITCH50.MWCBDeclineLevel?) {
    }

    override fun mwcbStatus(message: ITCH50.MWCBStatus?) {
    }

    override fun ipoQuotingPeriodUpdate(message: ITCH50.IPOQuotingPeriodUpdate?) {
    }

    override fun addOrder(message: ITCH50.AddOrder) {
        sink.timestamp(message.timestampHigh, message.timestampLow)

        market.add(message.stock, message.orderReferenceNumber,
                side(message.buySellIndicator), message.price, message.shares)
    }

    override fun addOrderMPID(message: ITCH50.AddOrderMPID) {
        sink.timestamp(message.timestampHigh, message.timestampLow)

        market.add(message.stock, message.orderReferenceNumber,
                side(message.buySellIndicator), message.price, message.shares)
    }

    override fun orderExecuted(message: ITCH50.OrderExecuted) {
        sink.timestamp(message.timestampHigh, message.timestampLow)

        market.execute(message.orderReferenceNumber, message.executedShares)
    }

    override fun orderExecutedWithPrice(message: ITCH50.OrderExecutedWithPrice) {
        sink.timestamp(message.timestampHigh, message.timestampLow)

        market.execute(message.orderReferenceNumber, message.executedShares,
                message.executionPrice)
    }

    override fun orderCancel(message: ITCH50.OrderCancel) {
        sink.timestamp(message.timestampHigh, message.timestampLow)

        market.cancel(message.orderReferenceNumber, message.canceledShares)
    }

    override fun orderDelete(message: ITCH50.OrderDelete) {
        sink.timestamp(message.timestampHigh, message.timestampLow)

        market.delete(message.orderReferenceNumber)
    }

    override fun orderReplace(message: ITCH50.OrderReplace) {
        val order = market.find(message.originalOrderReferenceNumber)
        if (order == null)
            return

        sink.timestamp(message.timestampHigh, message.timestampLow)

        val instrument = order.getInstrument()
        val side       = order.getSide()

        market.delete(message.originalOrderReferenceNumber)
        market.add(instrument, message.newOrderReferenceNumber, side,
                message.price, message.shares)
    }

    override fun trade(message: ITCH50.Trade) {
        sink.timestamp(message.timestampHigh, message.timestampLow)

        if (message.stock == instrument)
            sink.trade(message.price, message.shares)
    }

    override fun crossTrade(message: ITCH50.CrossTrade?) {
    }

    override fun brokenTrade(message: ITCH50.BrokenTrade?) {
    }

    override fun noii(message: ITCH50.NOII?) {
    }

    override fun rpii(message: ITCH50.RPII?) {
    }

    private fun side(buySellIndicator: Byte): Side? {
        when (buySellIndicator) {
            ITCH50.BUY  -> return Side.BUY
            ITCH50.SELL -> return Side.SELL
            else        -> return null
        }
    }

}
