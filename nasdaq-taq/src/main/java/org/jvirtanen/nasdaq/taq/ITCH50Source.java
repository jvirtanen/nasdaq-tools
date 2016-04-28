package org.jvirtanen.nasdaq.taq;

import com.paritytrading.juncture.nasdaq.itch50.ITCH50;
import com.paritytrading.juncture.nasdaq.itch50.ITCH50Listener;
import com.paritytrading.parity.top.Market;
import com.paritytrading.parity.top.Order;
import com.paritytrading.parity.top.Side;

class ITCH50Source implements ITCH50Listener {

    private Market market;

    private long instrument;

    private TAQSink sink;

    ITCH50Source(Market market, long instrument, TAQSink sink) {
        this.market = market;

        this.instrument = instrument;

        this.sink = sink;
    }

    @Override
    public void systemEvent(ITCH50.SystemEvent message) {
    }

    @Override
    public void stockDirectory(ITCH50.StockDirectory message) {
    }

    @Override
    public void stockTradingAction(ITCH50.StockTradingAction message) {
    }

    @Override
    public void regSHORestriction(ITCH50.RegSHORestriction message) {
    }

    @Override
    public void marketParticipantPosition(ITCH50.MarketParticipantPosition message) {
    }

    @Override
    public void mwcbDeclineLevel(ITCH50.MWCBDeclineLevel message) {
    }

    @Override
    public void mwcbStatus(ITCH50.MWCBStatus message) {
    }

    @Override
    public void ipoQuotingPeriodUpdate(ITCH50.IPOQuotingPeriodUpdate message) {
    }

    @Override
    public void addOrder(ITCH50.AddOrder message) {
        sink.timestamp(message.timestampHigh, message.timestampLow);

        market.add(message.stock, message.orderReferenceNumber,
                side(message.buySellIndicator), message.price, message.shares);
    }

    @Override
    public void addOrderMPID(ITCH50.AddOrderMPID message) {
        sink.timestamp(message.timestampHigh, message.timestampLow);

        market.add(message.stock, message.orderReferenceNumber,
                side(message.buySellIndicator), message.price, message.shares);
    }

    @Override
    public void orderExecuted(ITCH50.OrderExecuted message) {
        sink.timestamp(message.timestampHigh, message.timestampLow);

        market.execute(message.orderReferenceNumber, message.executedShares);
    }

    @Override
    public void orderExecutedWithPrice(ITCH50.OrderExecutedWithPrice message) {
        sink.timestamp(message.timestampHigh, message.timestampLow);

        market.execute(message.orderReferenceNumber, message.executedShares,
                message.executionPrice);
    }

    @Override
    public void orderCancel(ITCH50.OrderCancel message) {
        sink.timestamp(message.timestampHigh, message.timestampLow);

        market.cancel(message.orderReferenceNumber, message.canceledShares);
    }

    @Override
    public void orderDelete(ITCH50.OrderDelete message) {
        sink.timestamp(message.timestampHigh, message.timestampLow);

        market.delete(message.orderReferenceNumber);
    }

    @Override
    public void orderReplace(ITCH50.OrderReplace message) {
        Order order = market.get(message.originalOrderReferenceNumber);
        if (order == null)
            return;

        sink.timestamp(message.timestampHigh, message.timestampLow);

        long instrument = order.getInstrument();
        Side side       = order.getSide();

        market.delete(message.originalOrderReferenceNumber);
        market.add(instrument, message.newOrderReferenceNumber, side,
                message.price, message.shares);
    }

    @Override
    public void trade(ITCH50.Trade message) {
        sink.timestamp(message.timestampHigh, message.timestampLow);

        if (message.stock == instrument)
            sink.trade(message.price, message.shares);
    }

    @Override
    public void crossTrade(ITCH50.CrossTrade message) {
    }

    @Override
    public void brokenTrade(ITCH50.BrokenTrade message) {
    }

    @Override
    public void noii(ITCH50.NOII message) {
    }

    @Override
    public void rpii(ITCH50.RPII message) {
    }

    private Side side(byte buySellIndicator) {
        switch (buySellIndicator) {
        case ITCH50.BUY:
            return Side.BUY;
        case ITCH50.SELL:
            return Side.SELL;
        }

        return null;
    }

}
