package org.jvirtanen.nasdaq.taq;

import com.paritytrading.parity.book.MarketListener;
import com.paritytrading.parity.book.OrderBook;
import com.paritytrading.parity.book.Side;
import com.paritytrading.parity.file.taq.TAQ;
import com.paritytrading.parity.file.taq.TAQWriter;
import java.io.Flushable;

class TAQSink implements Flushable, MarketListener {

    private static final long NANOS_PER_MILLI = 1_000_000;

    private static final double PRICE_FACTOR = 10000.0;

    private TAQ.Quote quote;
    private TAQ.Trade trade;

    private TAQWriter writer;

    private long timestampHigh;
    private long timestampLow;

    public TAQSink(String date, String instrument) {
        quote = new TAQ.Quote();
        trade = new TAQ.Trade();

        quote.date = date;
        trade.date = date;

        quote.instrument = instrument;
        trade.instrument = instrument;

        writer = new TAQWriter(System.out);

        timestampHigh = 0;
        timestampLow  = 0;
    }

    public void timestamp(long high, long low) {
        timestampHigh = high;
        timestampLow  = low;
    }

    @Override
    public void update(OrderBook book, boolean bbo) {
        if (!bbo)
            return;

        long bidPrice = book.getBestBidPrice();
        long askPrice = book.getBestAskPrice();

        quote.timestampMillis = timestampMillis();

        quote.bidPrice = bidPrice / PRICE_FACTOR;
        quote.bidSize  = book.getBidSize(bidPrice);
        quote.askPrice = askPrice / PRICE_FACTOR;
        quote.askSize  = book.getAskSize(askPrice);

        writer.write(quote);
    }

    @Override
    public void trade(OrderBook book, Side side, long price, long size) {
        trade.timestampMillis = timestampMillis();

        trade.price = price / PRICE_FACTOR;
        trade.size  = size;
        trade.side  = side(side);

        writer.write(trade);
    }

    public void trade(long price, long size) {
        trade.timestampMillis = timestampMillis();

        trade.price = price / PRICE_FACTOR;
        trade.size  = size;
        trade.side  = TAQ.UNKNOWN;

        writer.write(trade);
    }

    @Override
    public void flush() {
        writer.flush();
    }

    private long timestampMillis() {
        return ((timestampHigh << 32) + timestampLow) / NANOS_PER_MILLI;
    }

    private char side(Side side) {
        switch (side) {
        case BUY:
            return TAQ.BUY;
        case SELL:
            return TAQ.SELL;
        }

        return TAQ.UNKNOWN;
    }

}
