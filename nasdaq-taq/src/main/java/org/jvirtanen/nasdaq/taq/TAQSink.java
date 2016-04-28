package org.jvirtanen.nasdaq.taq;

import com.paritytrading.parity.file.taq.TAQ;
import com.paritytrading.parity.file.taq.TAQWriter;
import com.paritytrading.parity.top.MarketListener;
import com.paritytrading.parity.top.Side;
import java.io.Flushable;

class TAQSink implements Flushable, MarketListener {

    private static final long NANOS_PER_MILLI = 1000 * 1000;

    private TAQ.Quote quote;

    private TAQ.Trade trade;

    private TAQWriter writer;

    private long timestampHigh;
    private long timestampLow;

    TAQSink(String date, String instrument) {
        quote = new TAQ.Quote();
        trade = new TAQ.Trade();

        quote.date = date;
        trade.date = date;

        quote.instrument = instrument;
        trade.instrument = instrument;

        writer = new TAQWriter(System.out);
    }

    public void timestamp(long high, long low) {
        timestampHigh = high;
        timestampLow  = low;
    }

    @Override
    public void bbo(long instrument, long bidPrice, long bidSize, long askPrice, long askSize) {
        quote.timestampMillis = timestampMillis();

        quote.bidPrice = bidPrice;
        quote.bidSize  = bidSize;
        quote.askPrice = askPrice;
        quote.askSize  = askSize;

        writer.write(quote);
    }

    @Override
    public void trade(long instrument, Side side, long price, long size) {
        trade.timestampMillis = timestampMillis();

        trade.price = price;
        trade.size  = size;
        trade.side  = side(side);

        writer.write(trade);
    }

    public void trade(long price, long size) {
        trade.timestampMillis = timestampMillis();

        trade.price = price;
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
