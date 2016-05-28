package org.jvirtanen.nasdaq.taq

import com.paritytrading.parity.file.taq.TAQ
import com.paritytrading.parity.file.taq.TAQWriter
import com.paritytrading.parity.top.MarketListener
import com.paritytrading.parity.top.Side
import java.io.Flushable

val NANOS_PER_MILLI = 1000 * 1000

class TAQSink(date: String, instrument: String): Flushable, MarketListener {

    private val quote = TAQ.Quote()
    private val trade = TAQ.Trade()

    private val writer = TAQWriter(System.out)

    private var timestampHigh = 0L
    private var timestampLow  = 0L

    init {
        quote.date = date
        trade.date = date

        quote.instrument = instrument
        trade.instrument = instrument
    }

    fun timestamp(high: Int, low: Long) {
        timestampHigh = high.toLong()
        timestampLow  = low
    }

    override fun bbo(instrument: Long, bidPrice: Long, bidSize: Long, askPrice: Long, askSize: Long) {
        quote.timestampMillis = timestampMillis()

        quote.bidPrice = bidPrice
        quote.bidSize  = bidSize
        quote.askPrice = askPrice
        quote.askSize  = askSize

        writer.write(quote)
    }

    override fun trade(instrument: Long, side: Side, price: Long, size: Long) {
        trade.timestampMillis = timestampMillis()

        trade.price = price
        trade.size  = size
        trade.side  = side(side)

        writer.write(trade)
    }

    fun trade(price: Long, size: Long) {
        trade.timestampMillis = timestampMillis()

        trade.price = price
        trade.size  = size
        trade.side  = TAQ.UNKNOWN

        writer.write(trade)
    }

    override fun flush() {
        writer.flush()
    }

    private fun timestampMillis(): Long {
        return ((timestampHigh shl 32) + timestampLow) / NANOS_PER_MILLI
    }

    private fun side(side: Side): Char {
        when (side) {
            Side.BUY  -> return TAQ.BUY
            Side.SELL -> return TAQ.SELL
            else      -> return TAQ.UNKNOWN
        }
    }

}
