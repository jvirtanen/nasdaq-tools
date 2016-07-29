package org.jvirtanen.nasdaq.taq

import org.jvirtanen.util.Applications.*

import com.paritytrading.foundation.ASCII
import com.paritytrading.juncture.nasdaq.itch50.ITCH50Parser
import com.paritytrading.nassau.util.BinaryFILE
import com.paritytrading.parity.top.Market
import java.io.File
import java.io.IOException

fun main(args: Array<String>) {
    if (args.size != 3)
        usage("nasdaq-taq <filename> <date> <instrument>")

    try {
        main(File(args[0]), args[1], args[2])
    } catch (e: IOException) {
        error(e)
    }
}

private fun main(file: File, date: String, instrument: String) {
    val sink   = TAQSink(date, instrument)
    val market = Market(sink)

    val listener = ITCH50Parser(ITCH50Source(market, ASCII.packLong(instrument), sink))

    market.open(ASCII.packLong(instrument))

    BinaryFILE.read(file, listener)

    sink.flush()
}
