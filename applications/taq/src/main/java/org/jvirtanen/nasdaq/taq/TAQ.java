package org.jvirtanen.nasdaq.taq;

import static org.jvirtanen.util.Applications.*;

import com.paritytrading.foundation.ASCII;
import com.paritytrading.juncture.nasdaq.itch50.ITCH50Parser;
import com.paritytrading.nassau.util.BinaryFILE;
import com.paritytrading.parity.book.Market;
import java.io.File;
import java.io.IOException;

class TAQ {

    public static void main(String[] args) {
        if (args.length != 3)
            usage("nasdaq-taq <filename> <date> <instrument>");

        try {
            main(new File(args[0]), args[1], args[2]);
        } catch (IOException e) {
            error(e);
        }
    }

    private static void main(File file, String date, String instrument) throws IOException {
        TAQSink sink   = new TAQSink(date, instrument);
        Market  market = new Market(sink);

        ITCH50Parser listener = new ITCH50Parser(new ITCH50Source(market, ASCII.packLong(instrument), sink));

        market.open(ASCII.packLong(instrument));

        BinaryFILE.read(file, listener);

        sink.flush();
    }

}
