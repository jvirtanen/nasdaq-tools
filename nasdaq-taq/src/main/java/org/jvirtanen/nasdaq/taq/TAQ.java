package org.jvirtanen.nasdaq.taq;

import static org.jvirtanen.lang.Strings.*;
import static org.jvirtanen.util.Applications.*;

import java.io.File;
import java.io.IOException;
import org.jvirtanen.juncture.nasdaq.itch50.ITCH50Parser;
import org.jvirtanen.nassau.binaryfile.BinaryFILEReader;
import org.jvirtanen.nassau.binaryfile.BinaryFILEStatusListener;
import org.jvirtanen.nassau.binaryfile.BinaryFILEStatusParser;
import org.jvirtanen.parity.top.Market;

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
        TAQSink sink = new TAQSink(date, instrument);

        Market market = new Market(sink);

        ITCH50Parser listener = new ITCH50Parser(new ITCH50Source(market, encodeLong(instrument), sink));

        BinaryFILEStatusListener statusListener = new BinaryFILEStatusListener() {

            @Override
            public void endOfSession() {
            }

        };

        BinaryFILEStatusParser statusParser = new BinaryFILEStatusParser(listener, statusListener);

        BinaryFILEReader reader = BinaryFILEReader.open(file, statusParser);

        market.open(encodeLong(instrument));

        while (reader.read());

        sink.flush();
    }

}
