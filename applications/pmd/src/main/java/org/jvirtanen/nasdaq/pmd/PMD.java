package org.jvirtanen.nasdaq.pmd;

import static org.jvirtanen.util.Applications.*;

import com.paritytrading.juncture.nasdaq.itch50.ITCH50Parser;
import com.paritytrading.nassau.util.BinaryFILE;
import java.io.File;
import java.io.IOException;

class PMD {

    public static void main(String[] args) {
        if (args.length != 2)
            usage("nasdaq-pmd <input-filename> <output-filename>");

        try {
            main(new File(args[0]), new File(args[1]));
        } catch (IOException e) {
            error(e);
        }
    }

    private static void main(File inputFile, File outputFile) throws IOException {
        PMDSink sink = new PMDSink(outputFile);

        BinaryFILE.read(inputFile, new ITCH50Parser(sink));

        sink.close();
    }

}
