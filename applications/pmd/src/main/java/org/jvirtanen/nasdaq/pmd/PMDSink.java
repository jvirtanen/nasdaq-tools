package org.jvirtanen.nasdaq.pmd;

import com.paritytrading.juncture.nasdaq.itch50.ITCH50;
import com.paritytrading.juncture.nasdaq.itch50.ITCH50Listener;
import com.paritytrading.nassau.binaryfile.BinaryFILEWriter;
import com.paritytrading.parity.net.pmd.PMD;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

class PMDSink implements Closeable, ITCH50Listener {

    private static final int NANOS_PER_SECOND = 1_000_000_000;

    private static final int BUFFER_CAPACITY = 64;

    private PMD.Version       version;
    private PMD.Seconds       seconds;
    private PMD.OrderAdded    orderAdded;
    private PMD.OrderExecuted orderExecuted;
    private PMD.OrderCanceled orderCanceled;
    private PMD.OrderDeleted  orderDeleted;
    private PMD.BrokenTrade   brokenTrade;

    private long currentSecond;

    private Long2LongOpenHashMap instrument;

    private Long2ByteOpenHashMap side;

    private ByteBuffer buffer;

    private BinaryFILEWriter writer;

    public PMDSink(File file) throws IOException {
        version       = new PMD.Version();
        seconds       = new PMD.Seconds();
        orderAdded    = new PMD.OrderAdded();
        orderExecuted = new PMD.OrderExecuted();
        orderCanceled = new PMD.OrderCanceled();
        orderDeleted  = new PMD.OrderDeleted();
        brokenTrade   = new PMD.BrokenTrade();

        currentSecond = 0;

        instrument = new Long2LongOpenHashMap();

        side = new Long2ByteOpenHashMap();

        buffer = ByteBuffer.allocate(BUFFER_CAPACITY);

        writer = BinaryFILEWriter.open(file);
    }

    @Override
    public void close() throws IOException {
        writer.close();
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
    public void addOrder(ITCH50.AddOrder message) throws IOException {
        orderAdded.timestamp   = timestamp(message.timestampHigh, message.timestampLow);
        orderAdded.orderNumber = message.orderReferenceNumber;
        orderAdded.side        = side(message.buySellIndicator);
        orderAdded.instrument  = message.stock;
        orderAdded.quantity    = message.shares;
        orderAdded.price       = message.price;

        write(message.timestampHigh, message.timestampLow, orderAdded);

        side.put(orderAdded.orderNumber, orderAdded.side);
        instrument.put(orderAdded.orderNumber, orderAdded.instrument);
    }

    @Override
    public void addOrderMPID(ITCH50.AddOrderMPID message) throws IOException {
        orderAdded.timestamp   = timestamp(message.timestampHigh, message.timestampLow);
        orderAdded.orderNumber = message.orderReferenceNumber;
        orderAdded.side        = side(message.buySellIndicator);
        orderAdded.instrument  = message.stock;
        orderAdded.quantity    = message.shares;
        orderAdded.price       = message.price;

        write(message.timestampHigh, message.timestampLow, orderAdded);

        side.put(orderAdded.orderNumber, orderAdded.side);
        instrument.put(orderAdded.orderNumber, orderAdded.instrument);
    }

    @Override
    public void orderExecuted(ITCH50.OrderExecuted message) throws IOException {
        orderExecuted.timestamp   = timestamp(message.timestampHigh, message.timestampLow);
        orderExecuted.orderNumber = message.orderReferenceNumber;
        orderExecuted.quantity    = message.executedShares;
        orderExecuted.matchNumber = message.matchNumber;

        write(message.timestampHigh, message.timestampLow, orderExecuted);
    }

    @Override
    public void orderExecutedWithPrice(ITCH50.OrderExecutedWithPrice message) throws IOException {
        orderExecuted.timestamp   = timestamp(message.timestampHigh, message.timestampLow);
        orderExecuted.orderNumber = message.orderReferenceNumber;
        orderExecuted.quantity    = message.executedShares;
        orderExecuted.matchNumber = message.matchNumber;

        write(message.timestampHigh, message.timestampLow, orderExecuted);
    }

    @Override
    public void orderCancel(ITCH50.OrderCancel message) throws IOException {
        orderCanceled.timestamp        = timestamp(message.timestampHigh, message.timestampLow);
        orderCanceled.orderNumber      = message.orderReferenceNumber;
        orderCanceled.canceledQuantity = message.canceledShares;

        write(message.timestampHigh, message.timestampLow, orderCanceled);
    }

    @Override
    public void orderDelete(ITCH50.OrderDelete message) throws IOException {
        orderDeleted.timestamp   = timestamp(message.timestampHigh, message.timestampLow);
        orderDeleted.orderNumber = message.orderReferenceNumber;

        write(message.timestampHigh, message.timestampLow, orderDeleted);

        side.remove(orderDeleted.orderNumber);
        instrument.remove(orderDeleted.orderNumber);
    }

    @Override
    public void orderReplace(ITCH50.OrderReplace message) throws IOException {
        orderDeleted.timestamp   = timestamp(message.timestampHigh, message.timestampLow);
        orderDeleted.orderNumber = message.originalOrderReferenceNumber;

        write(message.timestampHigh, message.timestampLow, orderDeleted);

        orderAdded.timestamp   = timestamp(message.timestampHigh, message.timestampLow);
        orderAdded.orderNumber = message.newOrderReferenceNumber;
        orderAdded.side        = side.get(message.originalOrderReferenceNumber);
        orderAdded.instrument  = instrument.get(message.originalOrderReferenceNumber);
        orderAdded.quantity    = message.shares;
        orderAdded.price       = message.price;

        write(message.timestampHigh, message.timestampLow, orderAdded);

        side.remove(orderDeleted.orderNumber);
        instrument.remove(orderDeleted.orderNumber);

        side.put(orderAdded.orderNumber, orderAdded.side);
        instrument.put(orderAdded.orderNumber, orderAdded.instrument);
    }

    @Override
    public void trade(ITCH50.Trade message) {
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

    private void write(long timestampHigh, long timestampLow, PMD.Message message) throws IOException {
        long second = second(timestampHigh, timestampLow);

        if (currentSecond != second) {
            currentSecond = second;

            seconds.second = second;

            buffer.clear();
            seconds.put(buffer);
            buffer.flip();

            writer.write(buffer);
        }

        buffer.clear();
        message.put(buffer);
        buffer.flip();

        writer.write(buffer);
    }

    private long second(long timestampHigh, long timestampLow) {
        return ((timestampHigh << 32) + timestampLow) / NANOS_PER_SECOND;
    }

    private long timestamp(long timestampHigh, long timestampLow) {
        return ((timestampHigh << 32) + timestampLow) % NANOS_PER_SECOND;
    }

    private byte side(byte buySellIndicator) {
        return buySellIndicator == ITCH50.BUY ? PMD.BUY : PMD.SELL;
    }

}
