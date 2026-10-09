package com.renaissance.pipeline.buffer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.renaissance.pipeline.buffer.ReorderBuffer.TakeResult;
import com.renaissance.pipeline.model.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(value = 10, unit = TimeUnit.SECONDS)
class ReorderBufferTest {

    @Test
    void inOrderPutThenTakeYieldsSeqOrder() throws Exception {
        ReorderBuffer buf = new ReorderBuffer(8);
        buf.put(0, msg(0));
        buf.put(1, msg(1));
        buf.put(2, msg(2));

        assertData(buf.take(), 0);
        assertData(buf.take(), 1);
        assertData(buf.take(), 2);
    }

    @Test
    void outOfOrderPutsYieldStrictSeqOrderOnTake() throws Exception {
        ReorderBuffer buf = new ReorderBuffer(8);
        buf.put(2, msg(2));
        buf.put(0, msg(0));
        buf.put(1, msg(1));

        assertData(buf.take(), 0);
        assertData(buf.take(), 1);
        assertData(buf.take(), 2);
    }

    @Test
    void takeBlocksUntilNextExpectedArrives() throws Exception {
        ReorderBuffer buf = new ReorderBuffer(8);
        CountDownLatch takeStarted = new CountDownLatch(1);
        CountDownLatch takeDone = new CountDownLatch(1);
        AtomicReference<TakeResult> result = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();

        Thread taker = new Thread(() -> {
            try {
                takeStarted.countDown();
                result.set(buf.take());
                takeDone.countDown();
            } catch (Throwable t) {
                error.set(t);
                takeDone.countDown();
            }
        }, "take-waiter");
        taker.start();

        assertTrue(takeStarted.await(2, TimeUnit.SECONDS));
        // Give taker time to block on EMPTY at seq 0
        assertTrue(taker.isAlive());
        assertEquals(1, takeDone.getCount());

        buf.put(1, msg(1)); // out-of-order; must not unblock take
        Thread.sleep(50);
        assertEquals(1, takeDone.getCount());

        buf.put(0, msg(0));
        assertTrue(takeDone.await(2, TimeUnit.SECONDS));
        if (error.get() != null) {
            fail(error.get());
        }
        assertData(result.get(), 0);
        assertData(buf.take(), 1);
        taker.join(2000);
    }

    @Test
    void tombstoneAdvancesCursorWithoutPayload() throws Exception {
        ReorderBuffer buf = new ReorderBuffer(8);
        buf.putTombstone(0);
        buf.put(1, msg(1));

        assertInstanceOf(TakeResult.Tombstone.class, buf.take());
        assertData(buf.take(), 1);
    }

    @Test
    void windowBackpressureBlocksPutUntilTakeAdvances() throws Exception {
        ReorderBuffer buf = new ReorderBuffer(2);
        buf.put(0, msg(0));
        buf.put(1, msg(1));

        CountDownLatch putStarted = new CountDownLatch(1);
        CountDownLatch putDone = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        Thread producer = new Thread(() -> {
            try {
                putStarted.countDown();
                buf.put(2, msg(2));
                putDone.countDown();
            } catch (Throwable t) {
                error.set(t);
                putDone.countDown();
            }
        }, "put-backpressure");
        producer.start();

        assertTrue(putStarted.await(2, TimeUnit.SECONDS));
        Thread.sleep(50);
        assertEquals(1, putDone.getCount(), "put(2) should block while window is full");

        assertData(buf.take(), 0);
        assertTrue(putDone.await(2, TimeUnit.SECONDS));
        if (error.get() != null) {
            fail(error.get());
        }

        assertData(buf.take(), 1);
        assertData(buf.take(), 2);
        producer.join(2000);
    }

    @Test
    void duplicateSeqIsRejected() throws Exception {
        ReorderBuffer buf = new ReorderBuffer(8);
        buf.put(0, msg(0));
        assertThrows(IllegalStateException.class, () -> buf.put(0, msg(0)));
        assertThrows(IllegalStateException.class, () -> buf.putTombstone(0));

        buf.putTombstone(1);
        assertThrows(IllegalStateException.class, () -> buf.put(1, msg(1)));
        assertThrows(IllegalStateException.class, () -> buf.putTombstone(1));
    }

    @Test
    void signalEndOnEmptyBufferUnblocksTakeWithEnd() throws Exception {
        ReorderBuffer buf = new ReorderBuffer(4);
        CountDownLatch takeStarted = new CountDownLatch(1);
        AtomicReference<TakeResult> result = new AtomicReference<>();

        Thread taker = new Thread(() -> {
            try {
                takeStarted.countDown();
                result.set(buf.take());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "take-end");
        taker.start();

        assertTrue(takeStarted.await(2, TimeUnit.SECONDS));
        Thread.sleep(30);
        buf.signalEnd();
        taker.join(2000);

        assertInstanceOf(TakeResult.End.class, result.get());
        assertTrue(buf.isEnded());
        assertTrue(buf.isDrained());
    }

    @Test
    void putThenSignalEndYieldsDataThenEnd() throws Exception {
        ReorderBuffer buf = new ReorderBuffer(4);
        buf.put(0, msg(0));
        buf.signalEnd();

        assertData(buf.take(), 0);
        assertInstanceOf(TakeResult.End.class, buf.take());
        assertTrue(buf.isDrained());
    }

    @Test
    void abortUnblocksTakeOnHoleWithoutWaiting() throws Exception {
        ReorderBuffer buf = new ReorderBuffer(8);
        buf.put(1, msg(1)); // hole at seq 0
        buf.abort();

        assertTrue(buf.isAborted());
        assertInstanceOf(TakeResult.End.class, buf.take());
    }

    @Test
    void concurrencyMultipleProducersOneConsumerPreservesOrder() throws Exception {
        int window = 64;
        int total = 200;
        int producers = 4;
        ReorderBuffer buf = new ReorderBuffer(window);

        ExecutorService pool = Executors.newFixedThreadPool(producers + 1);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> putFutures = new ArrayList<>();

            for (int p = 0; p < producers; p++) {
                final int producerId = p;
                putFutures.add(pool.submit(() -> {
                    start.await();
                    for (long seq = producerId; seq < total; seq += producers) {
                        if (seq % 17 == 0) {
                            buf.putTombstone(seq);
                        } else {
                            buf.put(seq, msg(seq));
                        }
                    }
                    return null;
                }));
            }

            Future<List<TakeResult>> takeFuture = pool.submit(() -> {
                start.await();
                List<TakeResult> got = new ArrayList<>(total);
                for (int i = 0; i < total; i++) {
                    TakeResult r = buf.take();
                    if (r instanceof TakeResult.End) {
                        fail("unexpected End before all " + total + " seqs, at i=" + i);
                    }
                    got.add(r);
                }
                return got;
            });

            start.countDown();

            for (Future<?> f : putFutures) {
                f.get(8, TimeUnit.SECONDS);
            }
            List<TakeResult> results = takeFuture.get(8, TimeUnit.SECONDS);

            assertEquals(total, results.size());
            long expectedSeq = 0;
            for (TakeResult r : results) {
                if (expectedSeq % 17 == 0) {
                    assertInstanceOf(TakeResult.Tombstone.class, r, "seq=" + expectedSeq);
                } else {
                    assertData(r, expectedSeq);
                }
                expectedSeq++;
            }

            buf.signalEnd();
            assertInstanceOf(TakeResult.End.class, buf.take());
            assertTrue(buf.isDrained());
        } finally {
            pool.shutdownNow();
        }
    }

    private static Message msg(long seq) {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("id", seq);
        payload.put("text", "m-" + seq);
        return new Message(seq, payload);
    }

    private static void assertData(TakeResult result, long seq) {
        TakeResult.Data data = assertInstanceOf(TakeResult.Data.class, result);
        assertEquals(seq, data.message().seq());
    }
}
