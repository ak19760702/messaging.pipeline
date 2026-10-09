package com.renaissance.pipeline.buffer;

import com.renaissance.pipeline.model.Message;

import java.util.Objects;

/**
 * Потокобезопасное кольцевое окно reorder: workers — {@link #put}/{@link #putTombstone},
 * один потребитель (Writer) — {@link #take} по порядку seq.
 * Индекс слота = {@code seq % window}; put вне окна блокируется (backpressure).
 * Дубликат seq → {@link IllegalStateException}.
 */
public final class ReorderBuffer {

    public enum Kind {
        EMPTY,
        DATA,
        TOMBSTONE
    }

    /** Результат блокирующего {@link #take()}. */
    public sealed interface TakeResult {
        record Data(Message message) implements TakeResult {
            public Data {
                Objects.requireNonNull(message, "message");
            }
        }

        record Tombstone() implements TakeResult {
        }

        /** Конец потока: {@link #signalEnd()} и больше нечего отдавать. */
        record End() implements TakeResult {
        }
    }

    private static final class Slot {
        Kind kind = Kind.EMPTY;
        Message payload;
        /** Seq, занимающий физический слот (значим при kind != EMPTY). */
        long seq = -1L;

        /** Очистить слот после take. */
        void clear() {
            kind = Kind.EMPTY;
            payload = null;
            seq = -1L;
        }
    }

    private final int window;
    private final Slot[] slots;

    /** Следующий seq, который Writer может забрать. */
    private long nextToWrite;
    /** Мягкий конец: новых put не будет. */
    private boolean ended;
    /** Fatal abort: разбудить waiters и не ждать дыры. */
    private boolean aborted;

    /** Создать buffer с размером окна {@code window}. */
    public ReorderBuffer(int window) {
        if (window <= 0) {
            throw new IllegalArgumentException("window must be > 0, got " + window);
        }
        this.window = window;
        this.slots = new Slot[window];
        for (int i = 0; i < window; i++) {
            slots[i] = new Slot();
        }
    }

    /** Размер окна reorder. */
    public int window() {
        return window;
    }

    /** Курсор Writer (nextExpected). */
    public synchronized long nextToWrite() {
        return nextToWrite;
    }

    /** Был ли вызван {@link #signalEnd()} / конец через abort. */
    public synchronized boolean isEnded() {
        return ended;
    }

    /** Был ли вызван {@link #abort()}. */
    public synchronized boolean isAborted() {
        return aborted;
    }

    /** Число занятых слотов (DATA/TOMBSTONE) — backlog перед Writer. */
    public synchronized int occupied() {
        int count = 0;
        for (Slot slot : slots) {
            if (slot.kind != Kind.EMPTY) {
                count++;
            }
        }
        return count;
    }

    /** End просигнален и до курсора не осталось pending DATA/TOMBSTONE. */
    public synchronized boolean isDrained() {
        return ended && !hasPendingAtOrAfterCursor();
    }

    /**
     * Положить обработанное сообщение в слот {@code seq}; блокируется вне окна.
     * Дубликат seq → {@link IllegalStateException}.
     */
    public synchronized void put(long seq, Message data) throws InterruptedException {
        Objects.requireNonNull(data, "data");
        if (data.seq() != seq) {
            throw new IllegalArgumentException(
                    "message.seq=" + data.seq() + " does not match put seq=" + seq);
        }
        awaitInWindow(seq);
        Slot slot = slots[index(seq)];
        if (slot.kind != Kind.EMPTY) {
            throw duplicate(seq, slot);
        }
        slot.kind = Kind.DATA;
        slot.payload = data;
        slot.seq = seq;
        notifyAll();
    }

    /**
     * Пометить {@code seq} как tombstone (пропуск); блокируется вне окна.
     * Дубликат seq → {@link IllegalStateException}.
     */
    public synchronized void putTombstone(long seq) throws InterruptedException {
        awaitInWindow(seq);
        Slot slot = slots[index(seq)];
        if (slot.kind != Kind.EMPTY) {
            throw duplicate(seq, slot);
        }
        slot.kind = Kind.TOMBSTONE;
        slot.payload = null;
        slot.seq = seq;
        notifyAll();
    }

    /**
     * Ждать DATA/TOMBSTONE на {@link #nextToWrite}, либо End при end/abort без остатка.
     */
    public synchronized TakeResult take() throws InterruptedException {
        while (true) {
            Slot slot = slots[index(nextToWrite)];
            if (slot.kind == Kind.DATA && slot.seq == nextToWrite) {
                Message msg = slot.payload;
                slot.clear();
                nextToWrite++;
                notifyAll();
                return new TakeResult.Data(msg);
            }
            if (slot.kind == Kind.TOMBSTONE && slot.seq == nextToWrite) {
                slot.clear();
                nextToWrite++;
                notifyAll();
                return new TakeResult.Tombstone();
            }
            // EMPTY at cursor
            if (aborted) {
                // Fatal stop: do not hang on a hole; App integrity check catches shortfall.
                return new TakeResult.End();
            }
            if (ended && !hasPendingAtOrAfterCursor()) {
                return new TakeResult.End();
            }
            // ended with pending later seqs but hole at cursor: wait briefly only if not
            // aborted — under normal Pipeline workers already finished, so this would hang.
            // Treat as integrity failure: End, caller sees assigned != written+tombstones.
            if (ended) {
                return new TakeResult.End();
            }
            wait();
        }
    }

    /** Больше put не будет; разблокирует waiting {@link #take()} с End, когда курсор пуст. */
    public synchronized void signalEnd() {
        ended = true;
        notifyAll();
    }

    /** Fatal abort: разбудить put/take; дальнейшие put падают; take сразу End. */
    public synchronized void abort() {
        aborted = true;
        ended = true;
        notifyAll();
    }

    /** Ждать, пока {@code seq} попадёт в текущее окно [nextToWrite, nextToWrite+window). */
    private void awaitInWindow(long seq) throws InterruptedException {
        while (true) {
            if (ended) {
                throw new IllegalStateException(
                        "ReorderBuffer already ended; cannot put seq=" + seq);
            }
            long low = nextToWrite;
            long high = nextToWrite + window;
            if (seq >= low && seq < high) {
                return;
            }
            if (seq < low) {
                throw new IllegalArgumentException(
                        "seq=" + seq + " is behind nextToWrite=" + low);
            }
            // seq >= high → wait for Writer to advance the window
            wait();
        }
    }

    /** Есть ли занятые слоты с seq ≥ курсора. */
    private boolean hasPendingAtOrAfterCursor() {
        for (Slot slot : slots) {
            if (slot.kind != Kind.EMPTY && slot.seq >= nextToWrite) {
                return true;
            }
        }
        return false;
    }

    /** Индекс кольцевого слота для seq. */
    private int index(long seq) {
        return (int) (seq % window);
    }

    /** Исключение о дубликате seq. */
    private static IllegalStateException duplicate(long seq, Slot slot) {
        return new IllegalStateException(
                "duplicate seq=" + seq + " (slot already " + slot.kind + ")");
    }
}
