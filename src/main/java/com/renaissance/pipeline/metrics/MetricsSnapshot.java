package com.renaissance.pipeline.metrics;

/**
 * Неизменяемая сводка сэмплов глубин, скоростей и латентности пула.
 */
public final class MetricsSnapshot {

    private final StageStats in;
    private final StageStats threadPool;
    private final StageStats out;
    private final StageStats inRate;
    private final StageStats outRate;
    private final double inRateMsgPerSec;
    private final double outRateMsgPerSec;
    private final long durationMs;
    private final StageStats poolLatencyMs;
    private final long arrived;
    private final long departed;
    private final int sampleCount;

    /** Собрать снимок из готовых StageStats и счётчиков. */
    public MetricsSnapshot(
            StageStats in,
            StageStats threadPool,
            StageStats out,
            StageStats inRate,
            StageStats outRate,
            double inRateMsgPerSec,
            double outRateMsgPerSec,
            long durationMs,
            StageStats poolLatencyMs,
            long arrived,
            long departed,
            int sampleCount) {
        this.in = in;
        this.threadPool = threadPool;
        this.out = out;
        this.inRate = inRate;
        this.outRate = outRate;
        this.inRateMsgPerSec = inRateMsgPerSec;
        this.outRateMsgPerSec = outRateMsgPerSec;
        this.durationMs = durationMs;
        this.poolLatencyMs = poolLatencyMs;
        this.arrived = arrived;
        this.departed = departed;
        this.sampleCount = sampleCount;
    }

    /** Статистика глубины In (очередь), msg. */
    public StageStats in() {
        return in;
    }

    /** Статистика in-flight в пуле, msg. */
    public StageStats threadPool() {
        return threadPool;
    }

    /** Статистика глубины Out/reorder, msg. */
    public StageStats out() {
        return out;
    }

    /** Сэмплы мгновенной скорости In, msg/s. */
    public StageStats inRate() {
        return inRate;
    }

    /** Сэмплы мгновенной скорости Out, msg/s. */
    public StageStats outRate() {
        return outRate;
    }

    /** Средняя скорость arrived за весь прогон, msg/s. */
    public double inRateMsgPerSec() {
        return inRateMsgPerSec;
    }

    /** Средняя скорость departed за весь прогон, msg/s. */
    public double outRateMsgPerSec() {
        return outRateMsgPerSec;
    }

    /** Длительность окна метрик, мс. */
    public long durationMs() {
        return durationMs;
    }

    /** Латентность обработки в пуле, мс. */
    public StageStats poolLatencyMs() {
        return poolLatencyMs;
    }

    /** Число arrived. */
    public long arrived() {
        return arrived;
    }

    /** Число departed. */
    public long departed() {
        return departed;
    }

    /** Число сэмплов глубины In. */
    public int sampleCount() {
        return sampleCount;
    }

    /** Многострочная текстовая таблица для лога при shutdown (с единицами). */
    public String formatTable() {
        StringBuilder sb = new StringBuilder();
        sb.append("--- metrics ---").append(System.lineSeparator());
        sb.append(String.format("%-12s %-6s %8s %8s %8s", "stage", "unit", "avg", "max", "p99"))
                .append(System.lineSeparator());
        appendStage(sb, "In", "msg", in);
        appendStage(sb, "ThreadPool", "msg", threadPool);
        appendStage(sb, "Out", "msg", out);
        appendStage(sb, "In rate", "msg/s", inRate);
        appendStage(sb, "Out rate", "msg/s", outRate);
        appendStage(sb, "pool latency", "ms", poolLatencyMs);
        sb.append(String.format(
                "overall: in=%.1f msg/s  out=%.1f msg/s  duration=%d ms  samples=%d",
                inRateMsgPerSec, outRateMsgPerSec, durationMs, sampleCount));
        return sb.toString();
    }

    private static void appendStage(StringBuilder sb, String name, String unit, StageStats s) {
        sb.append(String.format("%-12s %-6s %8.1f %8.0f %8.0f",
                        name, unit, s.avg(), s.max(), s.p99()))
                .append(System.lineSeparator());
    }

    /** Avg / max / p99 для одной серии. */
    public static final class StageStats {
        private final double avg;
        private final double max;
        private final double p99;

        public StageStats(double avg, double max, double p99) {
            this.avg = avg;
            this.max = max;
            this.p99 = p99;
        }

        /** Посчитать StageStats по списку сэмплов. */
        public static StageStats of(java.util.List<? extends Number> samples) {
            return new StageStats(
                    Percentiles.avg(samples),
                    Percentiles.max(samples),
                    Percentiles.percentile(samples, 99));
        }

        /** Пустая статистика (нули). */
        public static StageStats empty() {
            return new StageStats(0, 0, 0);
        }

        /** Среднее. */
        public double avg() {
            return avg;
        }

        /** Максимум. */
        public double max() {
            return max;
        }

        /** 99-й процентиль. */
        public double p99() {
            return p99;
        }
    }
}
