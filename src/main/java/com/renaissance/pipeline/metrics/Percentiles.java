package com.renaissance.pipeline.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Вспомогательные процентили по числовым сэмплам (копия + sort; nearest-rank).
 */
public final class Percentiles {

    private Percentiles() {
    }

    /**
     * Nearest-rank процентиль в {@code [0, 100]}.
     * Пустой список → {@code 0}. Индекс = {@code ceil(p/100 * n) - 1}.
     */
    public static double percentile(List<? extends Number> samples, double p) {
        if (samples == null || samples.isEmpty()) {
            return 0.0;
        }
        if (p < 0 || p > 100) {
            throw new IllegalArgumentException("percentile must be in [0, 100], got " + p);
        }
        List<Double> sorted = new ArrayList<>(samples.size());
        for (Number n : samples) {
            sorted.add(n.doubleValue());
        }
        Collections.sort(sorted);
        int n = sorted.size();
        if (p == 0) {
            return sorted.get(0);
        }
        int rank = (int) Math.ceil(p / 100.0 * n);
        int index = Math.min(n - 1, Math.max(0, rank - 1));
        return sorted.get(index);
    }

    /** Среднее арифметическое; пустой список → 0. */
    public static double avg(List<? extends Number> samples) {
        if (samples == null || samples.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (Number n : samples) {
            sum += n.doubleValue();
        }
        return sum / samples.size();
    }

    /** Максимум; пустой список → 0. */
    public static double max(List<? extends Number> samples) {
        if (samples == null || samples.isEmpty()) {
            return 0.0;
        }
        double m = Double.NEGATIVE_INFINITY;
        for (Number n : samples) {
            m = Math.max(m, n.doubleValue());
        }
        return m;
    }
}
