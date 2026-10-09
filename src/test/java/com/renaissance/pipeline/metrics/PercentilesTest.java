package com.renaissance.pipeline.metrics;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PercentilesTest {

    @Test
    void emptyReturnsZero() {
        assertEquals(0.0, Percentiles.percentile(List.of(), 99));
        assertEquals(0.0, Percentiles.avg(List.of()));
        assertEquals(0.0, Percentiles.max(List.of()));
    }

    @Test
    void singleSample() {
        assertEquals(5.0, Percentiles.percentile(List.of(5), 99));
        assertEquals(5.0, Percentiles.avg(List.of(5)));
        assertEquals(5.0, Percentiles.max(List.of(5)));
    }

    @Test
    void p99NearestRankOnHundred() {
        // 1..100 → ceil(0.99*100)-1 = 98 → value 99
        java.util.List<Integer> samples = new java.util.ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            samples.add(i);
        }
        assertEquals(99.0, Percentiles.percentile(samples, 99));
        assertEquals(100.0, Percentiles.max(samples));
        assertEquals(50.5, Percentiles.avg(samples), 1e-9);
    }

    @Test
    void doesNotMutateInput() {
        java.util.List<Integer> samples = new java.util.ArrayList<>(List.of(3, 1, 2));
        Percentiles.percentile(samples, 50);
        assertEquals(List.of(3, 1, 2), samples);
    }

    @Test
    void invalidPercentileThrows() {
        assertThrows(IllegalArgumentException.class, () -> Percentiles.percentile(List.of(1), -1));
        assertThrows(IllegalArgumentException.class, () -> Percentiles.percentile(List.of(1), 101));
    }
}
