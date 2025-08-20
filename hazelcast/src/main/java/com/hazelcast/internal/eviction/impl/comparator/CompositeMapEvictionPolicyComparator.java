package com.hazelcast.internal.eviction.impl.comparator;

import com.hazelcast.map.MapEvictionPolicyComparator;
import com.hazelcast.spi.eviction.EvictableEntryView;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

import static java.lang.Math.abs;
import static java.lang.Math.min;

/**
 * Comparator combining recency, frequency, cost and expiration into a single score.
 * Higher scores indicate that an entry is a better eviction candidate.
 */
@SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:methodcount"})
public class CompositeMapEvictionPolicyComparator
        implements MapEvictionPolicyComparator<Object, Object>, Serializable {

    private static final long serialVersionUID = 1L;

    /** 10 minutes window for recency calculations. */
    public static final long RECENCY_WINDOW_MS = 600_000L;
    /** Cap for cost normalization (1 MiB). */
    public static final long COST_CAP_BYTES = 1_048_576L;
    /** 1 hour window for expiration normalization. */
    public static final long EXP_WINDOW_MS = 3_600_000L;

    private static final double EPS = 1e-9;

    private final double timeWeight;
    private final double freqWeight;
    private final double costWeight;
    private final double expWeight;

    /**
     * Creates a comparator with equal weights.
     */
    public CompositeMapEvictionPolicyComparator() {
        this(0.25d, 0.25d, 0.25d, 0.25d);
    }

    /**
     * Creates a comparator with the supplied weights.
     */
    public CompositeMapEvictionPolicyComparator(double timeWeight, double freqWeight,
                                                double costWeight, double expWeight) {
        validateWeights(timeWeight, freqWeight, costWeight, expWeight);
        this.timeWeight = timeWeight;
        this.freqWeight = freqWeight;
        this.costWeight = costWeight;
        this.expWeight = expWeight;
    }

    private static void validateWeights(double timeWeight, double freqWeight,
                                        double costWeight, double expWeight) {
        if (!isValidWeight(timeWeight)) {
            throw new IllegalArgumentException("timeWeight must be in [0,1]");
        }
        if (!isValidWeight(freqWeight)) {
            throw new IllegalArgumentException("freqWeight must be in [0,1]");
        }
        if (!isValidWeight(costWeight)) {
            throw new IllegalArgumentException("costWeight must be in [0,1]");
        }
        if (!isValidWeight(expWeight)) {
            throw new IllegalArgumentException("expWeight must be in [0,1]");
        }
        double sum = timeWeight + freqWeight + costWeight + expWeight;
        if (abs(sum - 1.0d) > EPS) {
            throw new IllegalArgumentException("weights must sum to 1.0 but was " + sum);
        }
    }

    private static boolean isValidWeight(double weight) {
        return weight >= 0d && weight <= 1d && !Double.isNaN(weight);
    }

    @Override
    public int compare(EvictableEntryView e1, EvictableEntryView e2) {
        // degrade to built-in policies for backwards compatibility
        if (timeWeight == 1d && freqWeight == 0d && costWeight == 0d && expWeight == 0d) {
            return com.hazelcast.internal.eviction.impl.comparator.LRUEvictionPolicyComparator.INSTANCE.compare(e1, e2);
        }
        if (timeWeight == 0d && freqWeight == 1d && costWeight == 0d && expWeight == 0d) {
            return com.hazelcast.internal.eviction.impl.comparator.LFUEvictionPolicyComparator.INSTANCE.compare(e1, e2);
        }

        long now = System.currentTimeMillis();

        double s1 = compositeScore(e1, now);
        double s2 = compositeScore(e2, now);

        int result = Double.compare(s2, s1);
        if (result != 0) {
            return result;
        }

        long t1 = e1.getLastAccessTime();
        long t2 = e2.getLastAccessTime();
        if (t1 != -1 && t2 != -1) {
            int cmp = Long.compare(t1, t2);
            if (cmp != 0) {
                return cmp;
            }
        }

        long h1 = e1.getHits();
        long h2 = e2.getHits();
        if (h1 != -1 && h2 != -1) {
            int cmp = Long.compare(h1, h2);
            if (cmp != 0) {
                return cmp;
            }
        }

        long c1 = e1.getCost();
        long c2 = e2.getCost();
        if (c1 != -1 && c2 != -1) {
            int cmp = Long.compare(c2, c1);
            if (cmp != 0) {
                return cmp;
            }
        }

        long ex1 = e1.getExpirationTime();
        long ex2 = e2.getExpirationTime();
        if (ex1 > 0 && ex2 > 0) {
            int cmp = Long.compare(ex1, ex2);
            if (cmp != 0) {
                return cmp;
            }
        }

        long kh1 = keyHash(e1.getKey());
        long kh2 = keyHash(e2.getKey());
        return Long.compare(kh1, kh2);
    }

    private double compositeScore(EvictableEntryView e, long now) {
        double recency = recencyScore(e.getLastAccessTime(), now);
        double freq = frequencyScore(e.getHits());
        double cost = costScore(e.getCost());
        double exp = expirationScore(e.getExpirationTime(), now);

        double score = timeWeight * recency
                + freqWeight * freq
                + costWeight * cost
                + expWeight * exp;

        return Double.isFinite(score) ? score : 0.5d;
    }

    private static double recencyScore(long lastAccessTime, long now) {
        if (lastAccessTime == -1) {
            return 0.5d;
        }
        long diff = now - lastAccessTime;
        if (diff < 0) {
            return 0d;
        }
        return clamp01((double) diff / RECENCY_WINDOW_MS);
    }

    private static double frequencyScore(long hits) {
        if (hits == -1) {
            return 0.5d;
        }
        return checkFinite(1d / (1d + hits));
    }

    private static double costScore(long cost) {
        if (cost == -1) {
            return 0.5d;
        }
        long c = min(cost, COST_CAP_BYTES);
        return checkFinite(1d - (1d / (1d + c)));
    }

    private static double expirationScore(long expTime, long now) {
        if (expTime == -1 || expTime == 0) {
            return 0.5d;
        }
        if (expTime <= now) {
            return 1d;
        }
        return checkFinite(1d - clamp01((double) (expTime - now) / EXP_WINDOW_MS));
    }

    private static double clamp01(double v) {
        if (v < 0d) {
            return 0d;
        }
        if (v > 1d) {
            return 1d;
        }
        return v;
    }

    private static double checkFinite(double v) {
        return Double.isFinite(v) ? v : 0.5d;
    }

    private static long keyHash(Object key) {
        if (key == null) {
            return 0L;
        }
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
                out.writeObject(key);
            }
            byte[] bytes = bos.toByteArray();
            long h = Arrays.hashCode(bytes);
            h ^= (h << 13);
            h ^= (h >>> 7);
            h ^= (h << 17);
            return h;
        } catch (IOException ignored) {
            return key.hashCode() * 31L;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CompositeMapEvictionPolicyComparator)) {
            return false;
        }
        CompositeMapEvictionPolicyComparator that = (CompositeMapEvictionPolicyComparator) o;
        return abs(timeWeight - that.timeWeight) < EPS
                && abs(freqWeight - that.freqWeight) < EPS
                && abs(costWeight - that.costWeight) < EPS
                && abs(expWeight - that.expWeight) < EPS;
    }

    @Override
    public int hashCode() {
        int result = Double.hashCode(timeWeight);
        result = 31 * result + Double.hashCode(freqWeight);
        result = 31 * result + Double.hashCode(costWeight);
        result = 31 * result + Double.hashCode(expWeight);
        result = 31 * result + Long.hashCode(RECENCY_WINDOW_MS);
        result = 31 * result + Long.hashCode(COST_CAP_BYTES);
        result = 31 * result + Long.hashCode(EXP_WINDOW_MS);
        return result;
    }

    @Override
    public String toString() {
        return "CompositeMapEvictionPolicyComparator{" +
                "timeWeight=" + timeWeight +
                ", freqWeight=" + freqWeight +
                ", costWeight=" + costWeight +
                ", expWeight=" + expWeight +
                ", RECENCY_WINDOW_MS=" + RECENCY_WINDOW_MS +
                ", COST_CAP_BYTES=" + COST_CAP_BYTES +
                ", EXP_WINDOW_MS=" + EXP_WINDOW_MS +
                '}';
    }
}
