package net.intelie.disq.dson;

public class StringCache {
    public static final String EMPTY = "";
    private final int bucketCount;
    private final int bucketSize;
    private final int maxStringLength;
    private final String[] cache;

    public StringCache() {
        this(4096, 4, 1024);
    }

    public StringCache(int bucketCount, int bucketSize, int maxStringLength) {
        this.bucketCount = bucketCount;
        this.bucketSize = bucketSize;
        this.maxStringLength = maxStringLength;
        this.cache = new String[bucketCount * bucketSize];
    }

    public String get(CharSequence cs) {
        if (cs == null) return null;
        int length = cs.length();
        if (length == 0) return EMPTY;
        if (length > maxStringLength) return cs.toString();

        int hash = hash(cs, length);
        int n = Math.abs(hash % bucketCount) * bucketSize;
        String cached = cache[n];
        if (eq(cached, cs, hash))
            return cached;
        for (int k = 1; k < bucketSize; k++)
            if (eq(cached = cache[n + k], cs, hash))
                return finish(cached, n, k);
        return finish(cs.toString(), n, bucketSize - 1);
    }

    private int hash(CharSequence cs, int length) {
        int hash = 0;
        for (int i = 0; i < length; i++)
            hash = 31 * hash + cs.charAt(i);
        return hash;
    }


    private String finish(String cached, int n, int k) {
        while (k > 0)
            cache[n + k] = cache[n + --k];
        return cache[n] = cached;
    }

    private static boolean eq(String cached, CharSequence cs, int hash) {
        if (cached == null || cached.hashCode() != hash)
            return false;
        return cached.contentEquals(cs);
    }
}