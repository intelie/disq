package net.intelie.disq.dson;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StringCacheTest {
    @Test
    public void testCacheHit() {
        StringCache cache = new StringCache();
        StringBuilder original = new StringBuilder("abcde");
        String cached1 = cache.get(original);
        String cached2 = cache.get(original);

        assertThat(original.toString()).isEqualTo(cached1).isNotSameAs(cached1);
        assertThat(original.toString()).isEqualTo(cached2).isNotSameAs(cached2);

        assertThat(cached1).isSameAs(cached2);
    }

    @Test
    public void testCacheSecondHit() {
        StringCache cache = new StringCache();
        StringBuilder original1 = new StringBuilder("FB");
        StringBuilder original2 = new StringBuilder("Ea");

        String cached1 = cache.get(original1);
        String cached2 = cache.get(original2);
        String cached1b = cache.get(original1);

        assertThat(original1.toString()).isEqualTo(cached1).isNotSameAs(cached1);
        assertThat(original2.toString()).isEqualTo(cached2).isNotSameAs(cached2);

        assertThat(cached1).isNotEqualTo(cached2);
        assertThat(cached1.hashCode()).isEqualTo(cached2.hashCode());

        assertThat(cached1).isSameAs(cached1b);
    }
}