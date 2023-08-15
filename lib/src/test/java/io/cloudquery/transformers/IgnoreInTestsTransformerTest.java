package io.cloudquery.transformers;

import io.cloudquery.transformers.IgnoreInTestsTransformer.DefaultIgnoreInTestsTransformer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IgnoreInTestsTransformerTest {
    @Test
    public void shouldRetrunFalse() {
        assertFalse(new DefaultIgnoreInTestsTransformer().transform(null));
    }
}
