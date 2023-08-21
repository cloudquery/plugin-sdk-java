package io.cloudquery.transformers;

import static org.junit.jupiter.api.Assertions.*;

import io.cloudquery.transformers.IgnoreInTestsTransformer.DefaultIgnoreInTestsTransformer;
import org.junit.jupiter.api.Test;

class IgnoreInTestsTransformerTest {
  @Test
  public void shouldRetrunFalse() {
    assertFalse(new DefaultIgnoreInTestsTransformer().transform(null));
  }
}
