package io.cloudquery.transformers;

import java.lang.reflect.Field;

public interface IgnoreInTestsTransformer {

  boolean transform(Field field);

  class DefaultIgnoreInTestsTransformer implements IgnoreInTestsTransformer {
    @Override
    public boolean transform(Field field) {
      return false;
    }
  }
}
