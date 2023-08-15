package io.cloudquery.transformers;

import java.lang.reflect.Field;

public interface IgnoreInTestsTransformer {
    class DefaultIgnoreInTestsTransformer implements IgnoreInTestsTransformer {
        @Override
        public boolean transform(Field field) {
            return false;
        }
    }

    boolean transform(Field field);
}
