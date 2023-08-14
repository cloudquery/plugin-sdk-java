package io.cloudquery.transformers;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.cloudquery.caser.Caser;

import java.lang.reflect.Field;

public interface NameTransformer {
    class DefaultNameTransformer implements NameTransformer {
        private final Caser caser = Caser.builder().build();

        /**
         * Transforms the field name to the name of the property in the JSON
         * or use the {@link Caser#toSnake(String)} if no annotation.
         *
         * @param field Field to transform
         * @return Transformed field name
         */
        @Override
        public String transform(Field field) {
            JsonProperty annotation = field.getAnnotation(JsonProperty.class);
            if (annotation != null) {
                return annotation.value();
            }
            return caser.toSnake(field.getName());
        }
    }

    String transform(Field field) throws TransformerException;
}
