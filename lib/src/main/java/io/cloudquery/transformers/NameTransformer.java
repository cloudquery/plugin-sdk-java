package io.cloudquery.transformers;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.cloudquery.caser.Caser;

import javax.xml.transform.TransformerException;
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
         * @throws TransformerException If the field name cannot be transformed
         */
        @Override
        public String transform(Field field) throws TransformerException {
            JsonProperty annotation = field.getAnnotation(JsonProperty.class);
            if (annotation != null) {
                return annotation.value();
            }
            return caser.toSnake(field.getName());
        }
    }

    String transform(Field field) throws TransformerException;
}
