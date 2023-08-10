package io.cloudquery.transformers;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.cloudquery.transformers.NameTransformer.DefaultNameTransformer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.xml.transform.TransformerException;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DefaultNameTransformerTest {

    private DefaultNameTransformer transformer;

    @SuppressWarnings("unused")
    private static class SimpleClass {
        // Simple fields with no custom property mapping
        private String simpleField;
        private String aLongerFieldName;

        // Fields with custom property mapping
        @JsonProperty("id")
        private String userID;
    }

    @BeforeEach
    void setUp() {
        transformer = new DefaultNameTransformer();
    }

    @Test
    public void shouldReturnSnakeCaseFieldNamesByDefault() throws TransformerException {
        Field[] declaredFields = SimpleClass.class.getDeclaredFields();

        // Simple fields with no custom property mapping
        assertEquals("simple_field", transformer.transform(declaredFields[0]));
        assertEquals("a_longer_field_name", transformer.transform(declaredFields[1]));

        // Fields with custom property mapping
        assertEquals("id", transformer.transform(declaredFields[2]));
    }
}
