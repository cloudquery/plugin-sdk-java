package io.cloudquery.types;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ListTypeTest {
    @Test
    public void testEquality() {
        ListType listType1 = ListType.listOf(new ArrowType.Int(64, true));
        ListType listType2 = ListType.listOf(new ArrowType.Int(64, true));
        ListType listType3 = ListType.listOf(new ArrowType.Int(32, true));

        assertEquals(listType1, listType2);
        assertNotEquals(listType1, listType3);
    }
}
