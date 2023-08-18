package io.cloudquery.schema;

import io.cloudquery.scalar.ValidationException;
import io.cloudquery.transformers.TransformerException;
import io.cloudquery.types.UUIDType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParentCQUUIDResolverTest {
    private static final java.util.UUID PARENT_UUID = java.util.UUID.randomUUID();
    private static final java.util.UUID CHILD_UID = java.util.UUID.randomUUID();
    private static final String CHILD_COLUMN = "child_column";
    private static final Column COLUMN = Column.builder().name(CHILD_COLUMN).type(new UUIDType()).build();
    private static final Table parentTable = Table.builder().name("parent").columns(List.of(Column.CQ_ID_COLUMN)).build();
    private static final Table childTable = Table.builder().name("child").columns(List.of(COLUMN)).build();

    private ColumnResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new ParentCQUUIDResolver();
    }

    @Test
    public void shouldSetColumnToNullIfWeDoNotHaveAParent() throws TransformerException, ValidationException {
        Resource resourceWithNoParent = Resource.builder().table(childTable).build();
        resourceWithNoParent.set(CHILD_COLUMN, CHILD_UID);

        assertEquals(CHILD_UID, resourceWithNoParent.get(CHILD_COLUMN).get());
        resolver.resolve(null, resourceWithNoParent, COLUMN);

        assertEquals(null, resourceWithNoParent.get(CHILD_COLUMN).get());
    }

    @Test
    public void shouldSetColumnToNullIfParentDoesNotHaveCQID() throws TransformerException, ValidationException {
        Resource resource = Resource.builder().
                table(childTable).
                parent(Resource.builder().table(parentTable).build()).
                build();
        resource.set(CHILD_COLUMN, CHILD_UID);

        assertEquals(CHILD_UID, resource.get(CHILD_COLUMN).get());
        resolver.resolve(null, resource, COLUMN);

        assertEquals(null, resource.get(CHILD_COLUMN).get());
    }

    @Test
    public void shouldSetColumnToUUIDIfParentHasACQID() throws TransformerException, ValidationException {
        Resource parentResource = Resource.builder().table(parentTable).build();
        parentResource.set(Column.CQ_ID_COLUMN.getName(), PARENT_UUID);

        Resource resource = Resource.builder().table(childTable).parent(parentResource).build();
        resource.set(CHILD_COLUMN, CHILD_UID);

        assertEquals(CHILD_UID, resource.get(CHILD_COLUMN).get());
        resolver.resolve(null, resource, COLUMN);

        assertEquals(PARENT_UUID, resource.get(CHILD_COLUMN).get());
    }
}
