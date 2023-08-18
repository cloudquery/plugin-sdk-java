package io.cloudquery.schema;

import io.cloudquery.types.UUIDType;
import lombok.Builder;
import lombok.Getter;
import org.apache.arrow.vector.types.pojo.ArrowType;

@Builder(toBuilder = true)
@Getter
public class Column {
    private String name;
    private String description;
    private ArrowType type;
    private ColumnResolver resolver;
    private boolean primaryKey;
    private boolean notNull;
    private boolean unique;
    private boolean incrementalKey;
    private boolean ignoreInTests;

    public static final Column CQ_ID_COLUMN = Column.builder().name("_cq_id").type(new UUIDType())
            .description("Internal CQ ID of the row").notNull(true).unique(true).build();
    public static final Column CQ_PARENT_ID_COLUMN = Column.builder().name("_cq_parent_id").type(new UUIDType())
            .description("Internal CQ ID of the parent row").ignoreInTests(true).build();

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append(":");
        sb.append(type.toString());
        if (primaryKey) {
            sb.append(":PK");
        }
        if (notNull) {
            sb.append(":NotNull");
        }
        if (unique) {
            sb.append(":Unique");
        }
        if (incrementalKey) {
            sb.append(":IncrementalKey");
        }
        return sb.toString();
    }
}
