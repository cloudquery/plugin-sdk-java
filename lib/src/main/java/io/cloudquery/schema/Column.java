package io.cloudquery.schema;

import lombok.Builder;
import lombok.Getter;
import org.apache.arrow.vector.types.pojo.ArrowType;

@Builder
@Getter
public class Column {
    private String name;
    private ArrowType type;
    private ColumnResolver resolver;
    private boolean primaryKey;
    private boolean ignoreInTests;
}
