package io.cloudquery.schema;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class Resource {
    private Object item;


    public void set(String columnName, Object value) {
    }
}
