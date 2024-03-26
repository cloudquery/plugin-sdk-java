package io.cloudquery.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.Table;
import java.util.ArrayList;
import java.util.List;

public class TablesJson {
  public class ColumnJson {
    public String name;
    public String description;
    public String type;
    public boolean incremental_key;
    public boolean not_null;
    public boolean primary_key;
    public boolean unique;
  }

  public class TableJson {
    public String name;
    public String description;
    public boolean is_incremental;
    public String parent;
    public List<String> relations;
    public List<ColumnJson> columns;
  }

  private List<Table> tables;

  public TablesJson(List<Table> tables) {
    this.tables = tables;
  }

  public String toJson() throws JsonProcessingException {
    List<TableJson> json = new ArrayList<>();
    for (Table table : tables) {
      TableJson tableJson = new TableJson();
      tableJson.name = table.getName();
      tableJson.description = table.getDescription();
      tableJson.is_incremental = false;
      tableJson.parent = table.getParent() == null ? null : table.getParent().getName();
      tableJson.relations = new ArrayList<>();
      for (Table relation : table.getRelations()) {
        tableJson.relations.add(relation.getName());
      }
      tableJson.columns = new ArrayList<>();
      for (Column column : table.getColumns()) {
        ColumnJson columnJson = new ColumnJson();
        columnJson.name = column.getName();
        columnJson.description = column.getDescription();
        columnJson.type = column.getType().toString();
        columnJson.incremental_key = column.isIncrementalKey();
        columnJson.not_null = column.isNotNull();
        columnJson.primary_key = column.isPrimaryKey();
        columnJson.unique = column.isUnique();
        tableJson.columns.add(columnJson);
      }
      json.add(tableJson);
    }

    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    return objectMapper.writeValueAsString(json);
  }
}
