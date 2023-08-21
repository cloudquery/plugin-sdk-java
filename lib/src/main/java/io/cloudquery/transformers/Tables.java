package io.cloudquery.transformers;

import static io.cloudquery.schema.Table.*;

import io.cloudquery.schema.Table;
import java.util.List;

class Tables {
  public static void setParents(List<Table> tables, Table parent) {
    for (Table table : tables) {
      table.setParent(parent);
      setParents(table.getRelations(), table);
    }
  }

  public static void transformTables(List<Table> tables) throws TransformerException {
    for (Table table : tables) {
      table.transform();
      transformTables(table.getRelations());
    }
  }

  public static void apply(List<Table> tables, List<Transform> extraTransformers)
      throws TransformerException {
    for (Table table : tables) {
      for (Transform extraTransformer : extraTransformers) {
        extraTransformer.transformTable(table);
      }
      apply(table.getRelations(), extraTransformers);
    }
  }
}
