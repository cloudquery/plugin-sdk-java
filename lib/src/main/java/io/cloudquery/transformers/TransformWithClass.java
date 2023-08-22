package io.cloudquery.transformers;

import static io.cloudquery.schema.Table.*;
import static io.cloudquery.transformers.IgnoreInTestsTransformer.DefaultIgnoreInTestsTransformer;
import static io.cloudquery.transformers.NameTransformer.DefaultNameTransformer;
import static io.cloudquery.transformers.TypeTransformer.DefaultTypeTransformer;

import io.cloudquery.schema.Column;
import io.cloudquery.schema.Column.ColumnBuilder;
import io.cloudquery.schema.Table;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import lombok.Builder;
import lombok.Singular;

@Builder(builderMethodName = "innerBuilder")
public class TransformWithClass implements Transform {
  @Builder.Default private NameTransformer nameTransformer = new DefaultNameTransformer();
  @Builder.Default private TypeTransformer typeTransformer = new DefaultTypeTransformer();

  @Builder.Default
  private ResolverTransformer resolverTransformer =
      new ResolverTransformer.DefaultResolverTransformer();

  @Builder.Default
  private IgnoreInTestsTransformer ignoreInTestsTransformer = new DefaultIgnoreInTestsTransformer();

  @Singular private Set<String> pkFields;
  private final Set<String> pkFieldsFound = new HashSet<>();

  @Singular private Set<String> unwrapFields;

  private final Class<?> clazz;

  @Override
  public void transformTable(Table table) throws TransformerException {
    for (Field field : clazz.getDeclaredFields()) {
      if (shouldUnwrapField(field)) {
        for (Field innerField : field.getType().getDeclaredFields()) {
          addColumnFromField(table, innerField, field);
        }
      } else {
        addColumnFromField(table, field, null);
      }
    }

    validatePrimaryKeysHaveBeenFound();
  }

  private void addColumnFromField(Table table, Field field, Field parent)
      throws TransformerException {
    String path = field.getName();
    String name = nameTransformer.transform(field);

    if (parent != null) {
      name = nameTransformer.transform(parent) + "_" + name;
      path = parent.getName() + "." + path;
    }

    ColumnBuilder columnBuilder =
        Column.builder()
            .name(name)
            .type(typeTransformer.transform(field))
            .resolver(resolverTransformer.transform(field, path))
            .ignoreInTests(ignoreInTestsTransformer.transform(field));

    if (pkFields.contains(path)) {
      columnBuilder.primaryKey(true);
      pkFieldsFound.add(path);
    }

    table.getColumns().add(columnBuilder.build());
  }

  private boolean shouldUnwrapField(Field field) {
    return unwrapFields.contains(field.getName());
  }

  private void validatePrimaryKeysHaveBeenFound() throws TransformerException {
    Set<String> missingPrimaryKeys = new HashSet<>(pkFields);
    missingPrimaryKeys.removeAll(pkFieldsFound);
    if (!missingPrimaryKeys.isEmpty()) {
      throw new TransformerException(
          "failed to create all of the desired primary keys: " + missingPrimaryKeys);
    }
  }

  public static TransformWithClassBuilder builder(Class<?> clazz) {
    return innerBuilder().clazz(clazz);
  }
}
