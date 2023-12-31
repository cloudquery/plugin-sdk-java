package io.cloudquery.transformers;

import io.cloudquery.helper.ReflectionPathResolver;
import io.cloudquery.helper.ReflectionPathResolver.PathResolverException;
import io.cloudquery.scalar.ValidationException;
import io.cloudquery.schema.ColumnResolver;
import java.lang.reflect.Field;

public interface ResolverTransformer {

  ColumnResolver transform(Field field, String path) throws TransformerException;

  class DefaultResolverTransformer implements ResolverTransformer {
    @Override
    public ColumnResolver transform(Field field, String path) throws TransformerException {
      return (meta, resource, column) -> {
        try {
          resource.set(column.getName(), ReflectionPathResolver.resolve(resource.getItem(), path));
        } catch (PathResolverException | ValidationException ex) {
          throw new TransformerException("Failed to resolve path: " + path, ex);
        }
      };
    }
  }
}
