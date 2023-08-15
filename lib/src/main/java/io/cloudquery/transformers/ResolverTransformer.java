package io.cloudquery.transformers;

import io.cloudquery.helper.ReflectionPathResolver;
import io.cloudquery.helper.ReflectionPathResolver.PathResolverException;
import io.cloudquery.schema.ColumnResolver;

import java.lang.reflect.Field;

public interface ResolverTransformer {
    class DefaulResolverTransformer implements ResolverTransformer {
        @Override
        public ColumnResolver transform(Field field, String path) throws TransformerException {
            return (meta, resource, column) -> {
                try {
                    resource.set(column.getName(), ReflectionPathResolver.resolve(resource.getItem(), path));
                } catch (PathResolverException ex) {
                    throw new TransformerException("Failed to resolve path: " + path, ex);
                }
            };
        }
    }

    ColumnResolver transform(Field field, String path) throws TransformerException;
}
