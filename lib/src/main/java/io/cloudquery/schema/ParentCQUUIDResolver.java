package io.cloudquery.schema;

import io.cloudquery.scalar.Scalar;
import io.cloudquery.scalar.UUID;
import io.cloudquery.scalar.ValidationException;
import io.cloudquery.transformers.TransformerException;

import static io.cloudquery.schema.Column.CQ_ID_COLUMN;

public class ParentCQUUIDResolver implements ColumnResolver {
    @Override
    public void resolve(ClientMeta meta, Resource resource, Column column) throws TransformerException {
        Resource parent = resource.getParent();
        if (parent == null) {
            setOrThrow(resource, column, null);
            return;
        }

        Scalar<?> parentCqID = parent.get(CQ_ID_COLUMN.getName());
        if (parentCqID == null) {
            setOrThrow(resource, column, null);
            return;
        }

        if (!(parentCqID instanceof UUID)) {
            setOrThrow(resource, column, null);
            return;
        }

        setOrThrow(resource, column, parentCqID);
    }

    private static void setOrThrow(Resource resource, Column column, Scalar<?> parentCqID) throws TransformerException {
        try {
            resource.set(column.getName(), parentCqID);
        } catch (ValidationException ex) {
            throw new TransformerException("Failed to set parent CQ ID", ex);
        }
    }
}
