package io.cloudquery.helper;

import java.lang.reflect.Field;

/**
 * A simple reflection-based path resolver.
 */
public class ReflectionPathResolver {
    
    public static class PathResolverException extends Exception {
        public PathResolverException(String message, Throwable ex) {
            super(message, ex);
        }
    }

    /**
     * Resolve a path of an object using reflection.
     * <p>
     * e.g. if we have a class:
     * <pre>
     *      class TestClass {
     *          private String name;
     *          private TestClass child;
     *      }
     * </pre>
     * <p>
     * Then the following are valid paths to retrieve the associated values:
     * <p>
     * `name`
     * `child.name`
     * </p>
     * NOTE: this implementation is currently very simplistic and only supports simple field and nested field resolution.
     * It does not support collection resolution - unlike the Go SDK which uses <a href="https://github.com/thoas/go-funk#get">go-funk</a>.
     *
     * @param object The object to resolve the path on
     * @param path   The path to resolve
     * @return The value of the property at the resolved path
     * @throws PathResolverException If the path cannot be resolved
     */
    public static Object resolve(Object object, String path) throws PathResolverException {
        Object current = object;

        for (String currentPath : path.split("\\.")) {
            try {
                Field currentField = object.getClass().getDeclaredField(currentPath);
                if (!currentField.canAccess(current)) {
                    currentField.setAccessible(true);
                }
                object = currentField.get(object);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new PathResolverException("Unable to resolve path " + currentPath, e);
            }
        }

        return object;
    }
}
