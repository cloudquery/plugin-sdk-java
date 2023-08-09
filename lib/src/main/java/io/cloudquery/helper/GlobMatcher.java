package io.cloudquery.helper;

import lombok.Getter;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;

public class GlobMatcher {
    private final PathMatcher pathMatcher;

    @Getter
    private final String stringMatch;

    public GlobMatcher(String stringMatch) {
        this.stringMatch = stringMatch;
        this.pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + stringMatch);
    }

    public boolean matches(String name) {
        return pathMatcher.matches(Path.of(name));
    }
}
