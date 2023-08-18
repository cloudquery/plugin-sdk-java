package io.cloudquery.plugin;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

@Getter
@AllArgsConstructor
public abstract class Plugin {
    @NonNull
    protected final String name;
    @NonNull
    protected final String version;
}
