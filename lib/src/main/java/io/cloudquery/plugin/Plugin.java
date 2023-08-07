package io.cloudquery.plugin;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Builder(builderMethodName = "innerBuilder")
@Getter
public class Plugin {
    public static PluginBuilder builder(String name, String version) {
        return innerBuilder().name(name).verion(version);
    }

    @NonNull
    private final String name;
    @NonNull
    private final String verion;
}
