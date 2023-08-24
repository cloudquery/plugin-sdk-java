package io.cloudquery.types;

import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;

public class Extensions {
  public static void registerExtensions() {
    ExtensionTypeRegistry.register(new UUIDType());
    ExtensionTypeRegistry.register(new JSONType());
  }

  private Extensions() {}
}
