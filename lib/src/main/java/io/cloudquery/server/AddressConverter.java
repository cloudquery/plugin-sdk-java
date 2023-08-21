package io.cloudquery.server;

import picocli.CommandLine.ITypeConverter;

public class AddressConverter implements ITypeConverter<AddressConverter.Address> {
  public static class AddressParseException extends RuntimeException {}

  public record Address(String host, int port) {}

  @Override
  public Address convert(String rawAddress) throws Exception {
    String[] components = rawAddress.split(":");
    if (components.length != 2) {
      throw new AddressParseException();
    }
    return new Address(components[0], Integer.parseInt(components[1]));
  }
}
