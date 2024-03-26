package io.cloudquery.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class PackageJson {
  @NonNull private final String name;
  @NonNull private final String team;
  @NonNull private final String kind;
  @NonNull private final String version;
  @NonNull private final String message;
  @NonNull private final List<SupportedTargetJson> supported_targets;

  private final int schema_version = 1;
  private int[] protocols = {3};
  private String package_type = "docker";

  public String toJson() throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    return objectMapper.writeValueAsString(this);
  }
}
