package io.cloudquery.memdb;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class Table1Data {
  private UUID id;
  private String name;
  private LocalDateTime timestamp;
  private Map<String, String> json;
}
