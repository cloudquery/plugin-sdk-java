package io.cloudquery.memdb;

import java.util.UUID;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class Table1Data {
  private UUID id;
  private String name;
}
