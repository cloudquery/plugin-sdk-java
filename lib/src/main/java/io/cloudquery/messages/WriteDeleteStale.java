package io.cloudquery.messages;

import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class WriteDeleteStale extends WriteMessage {
  private String tableName;
  private String sourceName;
  private Date timestamp;
}
