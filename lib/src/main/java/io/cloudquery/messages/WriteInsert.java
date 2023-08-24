package io.cloudquery.messages;

import io.cloudquery.schema.Resource;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class WriteInsert extends WriteMessage {
  private Resource resource;
}
