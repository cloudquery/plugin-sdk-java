package io.cloudquery.memdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Spec {
  public static Spec fromJSON(String json) throws JsonMappingException, JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    Spec spec = objectMapper.readValue(json, Spec.class);
    if (spec.getConcurrency() == 0) {
      spec.setConcurrency(100);
    }
    return spec;
  }

  private int concurrency;

  public Spec() {}
}
