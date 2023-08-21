package io.cloudquery.memdb;

import com.google.gson.Gson;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Spec {
  public static Spec fromJSON(String json) {
    Spec spec = new Gson().fromJson(json, Spec.class);
    if (spec.getConcurrency() == 0) {
      spec.setConcurrency(10000);
    }
    return spec;
  }

  private int concurrency;

  public Spec() {}
}
