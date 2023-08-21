package io.cloudquery.plugin;

import io.cloudquery.schema.SchemaException;
import io.cloudquery.schema.Table;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.logging.log4j.Logger;

@RequiredArgsConstructor
@Getter
public abstract class Plugin {
  @NonNull protected final String name;
  @NonNull protected final String version;
  @Setter protected Logger logger;

  public abstract void init();

  public abstract List<Table> tables(
      List<String> includeList, List<String> skipList, boolean skipDependentTables)
      throws SchemaException;

  public abstract void sync();

  public abstract void read();

  public abstract void write();

  public abstract void close();
}
