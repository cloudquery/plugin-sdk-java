package io.cloudquery.plugin;

import java.util.List;

import org.apache.logging.log4j.Logger;

import io.cloudquery.schema.SchemaException;
import io.cloudquery.schema.Table;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
@Getter
public abstract class Plugin {
    @NonNull
    protected final String name;
    @NonNull
    protected final String version;
    @Setter
    protected Logger logger;

    public abstract void init();

    public abstract List<Table> tables() throws SchemaException;

    public abstract void sync();

    public abstract void read();

    public abstract void write();

    public abstract void close();

}
