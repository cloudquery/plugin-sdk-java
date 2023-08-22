package io.cloudquery.plugin;

import io.grpc.stub.StreamObserver;

public interface SyncStream extends StreamObserver<io.cloudquery.plugin.v3.Sync.Response> {}
