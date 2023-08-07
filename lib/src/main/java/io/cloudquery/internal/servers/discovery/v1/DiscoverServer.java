package io.cloudquery.internal.servers.discovery.v1;

import io.cloudquery.discovery.v1.DiscoveryGrpc.DiscoveryImplBase;
import io.cloudquery.discovery.v1.GetVersions.Request;
import io.cloudquery.discovery.v1.GetVersions.Response;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class DiscoverServer extends DiscoveryImplBase {
    private final List<Integer> versions;

    public DiscoverServer(List<Integer> versions) {
        this.versions = versions;
    }

    @Override
    public void getVersions(Request request, StreamObserver<Response> responseObserver) {
        responseObserver.onNext(Response.newBuilder().addAllVersions(versions).build());
        responseObserver.onCompleted();
    }
}
