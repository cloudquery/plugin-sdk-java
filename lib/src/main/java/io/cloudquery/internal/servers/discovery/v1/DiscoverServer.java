package io.cloudquery.internal.servers.discovery.v1;

import cloudquery.discovery.v1.DiscoveryGrpc.DiscoveryImplBase;
import cloudquery.discovery.v1.DiscoveryOuterClass.GetVersions.Request;
import cloudquery.discovery.v1.DiscoveryOuterClass.GetVersions.Response;
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
