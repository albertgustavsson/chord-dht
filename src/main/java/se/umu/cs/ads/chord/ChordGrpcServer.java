package se.umu.cs.ads.chord;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Optional;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

import io.grpc.Context;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class ChordGrpcServer extends ChordServiceGrpc.ChordServiceImplBase {
	private ChordGrpcServerHandler handler;
	private final Server server;

	/**
	 * Creates a new server for incoming gRPC calls.
	 * @param handler a handler for the requests.
	 * @param port the port to bind the server to.
	 * @throws IOException if there is an error with address resolution or server initialization.
	 */
	public ChordGrpcServer(ChordGrpcServerHandler handler, int port) throws IOException {
		super();
		this.handler = handler;
		server = ServerBuilder.forPort(port).addService(this).build();
		server.start();
	}

	/**
	 * Initiates shutdown of the server.
	 * Preexisting calls may continue, but no new calls can be made to the server.
	 * awaitTermination should be used to wait until all preexisting calls have finished.
	 */
	public void shutdown() {
		server.shutdown();
	}

	/**
	 * Wait for any ongoing calls to the server to finish.
	 * @throws InterruptedException if the method is interrupted while waiting.
	 */
	public void awaitTermination() throws InterruptedException {
		server.awaitTermination();
	}

	/**
	 * Handler for incoming healthCheck requests.
	 * @param request the request.
	 * @param responseObserver observer for the response.
	 */
	@Override
	public void healthCheck(Empty request, StreamObserver<HealthCheckResponse> responseObserver) {
		if (Context.current().isCancelled()) {
			responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
			return;
		}

		boolean status = handler.healthCheck();

		HealthCheckResponse response = HealthCheckResponse.newBuilder().setStatus(status).build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * Handler for incoming findSuccessor requests.
	 * @param request the request.
	 * @param responseObserver observer for the response.
	 */
	@Override
	public void findSuccessor(Identifier request, StreamObserver<Node> responseObserver) {
		BigInteger identifier = new BigInteger(1,request.getValue().toByteArray());

		NodeInfo successor = handler.findSuccessor(identifier);

		Node response = Node.newBuilder()
			.setIdentifier(Identifier.newBuilder().setValue(
				ByteString.copyFrom(successor.identifier.toByteArray())
			).build())
			.setAddress(successor.address)
			.build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * Handler for incoming getPredecessor requests.
	 * @param request the request.
	 * @param responseObserver observer for the response.
	 */
	@Override
	public void getPredecessor(Empty request, StreamObserver<GetPredecessorResponse> responseObserver) {
		Optional<NodeInfo> predecessorOptional = handler.getPredecessor();

		GetPredecessorResponse.Builder responseBuilder = GetPredecessorResponse.newBuilder();

		predecessorOptional.ifPresent(nodeInfo -> responseBuilder.setNode(Node.newBuilder()
			.setIdentifier(Identifier.newBuilder().setValue(ByteString.copyFrom(nodeInfo.identifier.toByteArray())).build())
			.setAddress(nodeInfo.address)
			.build()));

		GetPredecessorResponse response = responseBuilder.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * Handler for incoming notify requests.
	 * @param request the request.
	 * @param responseObserver observer for the response.
	 */
	@Override
	public void notify(Node request, StreamObserver<Empty> responseObserver) {
		NodeInfo potentialPredecessor = new NodeInfo(
			new BigInteger(1,request.getIdentifier().getValue().toByteArray()),
			request.getAddress());

		handler.notify(potentialPredecessor);

		responseObserver.onNext(Empty.getDefaultInstance());
		responseObserver.onCompleted();
	}
}
