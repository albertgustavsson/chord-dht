package se.umu.cs.ads.chord;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Empty;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public class ChordGrpcClient {
	/**
	 * Perform a health check of a Chord node.
	 *
	 * @param address the address to the node.
	 * @param port    the port to use for connecting to the node.
	 * @param timeout the maximum time to wait for a response in milliseconds.
	 *
	 * @return the status returned by the node.
	 */
	public static boolean healthCheck(String address, int port, int timeout) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
		ChordServiceGrpc.ChordServiceBlockingStub stub = ChordServiceGrpc.newBlockingStub(channel);

		boolean status = false;
		try {
			HealthCheckResponse response = stub.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS).healthCheck(
				Empty.getDefaultInstance());
			status = response.getStatus();
		} catch (StatusRuntimeException e) { // On timeout
			if (e.getStatus().getCode().equals(Status.DEADLINE_EXCEEDED.getCode())) {
				System.out.println("Health check deadline exceeded: " + e.getStatus().getDescription());
			} else {
				channel.shutdown();
				throw e;
			}
		}

		channel.shutdown();
		return status;
	}

	/**
	 * Call the findSuccessor method on another node.
	 *
	 * @param address    the address to the node.
	 * @param port       the port to use for connecting to the node.
	 * @param identifier the identifier to pass to the method.
	 *
	 * @return the successor returned from the node.
	 */
	public static NodeInfo findSuccessor(String address, int port, BigInteger identifier) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
		ChordServiceGrpc.ChordServiceBlockingStub stub = ChordServiceGrpc.newBlockingStub(channel);

		Identifier request = GrpcTypeHelper.identifierFromBigInteger(identifier);

		Node response = stub.findSuccessor(request);

		channel.shutdown();
		return GrpcTypeHelper.nodeInfoFromNode(response);
	}

	/**
	 * Call the getSuccessor method on another node.
	 *
	 * @param address the address to the node.
	 * @param port    the port to use for connecting to the node.
	 *
	 * @return the successor returned from the node.
	 */
	public static NodeInfo getSuccessor(String address, int port) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
		ChordServiceGrpc.ChordServiceBlockingStub stub = ChordServiceGrpc.newBlockingStub(channel);

		Node response = stub.getSuccessor(Empty.getDefaultInstance());

		channel.shutdown();

		return GrpcTypeHelper.nodeInfoFromNode(response);
	}

	/**
	 * Call the getPredecessor method on another node.
	 *
	 * @param address the address to the node.
	 * @param port    the port to use for connecting to the node.
	 *
	 * @return the predecessor returned from the node.
	 */
	public static NodeInfo getPredecessor(String address, int port) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
		ChordServiceGrpc.ChordServiceBlockingStub stub = ChordServiceGrpc.newBlockingStub(channel);

		Node response = stub.getPredecessor(Empty.getDefaultInstance());

		channel.shutdown();

		return GrpcTypeHelper.nodeInfoFromNode(response);
	}

	/**
	 * Call the setPredecessor method on another node.
	 *
	 * @param address     the address to the node.
	 * @param port        the port to use for connecting to the node.
	 * @param predecessor the node to set as the predecessor.
	 */
	public static void setPredecessor(String address, int port, NodeInfo predecessor) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
		ChordServiceGrpc.ChordServiceBlockingStub stub = ChordServiceGrpc.newBlockingStub(channel);

		Empty response = stub.setPredecessor(GrpcTypeHelper.nodeFromNodeInfo(predecessor));

		channel.shutdown();
	}

	/**
	 * Call the updateFingerTable method on another node.
	 *
	 * @param address the address to the node.
	 * @param port    the port to use for connecting to the node.
	 * @param node    the node to potentially put in the target node's finger table.
	 * @param index   the index in the finger table.
	 */
	public static void updateFingerTable(String address, int port, NodeInfo node, int index) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
		ChordServiceGrpc.ChordServiceBlockingStub stub = ChordServiceGrpc.newBlockingStub(channel);

		Empty response = stub.updateFingerTable(UpdateFingerTableRequest.newBuilder().setNode(
			GrpcTypeHelper.nodeFromNodeInfo(node)).setIndex(index).build());

		channel.shutdown();
	}

	/**
	 * Call the closestPrecedingFinger method on another node.
	 *
	 * @param address    the address to the node.
	 * @param port       the port to use for connecting to the node.
	 * @param identifier the identifier to pass to the method.
	 *
	 * @return the node returned from the node.
	 */
	public static NodeInfo closestPrecedingFinger(String address, int port, BigInteger identifier) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
		ChordServiceGrpc.ChordServiceBlockingStub stub = ChordServiceGrpc.newBlockingStub(channel);

		Identifier request = GrpcTypeHelper.identifierFromBigInteger(identifier);

		Node response = stub.closestPrecedingFinger(request);

		channel.shutdown();
		return GrpcTypeHelper.nodeInfoFromNode(response);
	}

	/**
	 * Call the notify method on another node.
	 *
	 * @param address the address to the node.
	 * @param port    the port to use for connecting to the node.
	 * @param node    the node to pass as the potential predecessor.
	 */
	public static void notify(String address, int port, NodeInfo node) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
		ChordServiceGrpc.ChordServiceBlockingStub stub = ChordServiceGrpc.newBlockingStub(channel);

		Node request = GrpcTypeHelper.nodeFromNodeInfo(node);
		Empty response = stub.notify(request);

		channel.shutdown();
	}
}
