package se.umu.cs.ads.chord;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

public class ChordGrpcClient {
	/**
	 * Perform a health check of a Chord node.
	 * @param address the address to the node.
	 * @param port the port to use for connecting to the node.
	 * @param timeout the maximum time to wait for a response in milliseconds.
	 * @return the status returned by the node.
	 */
	public static boolean healthCheck(String address, int port, int timeout) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
		ChordServiceGrpc.ChordServiceBlockingStub stub = ChordServiceGrpc.newBlockingStub(channel);

		HealthCheckRequest request = HealthCheckRequest.newBuilder().build();
		boolean status = false;
		try {
			HealthCheckResponse response = stub.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS).healthCheck(request);
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
	 * @param address the address to the node.
	 * @param port the port to use for connecting to the node.
	 * @param identifier the identifier to pass to the method.
	 * @return the successor returned from the node.
	 */
	public static NodeInfo findSuccessor(String address, int port, BigInteger identifier) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
		ChordServiceGrpc.ChordServiceBlockingStub stub = ChordServiceGrpc.newBlockingStub(channel);

		Identifier request = Identifier.newBuilder().setValue(ByteString.copyFrom(identifier.toByteArray())).build();

		Node response = stub.findSuccessor(request);

		channel.shutdown();
		return new NodeInfo(new BigInteger(1,response.getIdentifier().getValue().toByteArray()), response.getAddress());
	}
}
