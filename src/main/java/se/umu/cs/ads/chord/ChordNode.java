package se.umu.cs.ads.chord;

import io.grpc.Context;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class ChordNode extends ChordServiceGrpc.ChordServiceImplBase {
	private static final int port = 4321;

	private final Server server; // Server for incoming requests
	private final MessageDigest hasher;

	private static class NodeEntry {
		final BigInteger identifier;
		final String address;
		public NodeEntry(BigInteger identifier, String address) {
			this.identifier = identifier;
			this.address = address;
		}
		public NodeEntry(NodeEntry other) { // Copy constructor
			this(other.identifier, other.address);
		}
		@Override
		public String toString() {
			return "NodeEntry{identifier=0x" + identifier.toString(16) + ", address='" + address + "'}";
		}
	}

	private static final int fingerTableSize = 3; // 1 for only successor
	private final NodeEntry[] fingerTable = new NodeEntry[fingerTableSize];
	private NodeEntry predecessorNode; // Predecessor's address and identifier
	private final NodeEntry localNode; // This node's address and identifier

	/**
	 * Constructor for a Chord node.
	 * @throws NoSuchAlgorithmException if a MessageDigest for SHA-1 cannot be found.
	 * @throws IOException if there is an error with address resolution or server initialization.
	 */
	public ChordNode() throws NoSuchAlgorithmException, IOException {
		hasher = MessageDigest.getInstance("SHA-1");
		String localNodeAddress = InetAddress.getLocalHost().getHostAddress(); // Get the node's own address
		BigInteger localNodeIdentifier = calculateHash(localNodeAddress); // Calculate the node's own identifier
		localNode = new NodeEntry(localNodeIdentifier, localNodeAddress);

		fingerTable[0] = new NodeEntry(localNode); // Successor is the node itself

		// Start server for requests from other nodes
		server = ServerBuilder.forPort(port).addService(this).build();
		server.start();
		System.out.println("Node is listening on " + localNode.address + ":" + port);
	}

	@Override
	public String toString() {
		return "ChordNode{" +
			"\n\tfingerTable=" + Arrays.toString(fingerTable) +
			"\n\tpredecessorNode=" + predecessorNode +
			"\n\tlocalNode=" + localNode +
			"\n}";
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
	 * Calculate the hash of a String with SHA-1.
	 * @param input the String to be hashed.
	 * @return the 160-bit hash value.
	 */
	private BigInteger calculateHash(String input) {
		byte[] hashBytes = hasher.digest(input.getBytes());

		// Set the number of hash bits to use. Ignores any higher bits.
		int hashBits = 160;
		BigInteger bitMask = BigInteger.ONE.shiftLeft(hashBits).subtract(BigInteger.ONE);
		return new BigInteger(1, hashBytes).and(bitMask);
	}

	/**
	 * Handler for incoming healthCheck requests.
	 * @param request the request.
	 * @param responseObserver observer for the response.
	 */
	@Override
	public void healthCheck(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
		System.out.println("Got healthCheck request");

		if (Context.current().isCancelled()) {
			responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
			return;
		}

		HealthCheckResponse response = HealthCheckResponse.newBuilder().setStatus(true).build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * Does a health check on another node.
	 * @param nodeAddress the address to the other node.
	 * @return the status returned from the other node.
	 */
	public boolean doHealthCheck(String nodeAddress) {
		final int healthCheckTimeout = 100;
		return ChordGrpcClient.healthCheck(nodeAddress, port, healthCheckTimeout);
	}

	public static void main(String[] args) throws NoSuchAlgorithmException, IOException, InterruptedException {
		ChordNode node = new ChordNode();
		System.out.println(node);

		System.out.println("Performing health check on self...");
		System.out.println("Health check returned: " + node.doHealthCheck("localhost"));

		node.shutdown();
		node.awaitTermination();
	}
}
