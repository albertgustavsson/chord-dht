package se.umu.cs.ads.chord;

import com.google.protobuf.ByteString;
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
	private static final int hashBits = 160;
	private static final BigInteger hashRangeSize = BigInteger.ONE.shiftLeft(hashBits);

	private final Server server; // Server for incoming requests
	private final MessageDigest hasher;

	private static final int fingerTableSize = 3; // 1 for only successor
	private final NodeInfo[] fingerTable = new NodeInfo[fingerTableSize];
	private NodeInfo predecessorNode; // Predecessor's address and identifier
	private final NodeInfo localNode; // This node's address and identifier

	/**
	 * Constructor for a Chord node.
	 * @throws NoSuchAlgorithmException if a MessageDigest for SHA-1 cannot be found.
	 * @throws IOException if there is an error with address resolution or server initialization.
	 */
	public ChordNode() throws NoSuchAlgorithmException, IOException {
		hasher = MessageDigest.getInstance("SHA-1");
		String localNodeAddress = InetAddress.getLocalHost().getHostAddress(); // Get the node's own address
		BigInteger localNodeIdentifier = calculateHash(localNodeAddress); // Calculate the node's own identifier
		localNode = new NodeInfo(localNodeIdentifier, localNodeAddress);

		fingerTable[0] = new NodeInfo(localNode); // Successor is the node itself

		// Start server for requests from other nodes
		server = ServerBuilder.forPort(port).addService(this).build();
		server.start();
		System.out.println("Node is listening on " + localNode.address + ":" + port);
	}

	public BigInteger getLocalIdentifier() {
		return localNode.identifier;
	}

	public String getLocalAddress() {
		return localNode.address;
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
	 * Handler for incoming findSuccessor requests.
	 * @param request the request.
	 * @param responseObserver observer for the response.
	 */
	@Override
	public void findSuccessor(Identifier request, StreamObserver<Node> responseObserver) {
		BigInteger identifier = new BigInteger(1,request.getValue().toByteArray());
		System.out.println("Got findSuccessor request for identifier 0x" + identifier.toString(16));

		NodeInfo successor;
		if (this.localNode.address.equals(this.fingerTable[0].address)) {
			// This node is its own successor. It is alone in the ring.
			System.out.println("The identifier is between me and my successor (myself). Returning myself.");
			successor = new NodeInfo(this.localNode);
		} else if (RangeUtils.valueIsInRangeExclIncl(identifier, this.localNode.identifier, this.fingerTable[0].identifier, hashRangeSize)) {
			// Requested identifier is between this node and the successor. Return the successor.
			System.out.println("The identifier is between me and my successor. Returning my successor.");
			successor = new NodeInfo(this.fingerTable[0]);
		} else {
			NodeInfo nPrime = closestPrecedingNode(identifier);
			System.out.println("Forwarding request to " + nPrime.address);
			successor = ChordGrpcClient.findSuccessor(identifier, nPrime.address, port);
		}

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
	 * Find the highest known node preceding the given identifier based on this node's finger table.
	 * @param identifier the identifier.
	 * @return the highest known node.
	 */
	private NodeInfo closestPrecedingNode(BigInteger identifier) {
		for (int i = fingerTableSize-1; i >= 0; i--) {
			if (fingerTable[i] != null && RangeUtils.valueIsInRangeExclExcl(fingerTable[i].identifier, this.localNode.identifier, identifier, hashRangeSize)) {
				return fingerTable[i];
			}
		}
		return new NodeInfo(localNode); // Return this node as the closest preceding node.
	}

	public static void main(String[] args) throws NoSuchAlgorithmException, IOException, InterruptedException {
		ChordNode node = new ChordNode();
		System.out.println(node);

		System.out.println("Performing health check on self...");
		System.out.println("Health check returned: " + ChordGrpcClient.healthCheck("localhost", port, 150));

		BigInteger identifier;
		identifier = node.getLocalIdentifier();
		System.out.println("Finding the successor to 0x" + identifier.toString(16) + " : " + ChordGrpcClient.findSuccessor(identifier,"localhost", port));
		identifier = node.getLocalIdentifier().add(BigInteger.ONE);
		System.out.println("Finding the successor to 0x" + identifier.toString(16) + " : " + ChordGrpcClient.findSuccessor(identifier,"localhost", port));
		identifier = node.getLocalIdentifier().subtract(BigInteger.ONE);
		System.out.println("Finding the successor to 0x" + identifier.toString(16) + " : " + ChordGrpcClient.findSuccessor(identifier,"localhost", port));

		node.shutdown();
		node.awaitTermination();
	}
}
