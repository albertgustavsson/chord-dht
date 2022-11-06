package se.umu.cs.ads.chord;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Optional;

public class ChordNode implements ChordGrpcServerHandler {
	private static final int port = 4321;
	private static final int hashBits = 160;
	private static final BigInteger hashRangeSize = BigInteger.ONE.shiftLeft(hashBits);

	private final ChordGrpcServer server; // Server for incoming requests
	private final MessageDigest hasher;

	private static final int fingerTableSize = 3; // 1 for only successor
	private final NodeInfo[] fingerTable = new NodeInfo[fingerTableSize];
	private NodeInfo predecessorNode; // Predecessor's address and identifier
	private final NodeInfo localNode; // This node's address and identifier

	private int nextFingerToFix;

	/**
	 * Constructor for a Chord node.
	 *
	 * @throws NoSuchAlgorithmException if a MessageDigest for SHA-1 cannot be found.
	 * @throws IOException              if there is an error with address resolution or server initialization.
	 */
	public ChordNode() throws NoSuchAlgorithmException, IOException {
		hasher = MessageDigest.getInstance("SHA-1");
		String localNodeAddress = InetAddress.getLocalHost().getHostAddress(); // Get the node's own address
		BigInteger localNodeId = calculateHash(localNodeAddress); // Calculate the node's own identifier
		localNode = new NodeInfo(localNodeId, localNodeAddress);

		fingerTable[0] = new NodeInfo(localNode); // Successor is the node itself

		// Start server for requests from other nodes
		server = new ChordGrpcServer(this, port);
		System.out.println("Node is listening on " + localNode.address + ":" + port);
	}

	/**
	 * Constructor for a Chord node. Also joins and existing Chord network.
	 *
	 * @param otherNode address to a Chord node in an existing Chord network.
	 *
	 * @throws NoSuchAlgorithmException if a MessageDigest for SHA-1 cannot be found.
	 * @throws IOException              if there is an error with address resolution or server initialization.
	 */
	public ChordNode(String otherNode) throws NoSuchAlgorithmException, IOException {
		this();
		this.join(otherNode);
	}

	public BigInteger getLocalId() {
		return localNode.id;
	}

	public String getLocalAddress() {
		return localNode.address;
	}

	@Override
	public String toString() {
		return "ChordNode{" + "\n\tfingerTable=" + Arrays.toString(fingerTable) + "\n\tpredecessorNode=" +
			predecessorNode + "\n\tlocalNode=" + localNode + "\n}";
	}

	/**
	 * Initiates shutdown of the server. Preexisting calls may continue, but no new calls can be made to the server.
	 * awaitTermination should be used to wait until all preexisting calls have finished.
	 */
	public void shutdown() {
		server.shutdown();
	}

	/**
	 * Wait for any ongoing calls to the server to finish.
	 *
	 * @throws InterruptedException if the method is interrupted while waiting.
	 */
	public void awaitTermination() throws InterruptedException {
		server.awaitTermination();
	}

	/**
	 * Calculate the hash of a String with SHA-1.
	 *
	 * @param input the String to be hashed.
	 *
	 * @return the 160-bit hash value.
	 */
	private BigInteger calculateHash(String input) {
		byte[] hashBytes = hasher.digest(input.getBytes());

		// Set the number of hash bits to use. Ignores any higher bits.
		BigInteger bitMask = BigInteger.ONE.shiftLeft(hashBits).subtract(BigInteger.ONE);
		return new BigInteger(1, hashBytes).and(bitMask);
	}

	/**
	 * Join an existing Chord network.
	 *
	 * @param address the address to a node in the existing network.
	 */
	private void join(String address) {
		predecessorNode = null;
		this.fingerTable[0] = ChordGrpcClient.findSuccessor(address, port, localNode.id);
	}

	/**
	 * Finds the predecessor of an identifier.
	 *
	 * @param id the identifier to find the predecessor of.
	 *
	 * @return the node that precedes the identifier.
	 */
	private NodeInfo findPredecessor(BigInteger id) {
		NodeInfo nPrime = localNode;
		NodeInfo nPrimeSuccessor = ChordGrpcClient.getSuccessor(nPrime.address, port);
		while (!((RangeUtils.valueIsInRangeExclIncl(id, nPrime.id, nPrimeSuccessor.id, hashRangeSize)) ||
			nPrime.address.equals(nPrimeSuccessor.address))) {
			nPrime = ChordGrpcClient.closestPrecedingFinger(nPrime.address, port, id);
			nPrimeSuccessor = ChordGrpcClient.getSuccessor(nPrime.address, port);
		}

		return new NodeInfo(nPrime);
	}

	/**
	 * Verifies this node's successor and notifies the successor of this node. This method should be called
	 * periodically.
	 */
	private void stabilize() { // TODO: call this periodically
		// Get successors predecessor
		Optional<NodeInfo> nodeInfoOptional = ChordGrpcClient.getPredecessor(fingerTable[0].address, port);
		if (nodeInfoOptional.isPresent()) {
			NodeInfo x = nodeInfoOptional.get();
			if (RangeUtils.valueIsInRangeExclExcl(x.id, localNode.id, fingerTable[0].id, hashRangeSize)) {
				fingerTable[0] = x;
			}
		}

		// Notify successor that I think I'm their predecessor
		ChordGrpcClient.notify(fingerTable[0].address, port, localNode);
	}

	/**
	 * Refreshes finger table entries. This method should be called periodically.
	 */
	private void fixFingers() { // TODO: call this periodically
		nextFingerToFix = (nextFingerToFix + 1) % fingerTableSize;
		fingerTable[nextFingerToFix] = ChordGrpcClient.findSuccessor(localNode.address, port,
			localNode.id.add(BigInteger.ONE.shiftLeft(nextFingerToFix)));
	}

	/**
	 * Checks if the predecessor has failed. This method should be called periodically.
	 */
	private void checkPredecessor() { // TODO: call this periodically
		int healthCheckTimeout = 150;
		if (ChordGrpcClient.healthCheck(predecessorNode.address, port, healthCheckTimeout)) {
			// Predecessor has failed.
			predecessorNode = null;
		}
	}

	/**
	 * Handler for incoming healthCheck requests.
	 *
	 * @return the status of the node.
	 */
	@Override
	public boolean healthCheck() {
		System.out.println("Got healthCheck request");
		return true;
	}

	/**
	 * Handler for incoming findSuccessor requests.
	 *
	 * @param id the identifier to find the successor of.
	 *
	 * @return the node that succeeds the identifier.
	 */
	@Override
	public NodeInfo findSuccessor(BigInteger id) {
		System.out.println("Got findSuccessor request for identifier 0x" + id.toString(16));

		NodeInfo idPredecessor = this.findPredecessor(id);
		return ChordGrpcClient.getSuccessor(idPredecessor.address, port);
	}

	/**
	 * Handler for incoming getSuccessor requests.
	 *
	 * @return the successor of this node.
	 */
	@Override
	public NodeInfo getSuccessor() {
		return new NodeInfo(fingerTable[0]);
	}

	/**
	 * Handler for incoming getPredecessor requests.
	 *
	 * @return the predecessor of this node if it has one.
	 */
	@Override
	public Optional<NodeInfo> getPredecessor() {
		return Optional.ofNullable(predecessorNode);
	}

	/**
	 * Find the highest known node preceding the given identifier based on this node's finger table.
	 *
	 * @param id the identifier.
	 *
	 * @return the highest known node.
	 */
	@Override
	public NodeInfo closestPrecedingFinger(BigInteger id) {
		for (int i = fingerTableSize - 1; i >= 0; i--) {
			if (fingerTable[i] != null && RangeUtils.valueIsInRangeExclExcl(fingerTable[i].id, localNode.id, id,
				hashRangeSize)) {
				return fingerTable[i];
			}
		}
		return new NodeInfo(localNode); // Return this node as the closest preceding node.
	}

	/**
	 * Handler for incoming notify requests.
	 *
	 * @param potentialPredecessor a potential predecessor of this node.
	 */
	@Override
	public void notify(NodeInfo potentialPredecessor) {
		if (predecessorNode == null || RangeUtils.valueIsInRangeExclExcl(
			potentialPredecessor.id, predecessorNode.id, localNode.id, hashRangeSize)) {
			predecessorNode = potentialPredecessor;
		}
	}

	public static void main(String[] args) throws NoSuchAlgorithmException, IOException, InterruptedException {
		ChordNode node = new ChordNode();
		System.out.println(node);

		System.out.println("Performing health check on self...");
		System.out.println("Health check returned: " + ChordGrpcClient.healthCheck("localhost", port, 150));

		BigInteger id;
		id = node.getLocalId();
		System.out.println("Finding the successor to 0x" + id.toString(16) + " : " +
			ChordGrpcClient.findSuccessor("localhost", port, id));
		id = node.getLocalId().add(BigInteger.ONE);
		System.out.println("Finding the successor to 0x" + id.toString(16) + " : " +
			ChordGrpcClient.findSuccessor("localhost", port, id));
		id = node.getLocalId().subtract(BigInteger.ONE);
		System.out.println("Finding the successor to 0x" + id.toString(16) + " : " +
			ChordGrpcClient.findSuccessor("localhost", port, id));

		node.shutdown();
		node.awaitTermination();
	}
}
