package se.umu.cs.ads.chord;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class ChordNode implements ChordGrpcServerHandler {
	private static final int port = 4321;
	private static final int hashBits = 160;
	private static final BigInteger hashRangeSize = BigInteger.ONE.shiftLeft(hashBits);

	private final ChordGrpcServer server; // Server for incoming requests
	private final MessageDigest hasher;

	private static final int fingerTableSize = 3; // 1 for only successor
	private final NodeInfo[] fingerTable = new NodeInfo[fingerTableSize];
	private NodeInfo predecessor; // Predecessor's address and identifier
	private final NodeInfo localNode; // This node's address and identifier

	private int nextFingerToFix;

	/**
	 * Constructor for a Chord node that also connects to an existing Chord network.
	 *
	 * @param otherNode address to a Chord node in an existing Chord network.
	 *
	 * @throws NoSuchAlgorithmException if a MessageDigest for SHA-1 cannot be found.
	 * @throws IOException              if there is an error with address resolution or server initialization.
	 */
	public ChordNode(String otherNode) throws NoSuchAlgorithmException, IOException {
		hasher = MessageDigest.getInstance("SHA-1");
		String localNodeAddress = InetAddress.getLocalHost().getHostAddress(); // Get the node's own address
		BigInteger localNodeId = calculateHash(localNodeAddress); // Calculate the node's own identifier
		localNode = new NodeInfo(localNodeId, localNodeAddress);
		// Start server for requests from other nodes
		server = new ChordGrpcServer(this, port);
		System.out.println("Node is listening on " + localNode.address + ":" + port);
		join(otherNode);
	}

	/**
	 * Constructor for a Chord node that forms a new Chord network.
	 *
	 * @throws NoSuchAlgorithmException if a MessageDigest for SHA-1 cannot be found.
	 * @throws IOException              if there is an error with address resolution or server initialization.
	 */
	public ChordNode() throws NoSuchAlgorithmException, IOException {
		this(null);
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
			predecessor + "\n\tlocalNode=" + localNode + "\n}";
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

	private BigInteger fingerStart(int finger) {
		return localNode.id.add(BigInteger.ONE.shiftLeft(finger)).mod(hashRangeSize);
	}

	/**
	 * Join an existing Chord network.
	 *
	 * @param otherNode the address to a node in the existing network.
	 */
	private void join(String otherNode) {
		if (otherNode != null) { // Should join another node
			initFingerTable(otherNode);
			updateOthers();
			// TODO: move keys in (predecessor, n] from successor
		} else { // This is the only node in the network
			// All fingers point to the node itself
			for (int i = 0; i < fingerTableSize; i++) {
				fingerTable[i] = new NodeInfo(localNode);
			}
			// The predecessor is the node itself
			predecessor = new NodeInfo(localNode);
		}
	}

	/**
	 * Initialize finger table of local node.
	 *
	 * @param address an arbitrary node already in the network
	 */
	private void initFingerTable(String address) {
		fingerTable[0] = ChordGrpcClient.findSuccessor(address, port, fingerStart(0));

		// predecessor = successor.predecessor
		predecessor = ChordGrpcClient.getPredecessor(fingerTable[0].address, port);

		// successor.predecessor = this node
		ChordGrpcClient.setPredecessor(fingerTable[0].address, port, localNode);

		// TODO: double-check and test
		for (int i = 0; i < fingerTableSize - 1; i++) {
			if (RangeUtils.valueIsInRangeInclExcl(fingerStart(i + 1), localNode.id, fingerTable[i].id, hashRangeSize)) {
				fingerTable[i + 1] = fingerTable[i];
			} else {
				fingerTable[i + 1] = ChordGrpcClient.findSuccessor(address, port, fingerStart(i + 1));
			}
		}
	}

	/**
	 * Update all nodes whose finger tables should refer to this node.
	 */
	private void updateOthers() {
		for (int i = 0; i < fingerTableSize; i++) {
			// find last node p whose ith finger might be this node
			// p = find_predecessor(n - 2^(i));
			NodeInfo p = findPredecessor(localNode.id.subtract(BigInteger.ONE.shiftLeft(i)));
			// p.update_finger_table(n, i);
			ChordGrpcClient.updateFingerTable(p.address, port, localNode, i);
		}
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
		NodeInfo x = ChordGrpcClient.getPredecessor(fingerTable[0].address, port);
		if (RangeUtils.valueIsInRangeExclExcl(x.id, localNode.id, fingerTable[0].id, hashRangeSize)) {
			fingerTable[0] = x;
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
		if (ChordGrpcClient.healthCheck(predecessor.address, port, healthCheckTimeout)) {
			// Predecessor has failed.
			predecessor = null;
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
	 * @return the predecessor of this node.
	 */
	@Override
	public NodeInfo getPredecessor() {
		return predecessor;
	}

	/**
	 * Handler for incoming setPredecessor requests.
	 */
	@Override
	public void setPredecessor(NodeInfo predecessor) {
		this.predecessor = predecessor;
	}

	/**
	 * If the passed node is the i:th finger of this node, update this node’s finger table with the passed node.
	 *
	 * @param node  the node that might be put in the finger table.
	 * @param index the index in the finger table.
	 */
	@Override
	public void updateFingerTable(NodeInfo node, int index) {
		if (RangeUtils.valueIsInRangeInclExcl(node.id, localNode.id, fingerTable[index].id, hashRangeSize)) {
			fingerTable[index] = node;

			// pseudocode: predecessor.updateFingerTable(node, index)
			ChordGrpcClient.updateFingerTable(predecessor.address, port, node, index);

			/* TODO: Apparently the code from the paper is wrong, so this method should not work.
			 *  Figure out how, and fix it.
			 *  Potential issues:
			 *  - the index should maybe be changed for the call to the predecessor
			 *  - the range ends (inclusive / exclusive)
			 */
		}
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
		if (predecessor == null || RangeUtils.valueIsInRangeExclExcl(
			potentialPredecessor.id, predecessor.id, localNode.id, hashRangeSize)) {
			predecessor = potentialPredecessor;
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
