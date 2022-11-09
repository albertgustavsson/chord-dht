package se.umu.cs.ads.chord;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChordNode implements ChordGrpcServerHandler {
	private static final int port = 4321;
	private static final int hashBits = 160;
	private static final BigInteger hashRangeSize = BigInteger.ONE.shiftLeft(hashBits);

	private final Logger logger = LoggerFactory.getLogger(ChordNode.class);

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
		logger.info("Node 0x" + localNode.id.toString(16) + " is listening on " + localNode.address + ":" + port);
		join(otherNode);
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
		logger.info("Shutting down the node");
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
			logger.info("Joining node at " + otherNode);
			initFingerTable(otherNode);
			updateOthers();
			// TODO: move keys in (predecessor, n] from successor
		} else { // This is the only node in the network
			logger.info("Creating a new Chord network");
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
		logger.info("My successor is " + fingerTable[0]);

		// predecessor = successor.predecessor
		predecessor = ChordGrpcClient.getPredecessor(fingerTable[0].address, port);
		logger.info("My predecessor is " + predecessor);

		// successor.predecessor = this node
		ChordGrpcClient.setPredecessor(fingerTable[0].address, port, localNode);

		// TODO: double-check and test
		for (int i = 0; i < fingerTableSize - 1; i++) {
			logger.info("Initializing finger table at index " + (i + 1));
			if (RangeUtils.valueIsInRangeInclExcl(fingerStart(i + 1), localNode.id, fingerTable[i].id, hashRangeSize)) {
				fingerTable[i + 1] = fingerTable[i];
			} else {
				fingerTable[i + 1] = ChordGrpcClient.findSuccessor(address, port, fingerStart(i + 1));
			}
			logger.info("Finger " + (i + 1) + " is " + fingerTable[i + 1]);
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
		logger.info("Finding the predecessor of 0x" + id.toString(16));
		NodeInfo nPrime = localNode;
		NodeInfo nPrimeSuccessor = fingerTable[0];
		while (!((RangeUtils.valueIsInRangeExclIncl(id, nPrime.id, nPrimeSuccessor.id, hashRangeSize)) ||
			nPrime.address.equals(nPrimeSuccessor.address))) {
			nPrime = ChordGrpcClient.closestPrecedingFinger(nPrime.address, port, id);
			nPrimeSuccessor = ChordGrpcClient.getSuccessor(nPrime.address, port);
		}

		logger.info("Found predecessor " + nPrime);

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
		logger.info("Got healthCheck request");
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
		logger.info("Got findSuccessor request for identifier 0x" + id.toString(16));

		NodeInfo idPredecessor = findPredecessor(id);
		return ChordGrpcClient.getSuccessor(idPredecessor.address, port);
	}

	/**
	 * Handler for incoming getSuccessor requests.
	 *
	 * @return the successor of this node.
	 */
	@Override
	public NodeInfo getSuccessor() {
		logger.info("Got getSuccessor request");
		return new NodeInfo(fingerTable[0]);
	}

	/**
	 * Handler for incoming getPredecessor requests.
	 *
	 * @return the predecessor of this node.
	 */
	@Override
	public NodeInfo getPredecessor() {
		logger.info("Got getPredecessor request");
		return predecessor;
	}

	/**
	 * Handler for incoming setPredecessor requests.
	 */
	@Override
	public void setPredecessor(NodeInfo predecessor) {
		logger.info("Got setPredecessor request for Node " + predecessor.toString());
		this.predecessor = predecessor;
	}

	/**
	 * If the passed node is the ith finger of this node, update this node’s finger table with the passed node.
	 *
	 * @param node  the node that might be put in the finger table.
	 * @param index the index in the finger table.
	 */
	@Override
	public void updateFingerTable(NodeInfo node, int index) {
		logger.info("Got updateFingerTable request for Node " + node.toString() + " at index " + index);
		logger.info("My id is 0x" + localNode.id.toString(16) + " and finger[" + index + "] is 0x" +
			fingerTable[index].id.toString(16));
		if (RangeUtils.valueIsInRangeExclExcl(node.id, localNode.id, fingerTable[index].id, hashRangeSize) ||
			localNode.address.equals(fingerTable[index].address)) {
			fingerTable[index] = node;
			logger.info("Finger " + index + " is now " + fingerTable[index]);

			// pseudocode: predecessor.updateFingerTable(node, index)
			ChordGrpcClient.updateFingerTable(predecessor.address, port, node, index);

			/* TODO: Apparently the code from the paper is wrong, so this method should not work.
			 *  Figure out how, and fix it.
			 *  Potential issues:
			 *  - the index should maybe be changed for the call to the predecessor
			 *  - the range ends (inclusive / exclusive)
			 */
		} else {
			logger.info("Did not update finger table");
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
		logger.info("Got closestPrecedingFinger request for identifier 0x" + id.toString(16));
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
		logger.info("Got notify request for Node " + potentialPredecessor.toString());
		if (predecessor == null || RangeUtils.valueIsInRangeExclExcl(
			potentialPredecessor.id, predecessor.id, localNode.id, hashRangeSize)) {
			predecessor = potentialPredecessor;
			logger.info("My predecessor is now " + potentialPredecessor);
		}
	}

	public static void main(String[] args) throws NoSuchAlgorithmException, IOException, InterruptedException {
		System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");

		String otherNodeAddress = null;
		if (args.length >= 1) {
			otherNodeAddress = args[0];
		}
		ChordNode node = new ChordNode(otherNodeAddress);
		System.out.println("Node has been initialized: " + node);

		if (!ChordGrpcClient.healthCheck("localhost", port, 500)) {
			System.err.println("Performing health check on self failed!");
		}

		// Print the state every 10 seconds
		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		executor.scheduleAtFixedRate(() -> System.out.println("Current state of the node: " + node), 2, 10,
			TimeUnit.SECONDS);

		node.awaitTermination();
	}
}
