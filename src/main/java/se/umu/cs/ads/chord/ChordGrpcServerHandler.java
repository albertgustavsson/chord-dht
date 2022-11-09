package se.umu.cs.ads.chord;

import java.math.BigInteger;

/**
 * Interface for methods to handle incoming requests for the Chord node.
 */
public interface ChordGrpcServerHandler {
	/**
	 * Check if the node is healthy.
	 *
	 * @return true if the node is healthy.
	 */
	boolean healthCheck();

	/**
	 * Find the successor of an identifier.
	 *
	 * @param id the identifier to find the successor of.
	 *
	 * @return the Chord node succeeding the identifier.
	 */
	NodeInfo findSuccessor(BigInteger id);

	/**
	 * Get the successor of a Chord node.
	 *
	 * @return the other Chord node that succeeds the called Chord node.
	 */
	NodeInfo getSuccessor();

	/**
	 * Get the predecessor of a Chord node.
	 *
	 * @return the other Chord node that precedes the called Chord node.
	 */
	NodeInfo getPredecessor();

	/**
	 * Set the predecessor of a Chord node.
	 *
	 * @param predecessor the Chord node to set as predecessor.
	 */
	void setPredecessor(NodeInfo predecessor);

	/**
	 * Update the finger table with a Chord node at a given index.
	 *
	 * @param node  the Chord node to update the finger table with.
	 * @param index the index of the finger table to update.
	 */
	void updateFingerTable(NodeInfo node, int index);

	/**
	 * Find the closest (to the identifier) Chord node in the finger table that precedes an identifier.
	 *
	 * @param id the identifier.
	 *
	 * @return the closest finger that precedes the identifier.
	 */
	NodeInfo closestPrecedingFinger(BigInteger id);

	void notify(NodeInfo potentialPredecessor);
}
