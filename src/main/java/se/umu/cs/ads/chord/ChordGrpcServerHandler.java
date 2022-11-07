package se.umu.cs.ads.chord;

import java.math.BigInteger;

/**
 * Interface for methods to handle incoming requests for the Chord node.
 */
public interface ChordGrpcServerHandler {
	boolean healthCheck();

	NodeInfo findSuccessor(BigInteger identifier);

	NodeInfo getSuccessor();

	NodeInfo getPredecessor();

	void setPredecessor(NodeInfo predecessor);

	void updateFingerTable(NodeInfo node, int index);

	NodeInfo closestPrecedingFinger(BigInteger identifier);

	void notify(NodeInfo potentialPredecessor);
}
