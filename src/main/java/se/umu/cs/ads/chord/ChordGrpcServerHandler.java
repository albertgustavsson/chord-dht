package se.umu.cs.ads.chord;

import java.math.BigInteger;
import java.util.Optional;

/**
 * Interface for methods to handle incoming requests for the Chord node.
 */
public interface ChordGrpcServerHandler {
	boolean healthCheck();

	NodeInfo findSuccessor(BigInteger identifier);

	NodeInfo getSuccessor();

	Optional<NodeInfo> getPredecessor();

	NodeInfo closestPrecedingFinger(BigInteger identifier);

	void notify(NodeInfo potentialPredecessor);
}
