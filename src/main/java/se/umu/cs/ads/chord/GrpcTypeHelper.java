package se.umu.cs.ads.chord;

import java.math.BigInteger;

import com.google.protobuf.ByteString;

public class GrpcTypeHelper {
	public static NodeInfo nodeInfoFromNode(Node node) {
		return new NodeInfo(bigIntegerFromIdentifier(node.getIdentifier()), node.getAddress());
	}

	public static Node nodeFromNodeInfo(NodeInfo nodeInfo) {
		return Node.newBuilder().setIdentifier(identifierFromBigInteger(nodeInfo.id)).setAddress(nodeInfo.address)
			.build();
	}

	public static BigInteger bigIntegerFromIdentifier(Identifier identifier) {
		return new BigInteger(1, identifier.getValue().toByteArray());
	}

	public static Identifier identifierFromBigInteger(BigInteger bigInteger) {
		return Identifier.newBuilder().setValue(ByteString.copyFrom(bigInteger.toByteArray())).build();
	}
}
