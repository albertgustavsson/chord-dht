package se.umu.cs.ads.chord;

import java.math.BigInteger;

class NodeInfo {
	final BigInteger identifier;
	final String address;

	public NodeInfo(BigInteger identifier, String address) {
		this.identifier = identifier;
		this.address = address;
	}

	public NodeInfo(NodeInfo other) { // Copy constructor
		this(other.identifier, other.address);
	}

	@Override
	public String toString() {
		return "NodeInfo{identifier=0x" + identifier.toString(16) + ", address='" + address + "'}";
	}
}
