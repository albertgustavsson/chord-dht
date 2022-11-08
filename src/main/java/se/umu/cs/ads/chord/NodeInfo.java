package se.umu.cs.ads.chord;

import java.math.BigInteger;

class NodeInfo {
	final BigInteger id;
	final String address;

	public NodeInfo(BigInteger id, String address) {
		this.id = id;
		this.address = address;
	}

	/* TODO: this constructor can probably be removed, since all fields are final.
	    No need to copy an instance of NodeInfo.*/
	public NodeInfo(NodeInfo other) { // Copy constructor
		this(other.id, other.address);
	}

	@Override
	public String toString() {
		return "NodeInfo{identifier=0x" + id.toString(16) + ", address='" + address + "'}";
	}
}
