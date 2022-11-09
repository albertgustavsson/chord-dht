package se.umu.cs.ads.chord;

import java.math.BigInteger;

class NodeInfo {
	final BigInteger id;
	final String address;

	public NodeInfo(BigInteger id, String address) {
		this.id = id;
		this.address = address;
	}

	@Override
	public String toString() {
		return "NodeInfo{identifier=0x" + id.toString(16) + ", address='" + address + "'}";
	}
}
