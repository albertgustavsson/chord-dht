package se.umu.cs.ads.chord;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class GrpcServer {
	public static void main(String[] args) {
		int port = 8080;
		Server server = ServerBuilder.forPort(port).addService(new ChordServiceImpl()).build();

		try {
			server.start();
			System.out.println("Server is listening on port " + port);
			server.awaitTermination();
		} catch (Exception e) {
			throw new RuntimeException("Server failed", e);
		}
	}
}
