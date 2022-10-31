package se.umu.cs.ads.chord;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class GrpcClient {
	public static void main(String[] args) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext().build();
		ChordServiceGrpc.ChordServiceBlockingStub stub = ChordServiceGrpc.newBlockingStub(channel);

		StringMessage request = StringMessage.newBuilder().setContent("Firstname Lastname").build();
		StringMessage helloResponse = stub.hello(request);
		System.out.println("Server said: " + helloResponse.getContent());

		channel.shutdown();
	}
}
