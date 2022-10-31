package se.umu.cs.ads.chord;

import io.grpc.stub.StreamObserver;
import se.umu.cs.ads.chord.ChordServiceGrpc.ChordServiceImplBase;

public class ChordServiceImpl extends ChordServiceImplBase {
	@Override
	public void hello(StringMessage request, StreamObserver<StringMessage> responseObserver) {
		System.out.println("Got request with content: " + request.getContent());

		String greeting = "Hello, " + request.getContent();
		StringMessage response = StringMessage.newBuilder().setContent(greeting).build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}
}