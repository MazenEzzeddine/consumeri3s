import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ArrivalServiceImpl extends ArrivalServiceGrpc.ArrivalServiceImplBase {
    private static final Logger log = LogManager.getLogger(ArrivalServiceImpl.class);
    @Override
    public void consumptionRatee(RateRequest request, StreamObserver<RateResponse> responseObserver) {
        log.info("received new rate request {}", request.getRaterequest());
        RateResponse rate = RateResponse.newBuilder()
                .setRate(Consumer.latency)
                        .build();

        responseObserver.onNext(rate);
        responseObserver.onCompleted();

    }
}
