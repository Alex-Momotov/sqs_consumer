package alex;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.codahale.metrics.health.HealthCheck;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class SqsConsumerPool extends HealthCheck {

    private final Supplier<AmazonSQS> sqsSupplier;
    private final Supplier<RecordHandler> handlerSupplier;
    private final String queueUrl;
    private final List<SqsConsumer> consumerList;

    public SqsConsumerPool(String awsRegion, String awsProfile, Supplier<RecordHandler> handlerSupplier, String queueUrl, int numConsumers) {
        this.sqsSupplier = () -> AmazonSQSClientBuilder.standard().withRegion(awsRegion).withCredentials(new ProfileCredentialsProvider(awsProfile)).build();
        this.handlerSupplier = handlerSupplier;
        this.queueUrl = queueUrl;
        this.consumerList = initialiseConsumers(numConsumers);
    }

    private List<SqsConsumer> initialiseConsumers(int numConsumers) {
        List<SqsConsumer> consumers = new ArrayList<>(numConsumers);
        for (int i = 0; i < numConsumers; i++) {
            consumers.add(new SqsConsumer(sqsSupplier.get(), queueUrl, handlerSupplier.get()));
        }
        return consumers;
    }

    public void start() {
        for (SqsConsumer consumer : consumerList) {
            consumer.start();
        }
    }

    @Override
    protected Result check() throws Exception {
        int numPaused = 0;
        int numPolling = 0;
        int numProcessing = 0;
        int numTerminated = 0;

        for (SqsConsumer consumer : consumerList) {
            switch(consumer.getState()) {
                case PAUSED:
                    numPaused += 1;
                    break;
                case POLLING:
                    numPolling += 1;
                    break;
                case PROCESSING:
                    numProcessing += 1;
                    break;
                case TERMINATED_WITH_ERROR:
                    numTerminated += 1;
                    break;
            }
        }

        String message = "Consumers - PAUSED: " + numPaused +         ", POLLING: " + numPolling
                                 + ", PROCESSING: " + numProcessing + ", TERMINATED_WITH_ERROR: " + numTerminated;

        if (numPaused > 0 || numTerminated > 0)
            return Result.unhealthy(message);
        return Result.healthy(message);
    }
}
