package alex;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SqsProducer {

    private static final Logger logger = LoggerFactory.getLogger(SqsProducer.class);

    private final AmazonSQS sqs;
    private final String queueUrl;

    public SqsProducer(String awsRegion, String awsProfile, String queueUrl) {
        this.sqs = AmazonSQSClientBuilder.standard().withRegion(awsRegion).withCredentials(new ProfileCredentialsProvider(awsProfile)).build();
        this.queueUrl = queueUrl;
    }

    public void sendMessage(String messageBody) {
        // TODO: Add retry logic to keep trying to send message indefinitely if HTTP status is not 200.
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageGroupId(UUID.randomUUID().toString())
                .withMessageBody(messageBody);
        SendMessageResult result = sqs.sendMessage(send_msg_request);
        logger.info("SqsProducer: Successfully sent request: " + messageBody + " to SQS queue, HTTP response: " + result.getSdkHttpMetadata().getHttpStatusCode());
    }

}
