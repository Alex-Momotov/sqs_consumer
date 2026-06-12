package alex;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.github.javafaker.Faker;

import java.util.UUID;

public class App {
    public static void main(String[] args) {
        int numMessages = Integer.valueOf(args[0]);
        Faker faker = new Faker();

        AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion("eu-west-1").withCredentials(new ProfileCredentialsProvider("vonage-api-data-main-dev")).build();
        String queueUrl = "https://sqs.eu-west-1.amazonaws.com/467461571816/reports_queue.fifo";

        for (int i = 0; i < numMessages; i++) {
            String content = randContent(faker);
            sendMessage(sqs, queueUrl, UUID.randomUUID().toString(), content);
            System.out.println(content);
        }
    }


    static void sendMessage(AmazonSQS sqs, String queueUrl, String messageGroupId, String messageBody) {
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageGroupId(messageGroupId)
                .withMessageBody(messageBody);
        sqs.sendMessage(send_msg_request);
    }

    static String randContent(Faker faker) {
        return faker.name().firstName() + " " + faker.number().numberBetween(0, 100);
    }

}
