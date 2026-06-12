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

import java.util.List;
import java.util.UUID;

public class App {
    public static void main(String[] args) throws InterruptedException {
        AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion("eu-west-1").withCredentials(new ProfileCredentialsProvider("vonage-api-data-main-dev")).build();
        String queueUrl = "https://sqs.eu-west-1.amazonaws.com/467461571816/reports_queue.fifo";


//        Message message = receiveMessage(sqs, queueUrl);
//        System.out.println(message.getBody());
//        System.out.println(message.getMessageId());
//        System.out.println(message.getReceiptHandle());

//        String foreverMessageHandle = "AQEBhQF9cv4tdNb1IaqOHPfKbaz5BAKc673knMpgokhCD/rmaxWsj8Tk18mIZrVc8c0PWHlej9tQ8rRCPN7m1P2jYTDXVR++rTUP4iBjI+C8E6oL+HMSCxHnMqyIATdO6zLwYTsLm3Wdb+AY74QGnGy68sbMY+GbkmcZ6zZ9y2kC74NTCTyFNRdVxjKiuxY/r25jAOfyDViJYwfAdTDExE+f71yNgOiwh7lHejqHpc8a3dFAjNZo1cjx3rRAAk12egwsTzkzQ5T4GR96OlVt+NjvqP18E1LQ4fs5bw6Tqa1aKIg=";
//
//        for (int i = 0; i < 10; i++) {
//            ChangeMessageVisibilityResult result = sqs.changeMessageVisibility(queueUrl, foreverMessageHandle, 43200);
//            System.out.println(result.getSdkHttpMetadata().getHttpStatusCode());
//            Thread.sleep(2000);
//            System.out.println("success");
//        }


//        sqs.deleteMessage(queueUrl, foreverMessageHandle);


        sendMessage(sqs, queueUrl, "reports_group_id", "forever_new");
    }

    static Message receiveMessage(AmazonSQS sqs, String queueUrl) {
        ReceiveMessageRequest receive_request = new ReceiveMessageRequest()
                .withMaxNumberOfMessages(1)
                .withQueueUrl(queueUrl)
                .withWaitTimeSeconds(20);

        return sqs.receiveMessage(receive_request).getMessages().get(0);
    }

    static void sendMessage(AmazonSQS sqs, String queueUrl, String messageGroupId, String messageBody) {
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageGroupId(messageGroupId)
                .withMessageBody(messageBody);
        sqs.sendMessage(send_msg_request);
    }


    static void listQueues(AmazonSQS sqs) {
        ListQueuesResult lq_result = sqs.listQueues();
        System.out.println("Your SQS Queue URLs:");
        for (String url : lq_result.getQueueUrls()) {
            System.out.println(url);
        }
    }
}
