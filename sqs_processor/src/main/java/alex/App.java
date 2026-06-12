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

import java.util.Random;
import java.util.function.Supplier;


public class App {
    public static void main(String[] args) throws Exception {
        Supplier<RecordHandler> handlerSupplier = () -> new RecordHandler() {
            @Override
            public void handle(Message message) {
//                System.out.println("Started processing: " + message.getBody());
                try {
                    Thread.sleep(5_000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
//                System.out.println("Finished processing: "+ message.getBody());
            }
        };

        SqsConsumerPool pool = new SqsConsumerPool("eu-west-1", "vonage-api-data-main-dev", handlerSupplier, "https://sqs.eu-west-1.amazonaws.com/467461571816/reports_queue.fifo", 6);
        pool.start();

        while (true) {
            System.out.println(pool.check().getMessage());
            Thread.sleep(500);
        }

//        AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion("eu-west-1").withCredentials(new ProfileCredentialsProvider("vonage-api-data-main-dev")).build();
//
//        while (true) {
//            Message message = receiveMessage(sqs, queueUrl);
//            Thread t = new Thread(new JobProcessor(message.getBody()));
//            t.start();
//            while (t.isAlive()) {
//                int newVisibility = 8;
//                changeVisibility(sqs, queueUrl, message.getReceiptHandle(), newVisibility);
//                Thread.sleep(2_000);
//            }
//            sqs.deleteMessage(queueUrl, message.getReceiptHandle());
//            System.out.println("Message " + message.getBody() + " deleted!");
//        }

    }

//    static int changeVisibility(AmazonSQS sqs, String queueUrl, String messageHandle, int visibilitySec) {
//        ChangeMessageVisibilityResult result = sqs.changeMessageVisibility(queueUrl, messageHandle, visibilitySec);
//        System.out.println("Updated visibility by " + visibilitySec + " sec. Success: " + result.getSdkHttpMetadata().getHttpStatusCode());
//        return result.getSdkHttpMetadata().getHttpStatusCode();
//    }
//
//    static Message receiveMessage(AmazonSQS sqs, String queueUrl) {
//        ReceiveMessageRequest receive_request = new ReceiveMessageRequest()
//                .withMaxNumberOfMessages(1)
//                .withQueueUrl(queueUrl)
//                .withWaitTimeSeconds(20);
//
//        Message result = sqs.receiveMessage(receive_request).getMessages().get(0);
//        System.out.println("Message received: " + result.getBody());
//        return result;
//    }
//
    static int randInt() {
        return (new Random().nextInt(5) + 5) * 1000;
    }
//
//
//    public static class JobProcessor implements Runnable {
//
//        private String messageBody;
//
//        public JobProcessor(String messageBody) {
//            this.messageBody = messageBody;
//        }
//
//        @Override
//        public void run() {
//            System.out.println("Started processing: " + messageBody);
//            try {
//                Thread.sleep(randInt());
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//            System.out.println("Finished processing: "+ messageBody);
//        }
//    }
}
