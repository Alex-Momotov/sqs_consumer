package alex;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SqsConsumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SqsConsumer.class);

    private final AmazonSQS sqs;
    private final String queueUrl;
    private final RecordHandler handler;
    private State state;

    private static final int VISIBILITY_WINDOW_SEC = 8;
    private static final int VISIBILITY_UPDATE_INTERVAL_SEC = 4;

    public SqsConsumer(AmazonSQS sqs, String queueUrl,  RecordHandler handler) {
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.handler = handler;
        this.state = State.PAUSED;
    }

    public void start() {
        new Thread(this).start();
    }

    @Override
    public void run() {
        try {
            pollingLoop();
        } catch (Exception e) {
            logger.error("SqsConsumer quit unexpectedly with an error.", e);
            state = State.TERMINATED_WITH_ERROR;
        }
    }

    private void pollingLoop() {
        state = State.POLLING;
        while (state != State.PAUSED) {
            List<Message> messages = receiveMessage();
            if (messages.isEmpty())
                continue;
            if (messages.size() > 1)
                throw new RuntimeException("Something went wrong, SqsConsumer polled more than one message - " + messages.size() + " messages.");
            Message msg = messages.get(0);

            Thread t = new Thread(new ProcessorTask(handler, msg));
            t.start();
            state = State.PROCESSING;

            while (t.isAlive()) {
                changeVisibility(msg.getReceiptHandle());
                tryJoin(t, VISIBILITY_UPDATE_INTERVAL_SEC);
            }

            deleteMessage(msg);
        }
    }

    private List<Message> receiveMessage() {
        // TODO: if response is not 200, keep trying to poll for messages indefinitely, but log as error message
        state = State.POLLING;
        ReceiveMessageRequest receive_request = new ReceiveMessageRequest()
                .withMaxNumberOfMessages(1)
                .withQueueUrl(queueUrl)
                .withWaitTimeSeconds(20);

        return sqs.receiveMessage(receive_request).getMessages();
    }

    private void changeVisibility(String messageHandle) {
        ChangeMessageVisibilityResult result = sqs.changeMessageVisibility(queueUrl, messageHandle, VISIBILITY_WINDOW_SEC);
        logger.debug("Updated visibility by " + VISIBILITY_WINDOW_SEC + " sec, HTTP response: " + result.getSdkHttpMetadata().getHttpStatusCode());
        // TODO: if this request fails (status not 200) keep trying to extend visibility for VISIBILITY_WINDOW_SEC seconds. Also log the status code
    }

    private void deleteMessage(Message msg) {
        // TODO: wrap it around retry logic if status is not 200. Keep trying to delete message forever.
        DeleteMessageResult result = sqs.deleteMessage(queueUrl, msg.getReceiptHandle());
        logger.debug("Message " + msg.getBody() + " deleted, HTTP response: " + result.getSdkHttpMetadata().getHttpStatusCode());
    }

    public void pause() {
        state = State.PAUSED;
    }

    public State getState() {
        return state;
    }

    private static void tryJoin(Thread t, int sleepSec) {
        try {
            t.join(sleepSec * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public enum State {
        PAUSED("PAUSED"),
        POLLING("POLLING"),
        PROCESSING("PROCESSING"),
        TERMINATED_WITH_ERROR("TERMINATED_WITH_ERROR");

        private final String state;

        State(String state) {
            this.state = state;
        }

        @Override
        public String toString() {
            return state;
        }
    }

}
