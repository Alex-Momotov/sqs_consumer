package alex;

import com.amazonaws.services.sqs.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessorTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorTask.class);

    private final RecordHandler handler;
    private final Message message;

    public ProcessorTask(RecordHandler handler, Message message) {
        this.handler = handler;
        this.message = message;
    }

    @Override
    public void run() {
        try {
            // TODO: try to reprocess 3 times and then fail? Look at reports
            handler.handle(message);
        } catch (Exception e) {
            logger.error("SqsConsumer quit unexpectedly with an error.", e);
        }
    }
}
