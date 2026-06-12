package alex;

import com.amazonaws.services.sqs.model.Message;

@FunctionalInterface
public interface RecordHandler {

    void handle(Message message);

}
