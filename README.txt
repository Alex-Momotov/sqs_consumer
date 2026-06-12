# _____________________________________________________________________________________________
# AWS SQS
- Works but will limit length of longest report to 12 hours
- Easy to use and implement
- Heartbeat pattern of extending visibility window as the consumer is processing it 
https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-sqs-message-queues.html 
https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/working-with-messages.html

Deduplication
- automatic deduplication on message content (dedup ID is optional when sending message)
- or on dedup ID (dedup ID is mandatory)

Message Group ID
- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html
- Message Group ID must be different (or random), otherwise (if its same) only one consumer will be able to work against the queue

