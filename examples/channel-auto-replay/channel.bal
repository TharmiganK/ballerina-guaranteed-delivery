import tharmigan/messaging.replayablechannel;
import tharmigan/messaging.storeprocessor;

final storeprocessor:RabbitMqMessageStore failureStore = check new ("messages.bi.failure");
final storeprocessor:RabbitMqMessageStore dlstore = check new ("messages.bi.dlq");

final replayablechannel:Channel channel = check new (
    name = "replayable-channel",
    sourceFlow = processMessage,
    destinationsFlow = [
        writePayloadToFile,
        sendToHttpEp
    ],
    failureStore = failureStore,
    replayListenerConfig = {
        maxRetries: 3,
        pollingInterval: 10,
        deadLetterStore: dlstore
    }
);
