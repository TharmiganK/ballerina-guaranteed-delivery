import tharmigan/channel;
import tharmigan/msgstore;

final msgstore:RabbitMqMessageStore failureStore = check new ("messages.bi.failure");
final msgstore:RabbitMqMessageStore replayStore = check new ("messages.bi.replay");
final msgstore:RabbitMqMessageStore dlstore = check new ("messages.bi.dlq");

final channel:Channel channel = check new (
    name = "replayable-channel",
    sourceFlow = processMessage,
    destinationsFlow = [
        writePayloadToFile,
        sendToHttpEp
    ],
    failureStore = failureStore,
    replayListenerConfig = {
        replayStore: replayStore,
        maxRetries: 2,
        retryInterval: 2,
        pollingInterval: 5,
        deadLetterStore: dlstore
    }
);
