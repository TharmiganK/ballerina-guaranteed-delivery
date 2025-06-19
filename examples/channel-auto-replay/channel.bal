import tharmigan/messaging.replayablechannel;
import tharmigan/messaging.storeprocessor;

final replayablechannel:Channel channel;

final storeprocessor:RabbitMqMessageStore failureStore = check new ("messages.bi.failure");
final storeprocessor:RabbitMqMessageStore dlstore = check new ("messages.bi.dlq");

function init() returns error? {
    channel = check new (
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
}
