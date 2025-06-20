import tharmigan/messaging.storeprocessor;
import tharmigan/messaging.replayablechannel;

configurable "in-memory"|"rabbitmq"|"directory" storeType = "directory";

final storeprocessor:MessageStore failureStore = check getMessageStore(storeType, "failure");
final storeprocessor:MessageStore replayStore = check getMessageStore(storeType, "replay");
final storeprocessor:MessageStore deadLetterStore = check getMessageStore(storeType, "dls");

function getMessageStore("in-memory"|"rabbitmq"|"directory" storeType, "failure"|"dls"|"replay" storeName) returns storeprocessor:MessageStore|error {
    if storeType == "rabbitmq" {
        string queueName = storeName == "failure" ? "messages.bi.failure" : storeName == "dls" ? "messages.bi.dlq" : "messages.bi.replay";
        return new storeprocessor:RabbitMqMessageStore(queueName);
    } else if storeType == "in-memory" {
        return new storeprocessor:InMemoryMessageStore();
    } else {
        string dirName = storeName == "failure" ? "failure" : storeName == "dls" ? "dls" : "replay";
        return new storeprocessor:LocalDirectoryMessageStore(dirName);
    }
}

final replayablechannel:Channel msgChannel = check new (
    name = "ccda-to-fhir-channel",
    sourceFlow = transformToCCDAData,
    destinationsFlow = sendToHttpEp,
    failureStore = failureStore,
    replayListenerConfig = {
        replayStore,
        deadLetterStore,
        maxRetries: 3,
        pollingInterval: 10
    }
);
