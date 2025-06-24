import tharmigan/msgstore;
import tharmigan/channel;

configurable "in-memory"|"rabbitmq"|"directory" storeType = "directory";

final msgstore:MessageStore failureStore = check getMessageStore(storeType, "failure");
final msgstore:MessageStore replayStore = check getMessageStore(storeType, "replay");
final msgstore:MessageStore deadLetterStore = check getMessageStore(storeType, "dls");

function getMessageStore("in-memory"|"rabbitmq"|"directory" storeType, "failure"|"dls"|"replay" storeName) returns msgstore:MessageStore|error {
    if storeType == "rabbitmq" {
        string queueName = storeName == "failure" ? "messages.bi.failure" : storeName == "dls" ? "messages.bi.dlq" : "messages.bi.replay";
        return new msgstore:RabbitMqMessageStore(queueName);
    } else if storeType == "in-memory" {
        return new msgstore:InMemoryMessageStore();
    } else {
        string dirName = storeName == "failure" ? "failure" : storeName == "dls" ? "dls" : "replay";
        return new msgstore:LocalDirectoryMessageStore(dirName);
    }
}

final channel:Channel msgChannel = check new (
    name = "ccda-to-fhir-channel",
    sourceFlow = transformToCCDAData,
    destinationsFlow = sendToHttpEp,
    failureStore = failureStore,
    replayListenerConfig = {
        replayStore,
        deadLetterStore,
        maxRetries: 3,
        retryInterval: 20,
        pollingInterval: 10
    }
);
