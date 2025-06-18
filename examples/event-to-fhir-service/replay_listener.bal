import tharmigan/messaging.storeprocessor;
import tharmigan/messaging.replayablechannel;
import ballerina/log;

listener storeprocessor:Listener msgStoreListener = new (
    messageStore = replayStore,
    maxRetries = 3,
    pollingInterval = 10,
    deadLetterStore = deadLetterStore
);

service on msgStoreListener {

    public isolated function onMessage(anydata payload) returns error? {
        replayablechannel:Message message = check payload.toJson().fromJsonWithType();
        do {
            _ = check msgChannel.replay(message);
            log:printInfo("message replayed successfully", id = message.id);
        } on fail error err {
            log:printError("failed to replay message", 'error = err);
            return err;
        }
    }
}