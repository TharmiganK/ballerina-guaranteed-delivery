import ballerina/log;

import tharmigan/messaging.replayablechannel;
import tharmigan/messaging.storeprocessor;

listener storeprocessor:Listener dlsListener = check new (
    messageStore = dlstore,
    maxRetries = 3,
    pollingInterval = 20
);

service on dlsListener {

    public isolated function onMessage(anydata message) returns error? {
        replayablechannel:Message replayableMessage = check message.toJson().fromJsonWithType();
        do {
            _ = check channel.replay(replayableMessage);
            log:printInfo("message replayed successfully", id = replayableMessage.id);
        } on fail error err {
            log:printError("failed to replay message", 'error = err);
            return err;
        }
    }
}
