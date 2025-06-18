import ballerina/lang.runtime;
import ballerina/log;

import tharmigan/messaging.replayablechannel;
import tharmigan/messaging.storeprocessor;

# Generic error type
public type Error distinct error;

isolated function startReplayListener(Channel channel, ReplayListenerConfiguration config) returns Error? {
    storeprocessor:MessageStore targetStore = config.replayStore ?: channel.getFailureStore();
    do {
        storeprocessor:Listener replayListener = check new (
            messageStore = targetStore,
            deadLetterStore = config.deadLetterStore,
            maxRetries = config.maxRetries,
            pollingInterval = config.pollingInterval
        );
        ReplayService replayService = new (channel);
        check replayListener.attach(replayService);
        check replayListener.'start();
        runtime:registerListener(replayListener);
        log:printInfo("replay listener started successfully", channel = channel.getName());
    } on fail error err {
        log:printError("failed to start replay listener", 'error = err);
        return error Error("Failed to start replay listener", err);
    }
}

isolated service class ReplayService {
    *storeprocessor:Service;

    private final Channel channel;

    isolated function init(Channel channel) {
        self.channel = channel;
    }

    public isolated function onMessage(anydata message) returns error? {
        replayablechannel:Message|error replayableMessage = message.toJson().fromJsonWithType();
        if replayableMessage is error {
            log:printError("error converting message to replayable type", 'error = replayableMessage);
            return replayableMessage;
        }

        replayablechannel:ExecutionResult|error executionResult = self.channel.replay(replayableMessage);
        if executionResult is error {
            log:printError("error replaying message", 'error = executionResult);
            return executionResult;
        }
    }
}
