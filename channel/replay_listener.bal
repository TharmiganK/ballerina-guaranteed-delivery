import ballerina/lang.runtime;
import ballerina/log;

import tharmigan/msgstore;

isolated function startReplayListener(Channel channel, ReplayListenerConfiguration config) returns Error? {
    string channelName = channel.getName();
    ReplayListenerConfiguration {replayStore: replayStore, ...listenerConfig} = config;
    msgstore:MessageStore? targetStore = replayStore ?: channel.getFailureStore();
    if targetStore is () {
        log:printWarn("no replay store is configured, skipping the replay listener setup",
                channel = channelName);
        return;
    }
    do {
        msgstore:Listener replayListener = check new (targetStore, listenerConfig);
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
    *msgstore:Service;

    private final Channel channel;

    isolated function init(Channel channel) {
        self.channel = channel;
    }

    isolated remote function onMessage(anydata message) returns error? {
        Message|error replayableMessage = message.toJson().fromJsonWithType();
        if replayableMessage is error {
            log:printError("error converting message to replayable type", 'error = replayableMessage);
            return replayableMessage;
        }

        ExecutionResult|error executionResult = self.channel.replay(replayableMessage);
        if executionResult is error {
            log:printError("error replaying message", 'error = executionResult);
            return executionResult;
        }
    }
}
