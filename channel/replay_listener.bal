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
        msgstore:ListenerConfiguration replayListenerConfig = {
            pollingInterval: listenerConfig.pollingInterval,
            dropMessageAfterMaxRetries: listenerConfig.dropMessageAfterMaxRetries,
            deadLetterStore: listenerConfig.deadLetterStore,
            maxRetries: 0 // Retry logic is handled in the service where we use the updated message to replay
        };
        msgstore:Listener replayListener = check new (targetStore, replayListenerConfig);
        ReplayService replayService = new (channel, maxRetries = listenerConfig.maxRetries, retryInterval = listenerConfig.retryInterval);
        check replayListener.attach(replayService);
        check replayListener.'start();
        runtime:registerListener(replayListener);
        log:printInfo("replay listener started successfully", channel = channel.getName());
    } on fail error err {
        log:printError("failed to start replay listener", 'error = err);
        return error Error("Failed to start replay listener", err);
    }
}

type ServiceReplayConfiguration record {|
    int maxRetries = 3;
    decimal retryInterval = 1;
|};

isolated service class ReplayService {
    *msgstore:Service;

    private final Channel channel;
    private final readonly & ServiceReplayConfiguration config;

    isolated function init(Channel channel, *ServiceReplayConfiguration config) {
        self.channel = channel;
        self.config = config.cloneReadOnly();
    }

    isolated remote function onMessage(anydata message) returns error? {
        Message|error replayableMessage = message.toJson().fromJsonWithType();
        if replayableMessage is error {
            log:printError("error converting message to replayable type", 'error = replayableMessage);
            return replayableMessage;
        }

        ExecutionResult|ExecutionError executionResult = self.channel->replay(replayableMessage);
        if executionResult is ExecutionResult {
            log:printDebug("message replayed successfully", id = replayableMessage.id);
            return;
        }

        Message newReplayableMessage = executionResult.detail().message;
        foreach int attempt in 1 ... self.config.maxRetries {
            ExecutionResult|ExecutionError executionResultOnRetry = self.channel->replay(newReplayableMessage);
            if executionResultOnRetry is ExecutionResult {
                log:printDebug("message replayed successfully", id = newReplayableMessage.id);
                return;
            }
            newReplayableMessage = executionResultOnRetry.detail().message;
            runtime:sleep(self.config.retryInterval);
        }

        log:printError("failed to replay message after retries", id = newReplayableMessage.id,
                'error = executionResult);
        return error("Failed to replay message after retries", executionResult);
    }
}
