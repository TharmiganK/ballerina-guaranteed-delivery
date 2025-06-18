import ballerina/log;

import tharmigan/messaging.replayablechannel;
import tharmigan/messaging.storeprocessor;

isolated map<Channel> channels = {};

# Represent the configuration for a channel.
#
# + failureStore - The message store that acts as the failure store for the channel
# + replayConfig - The configuration for the replay listener that listens to the message store and
# replays messages.
public type ChannelConfiguration record {|
    *replayablechannel:MessageFlow;
    storeprocessor:MessageStore failureStore;
    ReplayListenerConfiguration replayConfig?;
|};

# Represents the configuration for the replay listener that listens to the message store and replays 
# messages.
#
# + replayStore - The message store which is listened by the listener. If not provided, the listener
# will be listened to the failure store defined in the channel configuration
public type ReplayListenerConfiguration record {|
    storeprocessor:MessageStore replayStore?;
    *storeprocessor:ListenerConfiguration;
|};

# A Channel which provides Mirth-like message processing capabilities.
public isolated class Channel {
    private final replayablechannel:Channel channel;
    private final storeprocessor:MessageStore failureStore;
    private final storeprocessor:MessageStore? replayStore;
    private final string name;

    # Initializes a new instance of Channel with the provided processors and destination.
    #
    # + name - The unique name to identify the channel.
    # + config - The configuration for the channel, which includes source flow, destinations flow, 
    # and dead letter store.
    # + return - An error if the channel could not be initialized, otherwise returns `()`.
    public isolated function init(string name, *ChannelConfiguration config) returns error? {
        lock {
            if channels.hasKey(name) {
                return error("Channel with name '" + name + "' already exists");
            }
        }
        self.name = name;
        self.channel = check new (
            sourceFlow = config.sourceFlow,
            destinationsFlow = config.destinationsFlow
        );
        self.failureStore = config.failureStore;
        ReplayListenerConfiguration? replayConfig = config.replayConfig;
        self.replayStore = replayConfig?.replayStore ?: self.failureStore;
        lock {
            channels[name] = self;
        }
        if replayConfig is () {
            log:printDebug("replay listener is not configured, messages that fail to process will not be replayed");
            return;
        }
        check startReplayListener(self, replayConfig);
    }

    # Replay the channel execution flow.
    #
    # + message - The message to replay process.
    # + return - Returns an error if the message could not be processed, otherwise returns the execution result.
    public isolated function replay(replayablechannel:Message message) returns replayablechannel:ExecutionResult|replayablechannel:ExecutionError {
        return self.channel.replay(message);
    }

    # Dispatch a message to the channel for processing with the defined processors and destinations.
    #
    # + content - The message content to be processed.
    # + skipDestinations - An array of destination names to skip during execution.
    # + return - Returns the execution result or an error if the processing failed.
    public isolated function execute(anydata content, string[] skipDestinations = []) returns replayablechannel:ExecutionResult|replayablechannel:ExecutionError {
        do {
            replayablechannel:ExecutionResult result = check self.channel.execute(content, skipDestinations);
            return result;
        } on fail replayablechannel:ExecutionError err {
            self.addToFailureStore(err.detail().message);
            return err;
        }
    }

    isolated function addToFailureStore(replayablechannel:Message message) {
        lock {
            string id = message.id;
            error? failureStoreError = self.failureStore.store(message.clone());
            if failureStoreError is error {
                log:printError("failed to add message to failure store", 'error = failureStoreError, msgId = id);
            } else {
                log:printDebug("message added to failure store", msgId = id);
            }
            return;
        }
    }

    isolated function getName() returns string {
        return self.name;
    }

    isolated function getFailureStore() returns storeprocessor:MessageStore {
        return self.failureStore;
    }

    isolated function getReplayStore() returns storeprocessor:MessageStore? {
        return self.replayStore;
    }
}
