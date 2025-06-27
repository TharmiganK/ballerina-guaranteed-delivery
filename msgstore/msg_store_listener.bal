import ballerina/lang.runtime;
import ballerina/log;
import ballerina/task;

# Represents the message store listener configuration,
#
# + pollingInterval - The interval in seconds at which the listener polls for new messages
# + maxRetries - The maximum number of retries for processing a message
# + retryInterval - The interval in seconds between retries for processing a message
# + dropMessageAfterMaxRetries - If true, the message will be dropped after the maximum number of retries is reached
# + deadLetterStore - An optional message store to store messages that could not be processed after the maximum number of retries.
# When set, `dropMessageAfterMaxRetries` will be ignored
public type ListenerConfiguration record {|
    decimal pollingInterval = 1;
    int maxRetries = 3;
    decimal retryInterval = 1;
    boolean dropMessageAfterMaxRetries = false;
    MessageStore deadLetterStore?;
|};

# Represents a message store listener that polls messages from a message store and processes them.
public isolated class Listener {

    private MessageStore messageStore;
    private Service? messageStoreService = ();
    private task:JobId? pollJobId = ();
    private final ListenerConfiguration config;

    # Initializes a new instance of Message Store Listener.
    #
    # + messageStore - The message store to retrieve messages from
    # + config - The configuration for the message store listener
    # + return - An error if the listener could not be initialized, or `()`
    public isolated function init(MessageStore messageStore, *ListenerConfiguration config) returns error? {
        self.messageStore = messageStore;
        if config.maxRetries < 0 {
            return error("maxRetries cannot be negative");
        }
        if config.pollingInterval <= 0d {
            return error("pollingInterval must be greater than zero");
        }
        if config.retryInterval <= 0d {
            return error("retryInterval must be greater than zero");
        }
        ListenerConfiguration {deadLetterStore, ...otherConfig} = config;
        self.config = {
            ...otherConfig.clone(),
            deadLetterStore
        };
    }

    # Attaches a message store service to the listener. Only one service can be attached to this listener.
    #
    # + msgStoreService - The message store service to attach
    # + path - The path is not relevant for this listener. Only allowing a nil value
    # + return - An error if the service could not be attached, or a nil value
    public isolated function attach(Service msgStoreService, () path = ()) returns error? {
        lock {
            if self.messageStoreService is Service {
                return error("messageStoreService is already attached. Only one service can be attached to the message store listener");
            }
            self.messageStoreService = msgStoreService;
        }
    }

    # Detaches the message store service from the listener.
    #
    # + msgStoreService - The message store service to detach
    # + return - An error if the service could not be detached, or a nil value
    public isolated function detach(Service msgStoreService) returns error? {
        lock {
            task:JobId? pollJobId = self.pollJobId;
            if pollJobId is task:JobId {
                error? stopResult = task:unscheduleJob(pollJobId);
                if stopResult is error {
                    return error("failed to detach the service", cause = stopResult);
                }
            }

            Service? currentService = self.messageStoreService;
            if currentService is () {
                return error("no messageStoreService is attached");
            }
            if currentService === msgStoreService {
                self.messageStoreService = ();
            } else {
                return error("the provided messageStoreService is not attached to the listener");
            }
        }
    }

    # Starts the message store listener to poll and process messages.
    #
    # + return - An error if the listener could not be started, or a nil value
    public isolated function 'start() returns error? {
        lock {
            Service? currentService = self.messageStoreService;
            if currentService is () || self.pollJobId !is () {
                return;
            }

            PollAndProcessMessages pollTask = new (self.messageStore, currentService, self.config);
            task:JobId|error pollJob = task:scheduleJobRecurByFrequency(pollTask, self.config.pollingInterval);
            if pollJob is error {
                return error("failed to start message store listener", cause = pollJob);
            }
        }
    }

    # Gracefully stops the message store listener by waiting for any ongoing processing to complete before stopping.
    # This is not implemented yet, and currently this will call immediateStop.
    #
    # + return - An error if the listener could not be stopped, or a nil value
    public isolated function gracefulStop() returns error? {
        return self.immediateStop();
    }

    # Immediately stops the message store listener without waiting for any ongoing processing to complete.
    #
    # + return - An error if the listener could not be stopped, or `()`.
    public isolated function immediateStop() returns error? {
        lock {
            task:JobId? pollJobId = self.pollJobId;
            if pollJobId is () {
                return;
            }

            error? stopResult = task:unscheduleJob(pollJobId);
            if stopResult is error {
                return error("failed to stop message store listener", cause = stopResult);
            }
            log:printInfo("message store listener stopped successfully");
        }
    }

}

isolated class PollAndProcessMessages {
    *task:Job;

    private final MessageStore messageStore;
    private final Service messageStoreService;
    private final readonly & record {*ListenerConfiguration; never deadLetterStore?;} config;
    private MessageStore? deadLetterStore = ();

    public isolated function init(MessageStore messageStore, Service messageStoreService,
            ListenerConfiguration config) {
        self.messageStore = messageStore;
        self.messageStoreService = messageStoreService;
        ListenerConfiguration {deadLetterStore, ...otherConfig} = config;
        self.deadLetterStore = deadLetterStore;
        self.config = otherConfig.cloneReadOnly();
    }

    public isolated function ackMessage(string id, boolean success = true) {
        error? result = self.messageStore->acknowledge(id, success);
        if result is error {
            log:printError("failed to acknowledge message", 'error = result);
        }
    }

    public isolated function execute() {
        Message|error? message = self.messageStore->retrieve();
        if message is error {
            log:printError("error polling messages", 'error = message);
            return;
        }
        if message is () {
            log:printDebug("found empty message, skipping processing");
            return;
        }

        anydata content = message.content;
        string id = message.id;

        error? result = trap self.messageStoreService->onMessage(content);
        if result is () {
            log:printDebug("message processed successfully", id = id);
            self.ackMessage(id);
            return;
        }
        log:printError("error processing message", 'error = result);

        if self.config.maxRetries <= 0 {
            log:printDebug("no retries configured", id = id);
        } else {
            foreach int attempt in 1 ... self.config.maxRetries {
                error? retryResult = self.messageStoreService->onMessage(content);
                if retryResult is error {
                    log:printError("error processing message on retry", retryAttempt = attempt, 'error = retryResult);
                } else {
                    log:printDebug("message processed successfully on retry", retryAttempt = attempt, id = id);
                    self.ackMessage(id);
                    return;
                }
                if attempt != self.config.maxRetries {
                    runtime:sleep(self.config.retryInterval);
                }
            }
        }
        MessageStore? dls;
        lock {
            dls = self.deadLetterStore;
        }
        if dls is MessageStore {
            error? dlsResult = dls->store(content.clone());
            if dlsResult is error {
                log:printError("failed to store message in dead letter store", 'error = dlsResult);
            } else {
                log:printDebug("message stored in dead letter store after max retries", payload = message);
                self.ackMessage(id);
                return;
            }
        }
        if self.config.dropMessageAfterMaxRetries {
            log:printDebug("max retries reached, dropping message", payload = message);
        } else {
            log:printError("max retries reached, message is kept in the store", payload = message);
        }
        self.ackMessage(id, self.config.dropMessageAfterMaxRetries);
    }
}

# This service object defines the contract for processing messages from a message store.
public type Service distinct isolated service object {

    # This function is called when a new message is received from the message store.
    #
    # + message - The message to be processed
    # + return - An error if the message could not be processed, or a nil value
    isolated remote function onMessage(anydata message) returns error?;
};
