import ballerina/file;
import ballerina/io;
import ballerina/log;
import ballerina/uuid;
import ballerinax/rabbitmq;

# Represents the message content with a unique consumer ID.
#
# + id - The unique identifier for the consumer
# + content - The actual message content
public type Message record {|
    string id;
    anydata content;
|};

# Represents a message store interface for storing and retrieving messages.
public type MessageStore isolated client object {

    # Stores a message in the message store.
    #
    # + message - The message to be stored
    # + return - An error if the message could not be stored, or `()`
    isolated remote function store(anydata message) returns error?;

    # Retrieves the top message from the message store without removing it.
    #
    # + return - The retrieved message, or `()` if the store is empty, or an error if an error occurs
    isolated remote function retrieve() returns Message|error?;

    # Acknowledges the top message retrieved from the message store.
    #
    # + id - The unique identifier of the message to acknowledge. This should be the same as the `id`
    # of the message retrieved from the store.
    # + success - Indicates whether the message was processed successfully or not
    # + return - An error if the acknowledgment could not be processed, or `()`
    isolated remote function acknowledge(string id, boolean success = true) returns error?;
};

# Represents an in-memory message store implementation.
public isolated client class InMemoryMessageStore {
    *MessageStore;

    private anydata[] messages;
    private map<anydata> receivedMessages = {};
    private final "FIFO"|"LIFO" mode;

    # Initializes a new instance of the InMemoryStore class
    public isolated function init("FIFO"|"LIFO" mode = "FIFO") {
        self.messages = [];
        self.mode = mode;
    }

    # Stores a message in the message store.
    #
    # + message - The message to be stored
    # + return - An error if the message could not be stored, or `()`
    isolated remote function store(anydata message) returns error? {
        lock {
            self.messages.push(message.clone());
        }
    }

    # Retrieves the top message from the message store. Retrieving concurrently without acknowledgment
    # will result in the same message being returned until it is acknowledged.
    #
    # + return - The retrieved message, or `()` if the store is empty, or an error if an error occurs
    isolated remote function retrieve() returns Message|error? {
        lock {
            if self.messages.length() == 0 {
                return;
            }
            anydata message;
            if self.mode == "FIFO" {
                message = self.messages.first();
            } else {
                message = self.messages.last();
            }
            string id = uuid:createType1AsString();
            self.receivedMessages[id] = message.clone();
            return {id, content: message.clone()};
        }
    }

    # Acknowledges the processing of a message.
    #
    # + id - The unique identifier of the message to acknowledge
    # + success - Indicates whether the message was processed successfully
    # + return - An error if the acknowledgment could not be processed, or `()`
    isolated remote function acknowledge(string id, boolean success = true) returns error? {
        lock {
            if !self.receivedMessages.hasKey(id) {
                return error("Message with the given ID not found", id = id);
            }
            if success {
                anydata message = self.receivedMessages.get(id);
                int? index = self.mode == "FIFO" ? self.messages.indexOf(message) : self.messages.lastIndexOf(message);
                if index is () {
                    return error("Message with the given ID not found in the store", id = id);
                }
                _ = self.messages.remove(index);
            } else {
                log:printDebug("message not acknowledged, keeping in store");
            }
            return;
        }
    }
}

# Represents a Local Directory message store implementation.
public isolated client class LocalDirectoryMessageStore {
    *MessageStore;

    private final string directoryName;

    # Initializes a new instance of the LocalDirectoryStore class.
    #
    # + directoryName - The name of the directory where messages will be stored
    public isolated function init(string directoryName) {
        self.directoryName = directoryName;
    }

    # Stores a message in the local file system message store.
    #
    # + message - The message to be stored
    # + return - An error if the message could not be stored, or `()`
    isolated remote function store(anydata message) returns error? {
        string uuid = uuid:createType1AsString();
        string filePath = self.directoryName + "/" + uuid + ".json";
        error? result = io:fileWriteJson(filePath, message.toJson());
        if result is error {
            return error("Failed to store message in local file system", cause = result);
        }
    }

    # Retrieves the top message from the local file system message store without removing it. Retrieving
    # concurrently without acknowledgment will result in the same message being returned until it is acknowledged.
    #
    # + return - The retrieved message, or `()` if the store is empty, or an error if an error occurs
    isolated remote function retrieve() returns Message|error? {
        file:MetaData[]|error files = file:readDir(self.directoryName);
        if files is error {
            return error("Failed to read directory", cause = files, directory = self.directoryName);
        }

        foreach file:MetaData fileMetaData in files {
            if fileMetaData.dir == true || !fileMetaData.absPath.endsWith(".json") {
                continue; // Skip directories and non-JSON files
            }
            json|error fileContent = io:fileReadJson(fileMetaData.absPath);
            if fileContent is error {
                log:printError("failed to read file", 'error = fileContent, filePath = fileMetaData.absPath);
                continue; // Skip files that cannot be read
            }
            // Return the first message found
            return {id: fileMetaData.absPath, content: fileContent};
        }
        return;
    }

    # Acknowledges the processing of a message. When acknowledged with success, the message is
    # removed from the store.
    #
    # + id - The file path of the message to acknowledge
    # + success - Indicates whether the message was processed successfully
    # + return - An error if the acknowledgment could not be processed, or `()`
    isolated remote function acknowledge(string id, boolean success = true) returns error? {
        if success {
            error? result = file:remove(id);
            if result is error {
                return error("Failed to acknowledge message", cause = result);
            }
        } else {
            log:printDebug("message not acknowledged, keeping in store");
        }
        return;
    }
}

# Represents a RabbitMQ client configuration.
#
# + host - The RabbitMQ server host. Defaults to "localhost"
# + port - The RabbitMQ server port. Defaults to 5672
# + connectionData - The RabbitMQ connection configuration
# + publishConfig - The RabbitMQ publish configuration
public type RabbitMqClientConfiguration record {|
    string host = "localhost";
    int port = 5672;
    rabbitmq:ConnectionConfiguration connectionData = {};
    RabbitMqPublishConfiguration publishConfig = {};
|};

# Represents a RabbitMQ publish configuration.
#
# + exchange - The RabbitMQ exchange to publish messages to. Defaults to an empty string
# + deliveryTag - The delivery tag for the message. Optional
# + properties - The RabbitMQ message properties. Optional
public type RabbitMqPublishConfiguration record {|
    string exchange = "";
    int deliveryTag?;
    rabbitmq:BasicProperties properties?;
|};

# Represents a RabbitMQ message store implementation.
public isolated client class RabbitMqMessageStore {
    *MessageStore;

    private final rabbitmq:Client rabbitMqClient;
    private final readonly & RabbitMqPublishConfiguration publishConfig;
    private final string queueName;
    private map<rabbitmq:AnydataMessage> consumedMessages;
    private final readonly & RabbitMqClientConfiguration clientConfig;

    # Initializes a new instance of the RabbitMqStore class.
    #
    # + queueName - The name of the RabbitMQ queue to use for storing messages
    # + clientCofig - The RabbitMQ client configuration
    public isolated function init(string queueName, RabbitMqClientConfiguration clientCofig = {}) returns error? {
        self.clientConfig = clientCofig.cloneReadOnly();
        self.rabbitMqClient = check new (clientCofig.host, clientCofig.port, clientCofig.connectionData);
        self.publishConfig = clientCofig.publishConfig.cloneReadOnly();
        self.queueName = queueName;
        self.consumedMessages = {};
    }

    # Stores a message in the RabbitMQ message store.
    #
    # + message - The message to be stored
    # + return - An error if the message could not be stored, or `()`
    isolated remote function store(anydata message) returns error? {
        error? result = self.rabbitMqClient->publishMessage({
            content: message,
            routingKey: self.queueName,
            ...self.publishConfig
        });

        if result is error {
            return error("Failed to store message in RabbitMQ", cause = result);
        }
    }

    # Retrieves the top message from the RabbitMQ message store without removing it. Retrieving
    # concurrently without acknowledgment will return the next message in the queue if the previous one
    # is not acknowledged.
    #
    # + return - The retrieved message, or `()` if the store is empty, or an error if an error occurs
    isolated remote function retrieve() returns Message|error? {
        lock {
            rabbitmq:AnydataMessage|error message = self.rabbitMqClient->consumeMessage(self.queueName, false);
            if message is error {
                return error("Failed to retrieve message from RabbitMQ", cause = message);
            }

            anydata content = message.content;
            if content is byte[] {
                string|error fromBytes = string:fromBytes(content);
                if fromBytes is error {
                    log:printDebug("Failed to convert byte[] to string", 'error = fromBytes);
                } else {
                    json|error fromJsonString = fromBytes.fromJsonString();
                    if fromJsonString is json {
                        content = fromJsonString;
                    } else {
                        log:printDebug("Failed to convert string to JSON", 'error = fromJsonString);
                    }
                }
            }
            string id = uuid:createType1AsString();
            self.consumedMessages[id] = message.clone();
            return {id, content: content.clone()};
        }
    }

    # Acknowledges the processing of a message. When acknowledged with success, the message is
    # removed from the store.
    #
    # + id - The unique identifier of the message to acknowledge
    # + success - Indicates whether the message was processed successfully
    # + return - An error if the acknowledgment could not be processed, or `()`
    isolated remote function acknowledge(string id, boolean success = true) returns error? {
        lock {
            if !self.consumedMessages.hasKey(id) {
                return error("Message with the given ID not found", id = id);
            }
            rabbitmq:AnydataMessage message = self.consumedMessages.get(id);
            error? result;
            if success {
                result = self.rabbitMqClient->basicAck(message);
            } else {
                result = self.rabbitMqClient->basicNack(message);
            }
            _ = self.consumedMessages.remove(id);
            if result is error {
                return error("Failed to acknowledge message from RabbitMQ", cause = result);
            }
        }
    }
}
