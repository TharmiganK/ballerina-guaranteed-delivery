import ballerina/file;
import ballerina/io;
import ballerina/log;
import ballerina/uuid;
import ballerinax/rabbitmq;

# Represents a message store interface for storing and retrieving messages.
public type MessageStore isolated object {

    # Stores a message in the message store.
    #
    # + message - The message to be stored
    # + return - An error if the message could not be stored, or `()`
    public isolated function store(anydata message) returns error?;

    # Retrieves the top message from the message store without removing it.
    #
    # + return - The retrieved message, or nil if the store is empty, or an error if an error occurs
    public isolated function retrieve() returns anydata|error;

    # Acknowledges the processing of a message.
    #
    # + success - Indicates whether the message was processed successfully
    # + return - An error if the acknowledgment could not be processed, or `()`
    public isolated function acknowledge(boolean success = true) returns error?;
};

# Represents an in-memory message store implementation.
public isolated class InMemoryMessageStore {
    *MessageStore;

    private anydata[] messages;
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
    public isolated function store(anydata message) returns error? {
        lock {
            self.messages.push(message.clone());
        }
    }

    # Retrieves the top message from the message store.
    #
    # + return - The retrieved message, or nil if the store is empty, or an error if an error occurs
    public isolated function retrieve() returns anydata|error {
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
            return message.clone();
        }
    }

    # Clears all messages from the message store.
    #
    # + return - An error if the store could not be cleared, or `()`.
    public isolated function clear() returns error? {
        lock {
            check trap self.messages.removeAll();
        }
    }

    # Deletes the top message from the message store.
    #
    # + return - An error if the message could not be deleted, or `()`
    isolated function delete() returns error? {
        lock {
            if self.messages.length() == 0 {
                return;
            }
            if self.mode == "FIFO" {
                _ = check trap self.messages.shift();
            } else {
                _ = check trap self.messages.pop();
            }
        }
    }

    # Acknowledges the processing of a message. When acknowledged with success, the message is 
    # removed from the store.
    #
    # + success - Indicates whether the message was processed successfully
    # + return - An error if the acknowledgment could not be processed, or `()`
    public isolated function acknowledge(boolean success = true) returns error? {
        if success {
            error? result = self.delete();
            if result is error {
                return error("Failed to acknowledge message", cause = result);
            }
        } else {
            log:printDebug("message not acknowledged, keeping in store");
        }
        return;
    }
}

# Represents a Local Directory message store implementation.
public isolated class LocalDirectoryMessageStore {
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
    public isolated function store(anydata message) returns error? {
        string uuid = uuid:createType1AsString();
        string filePath = self.directoryName + "/" + uuid + ".json";
        error? result = io:fileWriteJson(filePath, message.toJson());
        if result is error {
            return error("Failed to store message in local file system", cause = result);
        }
    }

    # Retrieves the top message from the local file system message store without removing it.
    #
    # + return - The retrieved message, or nil if the store is empty, or an error if an error occurs
    public isolated function retrieve() returns anydata|error {
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
            return fileContent;
        }
    }

    # Clears all messages from the local file system message store.
    #
    # + return - An error if the store could not be cleared, or `()`.
    public isolated function clear() returns error? {
        check file:remove(self.directoryName, option = file:RECURSIVE);
    }

    # Deletes the top message from the local file system message store.
    #
    # + return - An error if the message could not be deleted, or `()`
    isolated function delete() returns error? {
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
            // Delete the first message found
            error? result = file:remove(fileMetaData.absPath);
            if result is error {
                log:printError("failed to delete file", 'error = result, filePath = fileMetaData.absPath);
                return error("Failed to delete message from local file system", cause = result, filePath = fileMetaData.absPath);
            }
        }
    }

    # Acknowledges the processing of a message. When acknowledged with success, the message is
    # removed from the store.
    #
    # + success - Indicates whether the message was processed successfully
    # + return - An error if the acknowledgment could not be processed, or `()`
    public isolated function acknowledge(boolean success = true) returns error? {
        if success {
            error? result = self.delete();
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
public isolated class RabbitMqMessageStore {
    *MessageStore;

    private final rabbitmq:Client rabbitMqClient;
    private final readonly & RabbitMqPublishConfiguration publishConfig;
    private final string queueName;
    private rabbitmq:AnydataMessage? consumedMessage = ();
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
    }

    # Stores a message in the RabbitMQ message store.
    #
    # + message - The message to be stored
    # + return - An error if the message could not be stored, or `()`
    public isolated function store(anydata message) returns error? {
        error? result = self.rabbitMqClient->publishMessage({
            content: message,
            routingKey: self.queueName,
            ...self.publishConfig
        });

        if result is error {
            return error("Failed to store message in RabbitMQ", cause = result);
        }
    }

    # Retrieves the top message from the RabbitMQ message store without removing it.
    #
    # + return - The retrieved message, or nil if the store is empty, or an error if an error occurs
    public isolated function retrieve() returns anydata|error {
        lock {
            rabbitmq:AnydataMessage|error message = self.rabbitMqClient->consumeMessage(self.queueName, false);
            if message is error {
                return;
            }
            self.consumedMessage = message;
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
            return content.clone();
        }
    }

    # Clears all messages from the RabbitMQ message store.
    #
    # + return - An error if the store could not be cleared, or `()`.
    public isolated function clear() returns error? {
        error? result = self.rabbitMqClient->queuePurge(self.queueName);
        if result is error {
            return error("Failed to clear RabbitMQ queue", cause = result);
        }
    }

    # Acknowledges the processing of a message. When acknowledged with success, the message is
    # removed from the store.
    #
    # + success - Indicates whether the message was processed successfully
    # + return - An error if the acknowledgment could not be processed, or `()`
    public isolated function acknowledge(boolean success = true) returns error? {
        lock {
            rabbitmq:AnydataMessage? message = self.consumedMessage;
            if message is () {
                return;
            }
            error? result;
            if success {
                result = self.rabbitMqClient->basicAck(message);
            } else {
                result = self.rabbitMqClient->basicNack(message);
            }
            if result is error {
                return error("Failed to acknowledge message from RabbitMQ", cause = result);
            }
        }
    }
}
