## Overview

Ballerina developers often face challenges when integrating with diverse message brokers or database clients for message persistence and consumption due to their distinct APIs. This package addresses this by offering a unified API for storing and consuming messages, abstracting away the specifics of the underlying message store technology. This fosters consistency in development, allows for flexible switching or simultaneous use of different message stores, and enables the consistent implementation of critical messaging patterns like retry mechanisms and dead-letter queues (DLQs).

## Message Store Interface

The `MessageStore` interface defines the fundamental contract for message persistence and retrieval. Implementations of this interface allow Ballerina applications to interact with different message storage systems in a uniform manner.

```ballerina
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
    # + return - The retrieved message, or () if the store is empty, or an error if an error occurs
    isolated remote function retrieve() returns Message|error?;

    # Acknowledges the top message retrieved from the message store.
    #
    # + id - The unique identifier of the message to acknowledge. This should be the same as the `id`
    # of the message retrieved from the store.
    # + success - Indicates whether the message was processed successfully or not
    # + return - An error if the acknowledgment could not be processed, or `()`
    isolated remote function acknowledge(string id, boolean success = true) returns error?;
};
```

## Store Listener

The Store Listener is responsible for orchestrating message consumption from any `MessageStore` implementation. It operates by polling the associated message store at configurable intervals and dispatching messages to an attached service.

To initialize a listener, provide an instance of a `MessageStore`:

```ballerina
msgstore:MessageStore msgStore = new msgstore:InMemoryMessageStore(); // Example using an in-memory store

listener msgstore:Listener msgStoreListener = new(msgStore);
```

The listener's behavior, including polling frequency, retry mechanisms, and dead-letter queue (DLQ) support, can be customized using the `ListenerConfiguration` record:

```ballerina
# Represents the message store listener configuration,
#
# + pollingInterval - The interval in seconds at which the listener polls for new messages. This should be a positive decimal value
# + maxRetries - The maximum number of retries for processing a message. This should be a positive integer value
# + retryInterval - The interval in seconds between retries for processing a message. This should be a positive decimal value
# + dropMessageAfterMaxRetries - If true, the message will be dropped after the maximum number of retries is reached
# + deadLetterStore - An optional message store to store messages that could not be processed after the maximum number of retries.
# When set, `dropMessageAfterMaxRetries` option will be ignored
public type ListenerConfiguration record {|
    decimal pollingInterval = 1;
    int maxRetries = 3;
    decimal retryInterval = 1;
    boolean dropMessageAfterMaxRetries = false;
    MessageStore deadLetterStore?;
|};
```

## Message Store Service

A message store service, defined by the `msgstore:Service` type, can be attached to a `msgstore:Listener` to process messages retrieved from the message store. This service exposes a single remote method, `onMessage`, which is invoked when a new message is received.

```ballerina
# This service object defines the contract for processing messages from a message store.
public type Service distinct isolated service object {

    # This remote function is called when a new message is received from the message store.
    #
    # + content - The message content to be processed
    # + return - An error if the message could not be processed, or a nil value
    isolated remote function onMessage(anydata content) returns error?;
};
```

If the `onMessage` function returns an `error`, the message processing will be retried based on the configured `maxRetries` and `retryInterval`. If the maximum retries are exhausted and a `deadLetterStore` is configured, the message will be moved to the dead-letter store.

### Example Usage

The following example demonstrates how to utilize this package to set up an in-memory message store and a listener to process messages:

```ballerina
import ballerina/io;
import ballerina/http;

import tharmigan/msgstore;

// Initialize an in-memory message store
msgstore:MessageStore msgStore = new msgstore:InMemoryMessageStore();

// Initialize a message store listener with custom configuration
listener msgstore:Listener msgStoreListener = new(msgStore, {
    pollingInterval: 10,  // Poll every 10 seconds
    maxRetries: 2,        // Retry message processing up to 2 times
    retryInterval: 2      // Wait 2 seconds between retries
});

// Define and attach a service to the listener to handle incoming messages
service on msgStoreListener {

    isolated remote function onMessage(anydata content) returns error? {
        io:println("Received message: ", content);

        // Simulate a processing failure for specific message content
        if content is string && content == "fail" {
            return error("Message processing failed due to 'fail' content");
        }
        // If no error is returned, the message is acknowledged as successfully processed
    }
}

service /api/v1 on new http:Listener(8080) {

    // Endpoint to send messages to the message store
    resource function post messages(@http:Payload anydata content) returns http:Accepted|error {
        check msgStore.store(content);
        return http:ACCEPTED;
    }
}
```
