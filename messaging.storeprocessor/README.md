## Overview

This package provides abstractions for a message store and a listener which can be attached to a message store to process messages. The listener uses a poll mechanism to retrieve messages from the store and process them using th attached processor service.

## Components

### Message Store

The message store is an interface that defines the operations for storing and retrieving messages. It provides methods to store messages, retrieve top message, and acknowledge message on successfule or failure processing. The message store can be implemented using various storage mechanisms, such as in-memory, file-based, queue-based, or database-based storage.

This package provides the following message store implementations:

- **InMemoryMessageStore**: A simple in-memory message store that stores messages in a list. This is useful for testing and development purposes.
- **LocalDirectoryMessageStore**: A message store that stores messages in a local directory. This is useful for persisting messages on disk and can be used in production environments.
- **RabbitMQMessageStore**: A message store that uses RabbitMQ as the underlying storage mechanism. This is useful for distributed systems and provides reliable message delivery.

### Service

The service defines the `onMessage` method that processes messages retrieved from the message store. The service can be implemented to perform various operations on the messages, such as transforming, filtering, or routing them to different destinations.

### Listener

The listener can be created by providing a message store and a service can be attached to it. The listener uses a poll mechanism to retrieve messages from the store and process them using the attached service. The listener can be configured with various parameters, such as the polling interval, retry cound and a dead letter store for failed messages.

### Dead Letter Store

The dead letter store is an optional component that can be used to store messages that have failed processing after a certain number of retries. This allows for the retention of failed messages for later analysis or reprocessing. The dead letter store is also a message store and can be implemented using the same storage mechanisms as the main message store.
