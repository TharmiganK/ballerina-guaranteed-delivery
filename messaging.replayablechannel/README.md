## Overview

This package provides a replay mechanism for messages in a messaging system. It allows for the retrieval and reprocessing of messages that have already been sent, which can be useful for debugging, testing, or recovering from errors.

## Components

### Message

The message is a wrapper around the actual content being sent in the messaging system. It includes metadata such as the message ID, and any other relevant information that helps identify and process the message.

### Message Context

The message context is an object that provides functions to retrieve and update message content and the associated properties and metadata. This context is used to manage the state of the message as it is processed.

### Handlers

#### Processors

The processors are functions that accepts the message context and perform operations on the message. They can modify the content, add or update or remove properties and metadata, or perform any other necessary processing. The processors are assumed to be idempotent, meaning that they can be applied multiple times without changing the result beyond the initial application. The processors are executed sequentially in the order they are defined.

#### Destinations

The destinations are the final targets where the processed messages will be sent. Each destination can have its own processing logic, and they can be configured to handle different types of messages or perform different actions based on the message content. The destionations does not have to be idempotent, meaning that they can change the state of the system or perform actions that are not repeatable. The destinations are executed in parallel, allowing for efficient processing of multiple messages at once.

### Channel

The channel is a collection of processors and destinations that are used to process messages. The channel is defined as two distinct flows:

1. **Source Flow**: This flow can only contain processors which are executed sequentially. It is used to prepare the message before it is sent to the destinations.
2. **Destinations Flow**: This flow can a single destination or multiple destinations, which are executed in parallel. It is used to send the processed message to the final targets. Additionally, a destination can be configured with processors that will be executed before the message is sent to the destination.

The channel provides two functions:

1. `execute`: This function takes a message content, create a message and message context and processes the message through the source flow, and then sends it to the destinations flow.

2. `replay`: This function takes the message which is already processed in the channel and attempts to reprocess it through the source flow and send it to the destinations flow. This is useful for debugging or recovering from errors, as it allows you to reprocess messages that have already been sent with the knowledge of their previous state where it effectively skips the successful destinations.
