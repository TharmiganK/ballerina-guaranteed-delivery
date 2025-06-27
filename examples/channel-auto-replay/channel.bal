import ballerina/http;

import tharmigan/channel;
import tharmigan/msgstore;

final http:Client httpEndpoint = check new ("http://localhost:8080/api/v1");
final msgstore:RabbitMqMessageStore failureStore = check new ("messages.bi.failure");
final msgstore:RabbitMqMessageStore replayStore = check new ("messages.bi.replay");
final msgstore:RabbitMqMessageStore dlstore = check new ("messages.bi.dlq");

final channel:Channel channel = check new (
    name = "replayable-channel",
    sourceFlow = processMessage,
    destinationsFlow = [
        writePayloadToFile,
        sendToHttpEp
    ],
    failureConfig = {
        failureStore: failureStore,
        replayListenerConfig:  {
            replayStore: replayStore,
            maxRetries: 2,
            retryInterval: 2,
            pollingInterval: 5,
            deadLetterStore: dlstore
        }
    }
);
