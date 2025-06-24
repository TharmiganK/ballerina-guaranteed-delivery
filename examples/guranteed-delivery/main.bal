import ballerina/http;
import ballerina/log;

import tharmigan/msgstore;

final msgstore:RabbitMqMessageStore messageStore = check new("messages.bi");
final msgstore:RabbitMqMessageStore deadLetterStore = check new("messages.bi.dlq");

final http:Client httpEndpoint = check new ("localhost:8080/api/v1");

public type Message record {|
    string id;
    string content;
|};

service /api on new http:Listener(9090) {

    isolated resource function post messages(Message message) returns http:Accepted|error {
        log:printInfo("message received at http service", id = message.id);
        check messageStore->store(message);
        return http:ACCEPTED;
    }
}

listener msgstore:Listener msgStoreListener = check new (
    messageStore = messageStore,
    maxRetries = 2,
    pollingInterval = 5,
    deadLetterStore = deadLetterStore
);

service on msgStoreListener {

    isolated remote function onMessage(anydata payload) returns error? {
        Message message = check payload.toJson().fromJsonWithType();
        log:printInfo("start processing message", id = message.id, content = message.content);
        if !re `^[a-zA-Z0-9_-]+$`.isFullMatch(message.id) {
            return error("id should not contain special characters", id = message.id);
        }
        anydata|error response = httpEndpoint->/messages.post(message);
        if response is error {
            log:printError("failed to process message", id = message.id, 'error = response);
            return response;
        }
        log:printInfo("message processed successfully", id = message.id, response = response);
    }
}
