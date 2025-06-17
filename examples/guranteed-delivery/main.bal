import ballerina/http;
import ballerina/log;

import tharmigan/messaging.storeprocessor;

final storeprocessor:RabbitMqMessageStore messageStore = check new("messages.bi");
final storeprocessor:RabbitMqMessageStore deadLetterStore = check new("messages.bi.dlq");

public type Message record {|
    string id;
    string content;
|};

service /api on new http:Listener(9090) {

    isolated resource function post messages(Message message) returns http:Accepted|error {
        log:printInfo("message received at http service", id = message.id);
        check messageStore.store(message);
        return http:ACCEPTED;
    }
}

listener storeprocessor:Listener msgStoreListener = check new (
    messageStore = messageStore,
    maxRetries = 3,
    pollingInterval = 10,
    deadLetterStore = deadLetterStore
);

service on msgStoreListener {

    public isolated function onMessage(anydata payload) returns error? {
        Message message = check payload.toJson().fromJsonWithType();
        log:printInfo("start processing message", id = message.id, content = message.content);
        if message.id.equalsIgnoreCaseAscii("1") {
            return error("Simulated processing failure for message with id 1");
        }
    }
}
