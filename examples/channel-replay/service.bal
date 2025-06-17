import ballerina/http;
import ballerina/log;

import tharmigan/messaging.replayablechannel;
import tharmigan/messaging.storeprocessor;

configurable int port = 9090;

final storeprocessor:RabbitMqMessageStore dlstore = check new ("messages.bi.dlq");

service /api on new http:Listener(port) {

    isolated resource function post messages(Message message) returns http:Ok|error {
        do {
            _ = check channel.execute(message);
            return http:OK;
        } on fail replayablechannel:ExecutionError err {
            log:printError("failed to process message", 'error = err);
            error? result = dlstore.store(err.detail().message);
            if result is error {
                log:printError("failed to store message in dead letter store", 'error = result);
            } else {
                log:printInfo("message stored in dead letter store");
            }
            return err;
        }
    }
}
