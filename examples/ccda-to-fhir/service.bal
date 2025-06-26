import ballerina/http;
import ballerina/log;

import tharmigan/channel;

listener http:Listener httpListener = new (9090);

service / on httpListener {

    function init() returns error? {
        log:printInfo("health data consumer service started");
    }

    resource function post messages(CCDAData[] messages) returns json|error? {
        json[] responses = [];
        foreach var message in messages {
            channel:ExecutionResult|channel:ExecutionError result = msgChannel->execute(message);
            if result is channel:ExecutionError {
                log:printError("error processing message", 'error = result);
                continue;
            }
            if !result.destinationResults.hasKey("HttpEndpoint") {
                log:printWarn("HttpEndpoint destination not found in the result");
                continue;
            }
            json createdResource = check result.destinationResults["HttpEndpoint"].ensureType();
            responses.push(createdResource);
        }
        if responses.length() == 0 {
            return error("Failed  to process any messages");
        }
        log:printInfo("Processed messages successfully", 'responses = responses);
        return {responses: responses};
    }
}
