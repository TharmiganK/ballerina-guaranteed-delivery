import ballerina/http;
import ballerina/log;

import tharmigan/messaging.replayablechannel;

listener http:Listener httpListener = new (9090);

service / on httpListener {

    function init() returns error? {
        log:printInfo("health data consumer service started");
    }

    resource function post messages(CCDAData[] messages) returns json|error? {
        json[] createdResources = [];
        foreach var message in messages {
            replayablechannel:ExecutionResult|replayablechannel:ExecutionError result = msgChannel.execute(message);
            if result is replayablechannel:ExecutionError {
                log:printError("error processing message", 'error = result);
                continue;
            }
            if !result.destinationResults.hasKey("FHIRServer") {
                log:printWarn("FHIRServer destination not found in the result");
                continue;
            }
            json createdResource = check result.destinationResults["FHIRServer"].ensureType();
            createdResources.push(createdResource);
        }
        if createdResources.length() == 0 {
            return error("Failed to create resources");
        }
        // Return the created resources
        return {createdResources: createdResources};
    }
}
