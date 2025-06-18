import ballerina/http;
import ballerina/log;

import tharmigan/messaging.replayablechannel;

listener http:Listener httpListener = new (9090);

service / on httpListener {

    function init() returns error? {
        log:printInfo("health data consumer service started");
    }

    resource function post events(HealthDataEvent[] events) returns json|error? {
        json[] createdResources = [];
        foreach var event in events {
            replayablechannel:ExecutionResult|replayablechannel:ExecutionError result = msgChannel.execute(event);
            if result is replayablechannel:ExecutionError {
                log:printError("error processing event", 'error = result);
                // Store the message in the failure store
                error? storeResult = failureStore.store(result.detail().message);
                if storeResult is error {
                    log:printError("failed to store message in the failure store", 'error = storeResult);
                } else {
                    log:printInfo("message stored in the failure store");
                }
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
