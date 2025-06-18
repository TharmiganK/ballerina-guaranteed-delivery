import ballerina/http;
import ballerina/log;

import tharmigan/messaging.replayablechannel;
import tharmigan/messaging.storeprocessor;

configurable int mngtPort = 9095;

service on new http:Listener(9095) {

    isolated resource function get channels() returns string[] {
        lock {
            return channels.keys().clone();
        }
    }

    isolated resource function get channels/[string name]/failure() returns replayablechannel:Message|http:NotFound|http:InternalServerError {
        lock {
            if !channels.hasKey(name) {
                return <http:NotFound>{
                    body: {
                        "message": string `Channel with name: '${name}' not found`
                    }
                };
            }
            Channel channel = channels.get(name);

            storeprocessor:MessageStore failureStore = channel.getFailureStore();
            anydata|error topMessage = failureStore.retrieve();
            if topMessage is error {
                return <http:InternalServerError>{
                    body: {
                        "message": string `Failed to retrieve messages from failure store for channel: '${name}'`,
                        "cause": topMessage.message()
                    }
                };
            }

            error? result = failureStore.acknowledge(false);
            if result is error {
                log:printError("failed to acknowledge message from failure store for channel", channel = name, 'error = result);
            }

            replayablechannel:Message|error message = topMessage.toJson().fromJsonWithType();
            if message is error {
                return <http:InternalServerError>{
                    body: {
                        "message": string `Failed to deserialize message from failure store for channel: '${name}'`,
                        "cause": message.message()
                    }
                };
            }

            return message.clone();
        }
    }

    isolated resource function post channels/[string name]/failure/replay(replayablechannel:Message? message) returns http:Accepted|http:NotFound|http:NotImplemented|http:InternalServerError {
        lock {
            if !channels.hasKey(name) {
                return <http:NotFound>{
                    body: {
                        "message": string `Channel with name: '${name}' not found`
                    }
                };
            }
            Channel channel = channels.get(name);

            storeprocessor:MessageStore? replayStore = channel.getReplayStore();
            if replayStore is () {
                return <http:NotImplemented>{
                    body: {
                        "message": string `Replay store is not implemented for channel: '${name}'`
                    }
                };
            }

            storeprocessor:MessageStore failureStore = channel.getFailureStore();
            anydata|error topMessage = failureStore.retrieve();
            if topMessage is error {
                if message is () {
                    return <http:InternalServerError>{
                        body: {
                            "message": string `Failed to retrieve messages from failure store for channel: '${name}'`,
                            "cause": topMessage.message()
                        }
                    };
                }
                log:printError("failed to retrieve message from failure store for channel", channel = name, 'error = topMessage);
            } else {
                error? result = failureStore.acknowledge(true);
                if result is error {
                    log:printError("failed to acknowledge message from failure store for channel", channel = name, 'error = result);
                }
            }

            anydata replayableMessage = message.clone() ?: checkpanic topMessage.ensureType();

            error? result = replayStore.store(replayableMessage);
            if result is error {
                return <http:InternalServerError>{
                    body: {
                        "message": string `Failed to store message in replay store for channel: '${name}'`,
                        "cause": result.message()
                    }
                };
            }
            return <http:Accepted>{
                body: {
                    "message": string `Message successfully stored in replay store for channel: '${name}'`
                }
            };
        }
    }
}
