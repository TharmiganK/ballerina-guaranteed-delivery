import tharmigan/messaging.storeprocessor;
import tharmigan/mirth;

final mirth:Channel msgChannel;

configurable "in-memory"|"rabbitmq" storeType = "rabbitmq";

final storeprocessor:MessageStore failureStore;
final storeprocessor:MessageStore replayStore;

function init() returns error? {

    if storeType == "rabbitmq" {
        failureStore = check new storeprocessor:RabbitMqMessageStore("messages.bi.dlq");
        replayStore = check new storeprocessor:RabbitMqMessageStore("messages.bi");
    } else {
        failureStore = new storeprocessor:InMemoryMessageStore();
        replayStore = new storeprocessor:InMemoryMessageStore();
    }

    msgChannel = check new ("event-to-fhir", {
        sourceFlow: [
            hasEventDataType,
            extractPayload,
            routeByDataType
        ],
        destinationsFlow: [
            sendToFHIRServer,
            writePayloadToFile,
            sendToHttpEp
        ],
        failureStore: failureStore,
        replayConfig: {
            replayStore: replayStore,
            pollingInterval: 10
        }
    });
}
