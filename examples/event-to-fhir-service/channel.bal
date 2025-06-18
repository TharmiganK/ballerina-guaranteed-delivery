import tharmigan/messaging.storeprocessor;
import tharmigan/mirth;

final mirth:Channel msgChannel;

configurable "in-memory"|"rabbitmq"|"directory" storeType = "directory";

final storeprocessor:MessageStore failureStore;
final storeprocessor:MessageStore replayStore;

function init() returns error? {

    if storeType == "rabbitmq" {
        failureStore = check new storeprocessor:RabbitMqMessageStore("messages.bi.dlq");
        replayStore = check new storeprocessor:RabbitMqMessageStore("messages.bi");
    } else if storeType == "in-memory" {
        failureStore = new storeprocessor:InMemoryMessageStore();
        replayStore = new storeprocessor:InMemoryMessageStore();
    } else {
        failureStore = new storeprocessor:LocalDirectoryMessageStore("dls");
        replayStore = new storeprocessor:LocalDirectoryMessageStore("replay");
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
