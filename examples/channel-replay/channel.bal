import tharmigan/messaging.replayablechannel;

final replayablechannel:Channel channel;

function init() returns error? {
    channel = check new (
        sourceFlow = processMessage,
        destinationsFlow = [
            writePayloadToFile,
            sendToHttpEp
        ]
    );
}
