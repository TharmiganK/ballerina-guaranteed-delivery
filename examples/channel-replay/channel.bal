import tharmigan/messaging.replayablechannel;

final replayablechannel:Channel channel;

// final messaging:LocalFileMessageStore dlstore = new ("./dls");
// final messaging:LocalFileMessageStore replayStore = new ("./replay");

// messaging:DeadLetterStoreConfiguration dlstoreConfig = {
//     dlstore: dlstore,
//     listenerConfig: {
//         enabled: true,
//         targetStore: replayStore,
//         pollingConfig: {
//             pollingInterval: 10,
//             maxMessagesPerPoll: 2
//         },
//         maxRetries: 3
//     }
// };

function init() returns error? {
    channel = check new ("Channel",
        sourceFlow = processMessage,
        destinationsFlow = [
            writePayloadToFile,
            sendToHttpEp
        ]
    );
}
