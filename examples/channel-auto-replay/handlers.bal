import tharmigan/channel;
import ballerina/log;
import ballerina/file;
import ballerina/io;

@channel:ProcessorConfig {
    name: "MsgProcessor"
}
isolated function processMessage(channel:MessageContext ctx) returns error? {
    Message message = check ctx.getContent().toJson().fromJsonWithType(Message);
    log:printInfo("processing message with ID: " + message.id);
    // Simulate message processing
    log:printInfo("message processed successfully", id = ctx.getId());
}

@channel:DestinationConfig {
    name: "FileWriter"
}
isolated function writePayloadToFile(channel:MessageContext ctx) returns error? {
    json payload = ctx.getContent().toJson();
    string filePath = "./processed_data/" + ctx.getId() + ".json";
    if check file:test(filePath, file:EXISTS) {
        return error("Cannot write to file, file already exists: " + filePath);
    }
    check io:fileWriteJson(filePath, payload);
    log:printInfo("payload written to file", filePath = filePath);
}

@channel:DestinationConfig {
    name: "HttpEndpoint"
}
isolated function sendToHttpEp(channel:MessageContext ctx) returns json|error {
    json payload = ctx.getContent().toJson();
    return httpEndpoint->/patients.post(payload);
}
