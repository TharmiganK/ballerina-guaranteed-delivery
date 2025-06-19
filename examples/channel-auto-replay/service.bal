import ballerina/http;

configurable int port = 9090;

service /api on new http:Listener(port) {

    isolated resource function post messages(Message message) returns http:Ok|error {
        _ = check channel.execute(message);
        return http:OK;
    }
}
