import ballerina/http;

listener http:Listener httpListener = new (9090);

service /api on httpListener {

    isolated resource function post messages(Message message) returns http:Created|error {
        _ = check channel.execute(message);
        return http:CREATED;
    }
}
