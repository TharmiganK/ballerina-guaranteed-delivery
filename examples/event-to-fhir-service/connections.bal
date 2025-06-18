import ballerina/http;

final http:Client httpEndpoint = check new ("http://localhost:8080/api/v1");
