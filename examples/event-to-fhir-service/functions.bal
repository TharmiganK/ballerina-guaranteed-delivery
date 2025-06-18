import ballerina/http;
import ballerina/log;

import ballerinax/health.clients.fhir;
import ballerinax/health.fhir.r4;

# FHIR server configurations
configurable string fhirServerUrl = ?;
configurable string tokenUrl = ?;
configurable string[] scopes = ?;
configurable string client_id = ?;
configurable string client_secret = ?;

final fhir:FHIRConnector fhirConnectorObj = check new ({
    baseURL: fhirServerUrl,
    mimeType: fhir:FHIR_JSON,
    authConfig: {
        tokenUrl: tokenUrl,
        clientId: client_id,
        clientSecret: client_secret,
        scopes: scopes,
        optionalParams: {
            "resource": fhirServerUrl
        }
    }
});

public isolated function createResource(json payload) returns r4:FHIRError|fhir:FHIRResponse {
    fhir:FHIRResponse|fhir:FHIRError fhirResponse = fhirConnectorObj->create(payload);
    if fhirResponse is fhir:FHIRError {
        log:printError("error occurred while creating FHIR resource", 'error = fhirResponse);
        return r4:createFHIRError(fhirResponse.message(), r4:ERROR, r4:INVALID, httpStatusCode = http:STATUS_INTERNAL_SERVER_ERROR);
    }
    log:printInfo("data stored successfully");
    return fhirResponse;
}

