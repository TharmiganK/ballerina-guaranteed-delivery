import ballerina/log;
import ballerinax/health.fhir.r4;
import ballerinax/health.fhir.r4utils.ccdatofhir;

import tharmigan/channel;
import ballerina/http;

public type CCDAData record {
    string ccdaStr;
};

@channel:TransformerConfig {
    name: "CCDADataTransformer"
}
isolated function transformToCCDAData(channel:MessageContext ctx) returns anydata|error {
    anydata payload = ctx.getContent();
    CCDAData|error ccdaDataRecord = payload.cloneWithType();
    if ccdaDataRecord is error {
        return r4:createFHIRError("Error occurred while cloning the payload", r4:ERROR, r4:INVALID);
    }
    xml|error ccdData = ccdaDataRecord.ccdaStr.cloneWithType();
    if ccdData is error {
        return r4:createFHIRError("Error occurred while parsing the payload to xml", r4:ERROR, r4:INVALID);
    }
    r4:Bundle fhirPayload = check ccdatofhir:ccdaToFhir(ccdData);
    r4:BundleEntry[] entries = <r4:BundleEntry[]>fhirPayload.entry;
    foreach var entry in entries {
        map<anydata> fhirResource = <map<anydata>>entry?.'resource;
        if fhirResource["resourceType"] == "Patient" {
            log:printInfo("FHIR resource mapped");
            return fhirResource;
        }
    }
    return r4:createFHIRError("No Patient resource found in the CCDA data", r4:ERROR, r4:INVALID);
}

final http:Client httpEndpoint = check new ("http://localhost:8080/api/v1");

@channel:DestinationConfig {
    name: "HttpEndpoint"
}
isolated function sendToHttpEp(channel:MessageContext ctx) returns json|error {
    json payload = ctx.getContent().toJson();
    return httpEndpoint->/patients.post(payload);
}