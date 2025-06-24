import ballerina/file;
import ballerina/http;
import ballerina/io;
import ballerina/log;
import ballerinax/health.clients.fhir;
import ballerinax/health.fhir.r4;
import ballerinax/health.fhir.r4.international401;
import ballerinax/health.fhir.r4utils.ccdatofhir;
import ballerinax/health.fhir.r4.validator;
// import ballerinax/health.hl7v2.utils.v2tofhirr4;

import tharmigan/channel;

@channel:FilterConfig {
    name: "EventDataTypeFilter"
}
isolated function hasEventDataType(channel:MessageContext ctx) returns boolean|error {
    HealthDataEvent healthDataEvent = check ctx.getContent().toJson().fromJsonWithType();
    log:printInfo("health data event received", eventId = healthDataEvent.eventId);
    string? dataType = healthDataEvent?.dataType;
    if dataType is string {
        ctx.setProperty("dataType", dataType);
        return true;
    }
    log:printError("data type is not present in the health data event");
    return false;
}

@channel:TransformerConfig {
    name: "ExtractPayloadTransformer"
}
isolated function extractPayload(channel:MessageContext ctx) returns anydata|error {
    HealthDataEvent healthDataEvent = check ctx.getContent().toJson().fromJsonWithType();
    return healthDataEvent?.payload;
}

@channel:ProcessorRouterConfig {
    name: "DataTypeRouter"
}
isolated function routeByDataType(channel:MessageContext ctx) returns channel:Processor|error? {
    if !ctx.hasProperty("dataType") {
        log:printError("data type is not present in the health data event");
        return;
    }
    string dataType = check ctx.getProperty("dataType").ensureType();
    match dataType {
        "patient_data" => {
            return transformToPatientData;
        }
        // "hl7_data" => {
        //     return transformToHL7Data;
        // }
        "ccda_data" => {
            return transformToCCDAData;
        }
        _ => {
            log:printError("unsupported data type in the health data event", dataType = dataType);
            return error("Unsupported data type: " + dataType);
            // return r4:createFHIRError("Invalid event type", r4:ERROR, r4:INVALID);
        }
    }
}

@channel:TransformerConfig {
    name: "PatientDataTransformer"
}
isolated function transformToPatientData(channel:MessageContext ctx) returns international401:Patient|error {
    anydata payload = ctx.getContent();
    // return payload;
    Patient|error patientData = payload.cloneWithType();
    if patientData is error {
        return r4:createFHIRError("Error occurred while cloning the payload", r4:ERROR, r4:INVALID);
    }
    international401:Patient fhirPayload = mapPatient(patientData);
    r4:FHIRValidationError? validate = validator:validate(fhirPayload, international401:Patient);
    if validate is r4:FHIRValidationError {
        return r4:createFHIRError(validate.message(), r4:ERROR, r4:INVALID, cause = validate.cause(), errorType = r4:VALIDATION_ERROR, httpStatusCode = http:STATUS_BAD_REQUEST);
    }
    return fhirPayload;
}

// @channel:TransformerConfig {
//     name: "HL7DataTransformer"
// }
// isolated function transformToHL7Data(channel:MessageContext ctx) returns map<anydata>|error {
//     anydata payload = ctx.getContent();
//     HL7Data|error hl7Data = payload.cloneWithType();
//     if hl7Data is error {
//         return r4:createFHIRError("Error occurred while cloning the payload", r4:ERROR, r4:INVALID);
//     }
//     json|error v2tofhirResult = v2tofhirr4:v2ToFhir(hl7Data.mllpStr);
//     if v2tofhirResult is json {
//         log:printInfo(string `FHIR resource mapped: ${v2tofhirResult.toJsonString()}`, mappedData = v2tofhirResult);
//         r4:Bundle|error fhirPayload = v2tofhirResult.cloneWithType();
//         if fhirPayload is r4:Bundle {
//             r4:BundleEntry[] entries = <r4:BundleEntry[]>fhirPayload.entry;
//             foreach var entry in entries {
//                 map<anydata> fhirResource = <map<anydata>>entry?.'resource;
//                 if fhirResource["resourceType"] == "Patient" {
//                     log:printInfo(string `FHIR resource: ${fhirResource.toJsonString()}`, mappedData = fhirResource);
//                     return fhirResource;
//                 }
//             }
//         }
//     }
//     return r4:createFHIRError("Error occurred while mapping HL7 data to FHIR", r4:ERROR, r4:INVALID);
// }

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

@channel:DestinationConfig {
    name: "FHIRServer"
}
isolated function sendToFHIRServer(channel:MessageContext ctx) returns json|error {
    anydata payload = ctx.getContent();
    log:printInfo("sending data to FHIR server");
    r4:FHIRError|fhir:FHIRResponse response = createResource(payload.toJson());
    if response is fhir:FHIRResponse {
        log:printInfo("FHIR resource created successfully", statusCode = response.httpStatusCode);
        return response.'resource.toJson();
    }
    log:printError("error occurred while creating FHIR resource", 'error = response);
    return response;
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

