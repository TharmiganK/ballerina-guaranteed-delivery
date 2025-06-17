# MessageContext encapsulates the message and the relevant properties. Additionally, 
# it provides methods to manipulate the message properties and metadata.
public isolated class MessageContext {
    private Message message;

    # Initializes a new instance of MessageContext with the provided message.
    isolated function init(*Message message) {
        self.message = {...message.clone()};
    }

    # Get the unique identifier of the message.
    #
    # + return - The unique identifier of the message.
    public isolated function getId() returns string {
        lock {
            return self.message.id.clone();
        }
    }

    # Get the message as a record.
    #
    # + return - A record version of the message.
    public isolated function toRecord() returns Message {
        lock {
            return self.message.clone();
        }
    }

    # Get the content of the message.
    #
    # + return - The content of the message, which can be of any data type.
    public isolated function getContent() returns anydata {
        lock {
            return self.message.content.clone();
        }
    }

    # Set the content of the message.
    #
    # + content - The new content to set for the message.
    isolated function setContent(anydata content) {
        lock {
            self.message.content = content.clone();
        }
    }

    # Get message property by key.
    #
    # + key - The key of the property to retrieve.
    # + return - The value of the property if it exists, otherwise panics.
    public isolated function getProperty(string key) returns anydata {
        lock {
            if self.message.properties.hasKey(key) {
                return self.message.properties[key].clone();
            } else {
                panic error Error("Property not found: " + key);
            }
        }
    }

    # Set a property in the message.
    #
    # + key - The key of the property to set.
    # + value - The value to set for the property.
    public isolated function setProperty(string key, anydata value) {
        lock {
            self.message.properties[key] = value.clone();
        }
    }

    # Remove a property from the message.
    #
    # + key - The key of the property to remove.
    # + return - If the property exists, it is removed; otherwise, it panics.
    public isolated function removeProperty(string key) returns anydata {
        lock {
            if self.message.properties.hasKey(key) {
                return self.message.properties.remove(key).clone();
            } else {
                panic error Error("Property not found: " + key);
            }
        }
    }

    # Check if a property exists in the message.
    #
    # + key - The key of the property to check.
    # + return - Returns true if the property exists, otherwise false.
    public isolated function hasProperty(string key) returns boolean {
        lock {
            return self.message.properties.hasKey(key);
        }
    }

    # Returns whether the destination needs to be skipped or not.
    #
    # + destination - The name of the destination to check.
    # + return - Returns true if the destination is skipped, otherwise false.
    isolated function isDestinationSkipped(string destination) returns boolean {
        lock {
            return self.message.metadata.skipDestinations.indexOf(destination) !is ();
        }
    }

    # Mark a destination to be skipped.
    #
    # + destination - The name of the destination to skip.
    isolated function skipDestination(string destination) {
        lock {
            if !self.isDestinationSkipped(destination) {
                self.message.metadata.skipDestinations.push(destination);
            }
        }
    }

    # Add an error to the message context.
    #
    # + handlerName - The name of the handler where the error occurred.
    # + err - The error to set on the message context.
    isolated function addError(string handlerName, error err) {
        lock {
            ErrorInfo errorInfo = createErrorInfo(err);
            if self.message.destinationErrors is map<ErrorInfo> {
                self.message.destinationErrors[handlerName] = errorInfo;
            } else {
                self.message.destinationErrors = {[handlerName]: errorInfo};
            }
        }
    }


    # Set an error message on the message context.
    # 
    # + msg - The error message to set on the message context.
    isolated function setErrorMessage(string msg) {
        lock {
            self.message.errorMsg = msg;
        }
    }

    # Set an error to the message context.
    #
    # + msg - The error message to set on the message context.
    # + err - The error to set on the message context.
    isolated function setError(error err, string? msg = ()) {
        lock {
            string errorMsg = msg is string ? msg : err.message();
            self.message.errorMsg = errorMsg;
            self.message.errorStackTrace = createStackTrace(err);
            self.message.errorDetails = createErrorDetailMap(err);
        }
    }

    # Clone the message context.
    #
    # + return - A new instance of MessageContext with the same message.
    isolated function clone() returns MessageContext {
        lock {
            MessageContext clonedContext = new (self.message.clone());
            return clonedContext;
        }
    }

    # Clean the error information for replay.
    isolated function cleanErrorInfoForReplay() {
        lock {
            self.message.errorMsg = ();
            self.message.errorStackTrace = ();
            self.message.errorDetails = ();
            self.message.destinationErrors = ();
        }
    }
}

isolated function createErrorInfo(error err) returns ErrorInfo => {
    message: err.message(),
    stackTrace: createStackTrace(err),
    detail: createErrorDetailMap(err)
};

isolated function createStackTrace(error err) returns string[] {
    error:StackFrame[] stackTrace = err.stackTrace();
    string[] stackTraceStrings = [];
    foreach error:StackFrame frame in stackTrace {
        stackTraceStrings.push(frame.toString());
    }
    return stackTraceStrings;
}

isolated function createErrorDetailMap(error err) returns map<anydata> => err.detail() is map<anydata> ? <map<anydata>>err.detail() : {};
