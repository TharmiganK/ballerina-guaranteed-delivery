# Error details.
#
# + message - the message associated with the error, which includes an ID and the message content.
public type ErrorDetails record {|
    Message message;
|};

# Generic error type.
public type Error distinct error;

# Execution error type for the channel execution.
public type ExecutionError distinct Error & error<ErrorDetails>;
