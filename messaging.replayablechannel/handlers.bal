# Represents a transformer function that processes the message content and returns a modified 
# message content.
public type Transformer isolated function (MessageContext msgCtx) returns anydata|error;

# Represents a filter function that checks the message context and returns a boolean indicating 
# whether the message should be processed further.
public type Filter isolated function (MessageContext msgCtx) returns boolean|error;

# Represents a generic message processor that can process the message and return an error if the 
# processing fails.
public type GenericProcessor isolated function (MessageContext msgCtx) returns error?;

# Represents a processor router function that processes the message context and returns a processor.
# An error can be returned if the routing fails, or nil if it wants to stop processing the message.
public type ProcessorRouter isolated function (MessageContext msgCtx) returns Processor|error?;

# Represents a processor that can be a filter, transformer, or processor and can be attached to a 
# channel for processing messages. Processors should be idompotent i.e. repeating the execution 
# should not change the outcome or the channel state.
public type Processor GenericProcessor|Filter|Transformer|ProcessorRouter;

# Represents a source flow that processes the message context. The pipline can consist a single 
# processor or an array of processors. The source pipline will run in the configured order and the 
# result of the last processor will be used as the message content for the next step in the channel 
# flow.
public type SourceFlow Processor|[Processor...];

# Represents a destination function that processes the message context and returns a result or an 
# error if it failed to send the message to the destination. Destinations are typically contains a 
# sender or a writer that sends or writes the message to a specific destination.
public type Destination isolated function (MessageContext msgCtx) returns any|error;

# Represents a destination with processors that processes the message context and sends the message
# to a destination. The flow can consist of an array of processors followed by a destination. The
# processors in the flow will run in the configured order and the result of the last processor will
# be used as the message content for the destination.
public type DestinationWithProcessors [[Processor...], Destination];

# Represents a destination flow that processes the message context and sends the message to a
# destination. The flow can consist of a single destination or an array of processors followed by a
# destination. The processors in the flow will run in the configured order and the result of the last
# processor will be used as the message content for the destination.
public type DestinationFlow Destination|DestinationWithProcessors;

# Represents a destination router function that processes the message context and returns a 
# destination or an error if the routing fails, or nil if it wants to stop processing the message.
public type DestinationRouter isolated function (MessageContext msgCtx)
    returns DestinationFlow|error?;

# Represents the flow of destinations that can be used to send the message to multiple
# destinations. The flow can consist of a single destination router or a single destination flow, 
# or an array of destination flows. The destination flows will run in parallel and the results will 
# be collected.
public type DestinationsFlow DestinationRouter|DestinationFlow|DestinationFlow[];

# Handler related configuration.
#
# + name - The name of the handler.
public type HandlerConfiguration record {|
    string name;
|};

# Processor configuration annotation.
public const annotation HandlerConfiguration ProcessorConfig on function;

# Filter configuration annotation.
public const annotation HandlerConfiguration FilterConfig on function;

# Transformer configuration annotation.
public annotation HandlerConfiguration TransformerConfig on function;

# Processor router configuration annotation.
public annotation HandlerConfiguration ProcessorRouterConfig on function;

# Destination configuration annotation.
public annotation HandlerConfiguration DestinationConfig on function;

# Destination router configuration annotation.
public annotation HandlerConfiguration DestinationRouterConfig on function;
