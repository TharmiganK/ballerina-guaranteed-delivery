# Event to FHIR Service: Bridging Custom Health Data to FHIR

This service seamlessly accepts custom health data events, transforms them into standardized FHIR® (Fast Healthcare Interoperability Resources) and performs several key actions:

- Persists them in a configured FHIR repository.
- Sends them to an external HTTP endpoint for additional processing.
- Stores the processed messages in a dedicated `processed_data` directory.
- Archives failed messages in a `failure` directory for review and replay.
- Provides a replay mechanism for failed messages, allowing them to be reprocessed after issues are resolved.
- The message which is failed to be replayed with the configured retry count will be moved to a Dead Letter Storage (DLS) directory - `dls` for further investigation.

## Overview

The Event to FHIR Service is designed to standardize and streamline your custom health data workflows. By converting diverse health data events into the universally recognized FHIR format, it ensures unparalleled interoperability and data standardization. This demo specifically showcases the conversion of patient-related health data events into a FHIR Patient resource.

Key functionalities include:

- FHIR Repository Storage: Automatically stores transformed FHIR resources in your specified FHIR repository, ensuring centralized and compliant data storage.
- External Endpoint Integration: For real-time processing or integration with other systems, the service can forward transformed FHIR resources to any external HTTP endpoint.
- Local Data Archiving: Both successfully processed and failed messages are archived locally. This provides a robust mechanism for record-keeping, auditing, and error handling.

## Setup and Run

Getting the Event to FHIR Service up and running is straightforward. Follow these steps:

1. Clone the Repository

    First, obtain the project files by cloning the repository. Replace [repository-url] with the actual URL of your Git repository:

    ```bash
    git clone [repository-url]
    cd event_to_fhir_service # Navigate into the cloned directory
    ```

2. Install Ballerina

    This service is built with Ballerina. If you don't have it installed, follow the detailed instructions on the official Ballerina website to get it set up for your operating system.

3. Build the Service

    Once Ballerina is installed, you can build the service from the project's root directory:

    ```bash
    bal build
    ```

    This command compiles the Ballerina code and generates the necessary executable artifacts.

4. Configure the Service

    Before running, you'll need to configure the service to connect to your FHIR repository and define other operational parameters. Create a file named Config.toml in the root directory of your project and populate it with the following configurations:

    ```toml
    # --- FHIR Repository Settings ---
    # Base URL of your FHIR repository (e.g., "https://your-fhir-repository.com/fhir")
    fhirServerUrl = "https://your-fhir-repository.com/fhir"
    # URL for obtaining authentication tokens (e.g., "https://your-identity-provider.com/oauth2/token")
    tokenUrl = "https://your-identity-provider.com/oauth2/token"
    # Client ID for authenticating with the FHIR repository
    clientId = "your-client-id"
    # Client Secret for authenticating with the FHIR repository (keep this secure!)
    clientSecret = "your-client-secret"
    # Space-separated list of OAuth 2.0 scopes required for FHIR API access
    scopes = ["patient/*.read", "patient/*.write"]
    ```

5. Create the `replay` directory

    The service will monitor a `replay` directory for failed messages that you want to reprocess. Create this directory in the root of your project:

    ```bash
    mkdir replay
    ```

6. Run the Service

    With the service built and configured, you can now run it:

    ```bash
    bal run target/bin/event_to_fhir_service.jar
    ```

    This command starts the Event to FHIR Service, and it will begin listening for incoming custom health data events.

## Review and Replay Failed Messages

The Event to FHIR Service incorporates a robust error handling mechanism. Messages that fail processing are automatically moved to a failure directory.

### Reviewing Failed Messages

All failed messages, along with their corresponding error details, are stored in the `failure` directory. Each file in this directory represents a failed message, and its content will include both the original message and the reason for its failure. Regularly review this directory to identify and resolve underlying issues.

### Replaying Failed Messages

Once you've identified and resolved the cause of a message failure, you can easily replay it. To do this, simply move the failed message file from the `failure` directory to the `replay` directory. The service continuously monitors the replay directory. Any files moved into it will be automatically picked up and re-attempted for processing. If the replay is successful in any of the retries, the message will be removed from the `replay` directory. If the replay fails after all configured retries, the message will be moved to the `dls` (Dead Letter Storage) directory for further investigation.
