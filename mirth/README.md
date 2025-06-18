## Overview

This package provides a channel abstraction which can be configured with a default dead letter store and a replay store. When the channel is initialized, it will automatically start a message processor listener on the replay store to replay the messages.

Additionaly, the package exposes an HTTP endpoint to inspect the available channels, channel failure and replay failure messages. When the replay is called on a message failure, the message will be moved form the dead letter store to the replay store and the listener attached the replay store will automatically start processing the message again.
