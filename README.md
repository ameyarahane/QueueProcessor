# QueueProcessor
A multi-threaded queue processor for processing SQS messages.

### Design
The application consists of a number of threads that poll the SQS queue (MessagePollers) for messages and store those
messages in a local buffer. Each processor thread has a number of poller threads attached to it that read and
process messages from the buffer.

These classes can be extended to perform whatever processing task that needs to be performed. The number of pollers
and processors per poller can be configured depending on the rate at which messages can be polled and the time required
to process a message.

The SQS queue needs to be configured properly to ensure that each polled message remains invisible for the time
required for it to be processed, otherwise those messaged will be retried automatically by SQS. One way to test this
out is by having extra processors per poller until you can determine the right balance.

### Code structure
The QueueProcessInitializer is a class that can be instantiated by any application that provides the dependencies
required. The caller will basically block the main thread until it is time to stop polling and call the stop method
on the initializer.

