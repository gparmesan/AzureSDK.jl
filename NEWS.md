# Release notes

All notable changes to AzureSDK.jl are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[semantic versioning](https://semver.org/).

## [Unreleased]

### Fixed

- Service Bus: a late reply to a drain request no longer makes
  `receive_messages` exceed `max_wait_time` in `PEEK_LOCK` mode, or lose
  messages in `RECEIVE_AND_DELETE` mode. Closing a `PEEK_LOCK` receiver
  releases messages that were delivered but never received.

## [0.1.0]

First version (not registered yet).

- Event Hubs: producer and consumer clients, batches, partitions, checkpoints
  in Blob Storage
- Service Bus: queues, topics and subscriptions, sessions, peek-lock and
  receive-and-delete, settlement, scheduled messages, raw AMQP messages
- Blob Storage: service, container and blob clients
- Microsoft Entra ID authentication: client secret, certificate, client
  assertion, workload identity, managed identity, Azure CLI, Azure Developer
  CLI, chained and default credentials, continuous access evaluation
- Shared access keys and signatures
- AMQP 1.0, AMQP over WebSockets and HTTP transports; automatic reconnection,
  retries and token refresh
