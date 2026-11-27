# Errors, retries and security

## Errors and retries

Errors mirror the Python hierarchies: `EventHubError` and `ServiceBusError`
subtypes such as `ConnectionLostError`, `AuthenticationError`,
`MessageLockLostError`, `SessionLockLostError`, `MessageSizeExceededError` and
`ServiceBusServerBusyError`, all subtypes of `AzureCore.AzureError`.
Blob Storage raises the `azure.core` types (`ResourceNotFoundError`,
`ResourceExistsError`, `ResourceModifiedError`, `ClientAuthenticationError`,
`HttpResponseError`, `ServiceRequestError`) with the service's `error_code`
(compare with `StorageErrorCode.blob_not_found` etc.).

Transient failures (connection loss, timeouts, server busy) are retried
according to `retry_total`, `retry_backoff_factor`, `retry_backoff_max` and
`retry_mode`, and connections and links are re-established transparently.
`OperationTimeoutError` exists in both messaging modules and is not exported;
use `EventHubs.OperationTimeoutError` or `ServiceBus.OperationTimeoutError`.

## Security

- TLS certificate and host name verification are always on unless you pass
  `connection_verify=false` (which logs a warning). TLS 1.2 is the minimum.
- Secrets never appear in `show` output, log messages or error messages.
  URLs in errors are stripped of their query strings (SAS signatures).
- HTTP requests never follow redirects or store cookies, so credentials are
  never forwarded to another host. Service Bus REST lock URLs must point back
  to the namespace host. Bearer tokens, account keys and SAS tokens are only
  sent over HTTPS. Plain connections are limited to local emulators. On a
  loopback host any credential may be used; on a single-label container name
  such as `azurite` (which may resolve through the DNS search domain to another
  machine) only the emulators' public keys (Azurite's development key,
  `SharedAccessKey=SAS_KEY_VALUE`) or anonymous access are accepted, so real
  keys, SAS tokens and Entra ID tokens are never sent unencrypted off the
  machine.
- The Storage XML reader rejects DTDs and entity declarations (no XXE or entity
  expansion), bounds nesting depth and requires valid UTF-8.
- The AMQP decoder validates every length and count against the received
  bytes and bounds nesting depth, frame and message sizes, so malformed or
  hostile input raises an error instead of exhausting memory. Julia never
  frees interned symbols, so a process interns at most 50,000 new AMQP symbols
  (4 MiB) received from peers; symbols that already exist are not limited.
- Event position filters are validated (no filter-expression injection).
  Tenant IDs, scopes and client IDs are validated before they are used in URLs
  or command lines. The Azure CLI and `azd` are executed directly, never
  through a shell, and with a timeout.
- The Azure Arc key file path returned by the local endpoint is validated
  against the agent's key directory before it is read.

To report a vulnerability, follow `SECURITY.md` in the repository.
