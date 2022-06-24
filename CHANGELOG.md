# Release History

## 0.18.0 (Unreleased)

### Features Added
* Added `ConnectionError` type that's returned when a connection is no longer functional.
* Added `LinkTargetCapabilities()` option to specify desired target capabilities.
* Added `SASLType` used when configuring the SASL authentication mechanism.

### Breaking Changes
* Removed `ErrConnClosed` and `ErrTimeout` sentinel error types.
* The following methods now require a `context.Context` as their first parameter.
  * `Client.NewSession()`, `Session.NewReceiver()`, `Session.NewSender()`
* Removed `context.Context` parameter from method `Receiver.Prefetched()`.
* The following type names had the prefix `AMQP` removed to prevent stuttering.
  * `AMQPAddress`, `AMQPMessageID`, `AMQPSymbol`, `AMQPSequenceNumber`, `AMQPBinary`
* Various `Default*` constants are no longer exported.
* The args to `Receiver.ModifyMessage()` have changed.
* The "variadic config" pattern for `Client` and `Session` constructors has been replaced with a struct-based config.
  * This removes the `ConnOption` and `SessionOption` types and all of the associated configuration funcs.
  * The `ConnTLS()` option was removed as part of this change.
* The `Dial()` and `New()` constructors now require an `*ConnOptions` parameter.
* `Client.NewSession()` now requires a `*SessionOptions` parameter.
* The various SASL configuration funcs have been slightly renamed.

### Bugs Fixed
* Fixed potential panic in `muxHandleFrame()` when checking for manual creditor.
* Fixed potential panic in `attachLink()` when copying source filters.

### Other Changes
* Errors when reading/writing to the underlying `net.Conn` are now wrapped in a `ConnectionError` type.
* Disambiguate error message for distinct cases where a session wasn't found for the specified remote channel.
* Removed `link.Paused` as it didn't add much value and was broken in some cases.
