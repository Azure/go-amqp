# Release History

## 0.18.0 (Unreleased)

### Features Added
* Added `ConnectionError` type that's returned when a connection is no longer functional.
* Added `LinkTargetCapabilities()` option to specify desired target capabilities.

### Breaking Changes
* Removed `ErrConnClosed` and `ErrTimeout` sentinel error types.

### Bugs Fixed
* Fixed potential panic in `muxHandleFrame()` when checking for manual creditor.
* Fixed potential panic in `attachLink()` when copying source filters.

### Other Changes
* Errors when reading/writing to the underlying `net.Conn` are now wrapped in a `ConnectionError` type.
* Disambiguate error message for distinct cases where a session wasn't found for the specified remote channel.
* Removed `link.Paused` as it didn't add much value and was broken in some cases.
