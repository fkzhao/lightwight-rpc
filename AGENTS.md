# Repository Instructions

- Target Java 17. Keep source and build configuration compatible with JDK 17.
- Use Maven and keep Protocol Buffer definitions under `src/main/proto`.
- Keep all source comments, documentation, and README content in English.
- Preserve the documented 36-byte little-endian nshead frame layout unless the protocol is intentionally revised.
- Keep network event loops free of blocking application handlers; execute RPC methods on the server worker pool.
