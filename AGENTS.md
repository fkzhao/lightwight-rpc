# Repository Instructions

- Target Java 17. Keep source and build configuration compatible with JDK 17.
- Use `dev.yumix.rpc` as the base package for Java code and Protocol Buffer definitions.
- Use the Maven reactor. Keep reusable code in `rpc-sdk`, examples in `rpc-example`, and load tools in `rpc-benchmark`.
- Keep Protocol Buffer definitions under each module's `src/main/proto` directory.
- Keep all source comments, documentation, and README content in English.
- Preserve the documented 36-byte little-endian nshead base and 8-byte little-endian request ID extension.
- Use protocol version 3 and its 64-bit header request ID for multiplexed calls.
- Keep network event loops free of blocking application handlers; execute RPC methods on the server worker pool.
- Reserve method IDs 0 and 0xFFFFFFFF for empty-body PING and PONG control frames.
- Keep reconnects asynchronous and never replay an in-flight RPC automatically after a disconnect.
