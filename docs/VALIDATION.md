# Validation

Build the Java server with `mvn -f server/pom.xml clean verify` and the C++ client
with `make -C client`. Run `python3 tests/integration_test.py` on Linux/WSL.
CI checks the server and cross-language workflow on JDK 17 and 21.

## Coverage

The 14 JUnit tests cover routing, subscription IDs, authentication, header parsing,
password redaction, session cleanup, UTF-8 framing, and reactor disconnect races.

The nine integration tests exercise real processes and sockets, both server modes,
fragmented frames, login, report delivery, summaries, unsubscribe, graceful and
abrupt disconnects, and delayed server replies. Tests use synthetic credentials.

## Limits

- No TLS, password hashing, durable storage, transactions, ACK/NACK, binary
  content-length frames, or escaped-header processing.
- No negotiated heartbeat timers or configurable incoming frame/queue size limits.
- A slow recipient can delay other senders through shared routing locks.
- Command arguments cannot contain spaces; summary times use the local timezone.
- The server listens on available network interfaces. Use a trusted local network
  and demo credentials; this is not a hardened public service.

These tests establish specific behavior, not full STOMP conformance or load capacity.
See the [STOMP 1.2 specification](https://stomp.github.io/stomp-specification-1.2.html).
