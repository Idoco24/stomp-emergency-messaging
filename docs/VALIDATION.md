# Recovery validation

Prepared on 2026-10-15. Java build tested with Temurin 21 on Windows and the
cross-language tests run in Ubuntu/WSL with g++ and Boost 1.83. The server targets
Java 17; CI checks JDK 17 and 21.

## Baseline and history

- Preserved all eight original commits through `2580fc8`; no history rewrite.
- Recovered tracked working files from the final Assignment3 folder. Three files
  had substantive pending edits; most other status entries were line endings.
- Scanned 91 historical blobs for common credential/private-key patterns, known
  student IDs and absolute Windows user paths: no matches in those categories.
- Original commit email metadata is deliberately retained; preparation commits
  use the GitHub noreply address. Historical macOS/editor metadata remains in the
  original commits while being removed from the current tree.
- Original folder, Git state and submission ZIP are retained as recovery sources.

## Reproduced problems and corrections

The recovered server had no test suite. Of 13 new protocol/codec checks, 11 failed
against the recovered implementation before correction. The recovered C++ client
failed compilation because `std::this_thread::sleep_for` lacked its required
header; a compiler-version-specific iomanip include was also present.

Corrections include:

- Parse exact header names only before the body, split values at the first colon,
  accept CRLF headers and preserve message-body whitespace.
- Require authentication before subscription/send operations; reject duplicate
  subscription IDs and avoid echoing passwords in errors.
- Normalize the application's bare/slash destinations consistently and deliver
  each MESSAGE with its recipient's subscription ID and an atomic message ID.
- Retain independent subscriptions to the same channel when one is removed.
- Release usernames/subscriptions on both DISCONNECT and abrupt socket EOF.
- Serialize blocking socket writes; flush reactor final replies before closure;
  ignore selector updates for invalid/closed keys.
- Allocate connection IDs within each server instead of using remote TCP ports.
- Replace client protocol sharing across threads with one owner loop.
- Preserve partial received frames across polling timeouts, handle server errors
  without aborting, and stop cleanly on stdin EOF.
- Parse command names exactly, reject malformed login/report input, retain colons
  in event fields, select summary usernames exactly and apply unsubscribe receipts.
- Make C++ builds portable and incremental; make the Java JAR executable with
  explicit, pinned build/test plugins.

## Test coverage

**14 JUnit tests** check routing/subscription IDs, login requirements, malformed
headers, colon-containing credentials, password redaction, session cleanup,
duplicate/multiple subscription IDs, CRLF handling, body/header separation, UTF-8
framing, inter-frame blank lines and null-terminated encoding. A deterministic
handler test forces the selector to drain a DISCONNECT receipt before the protocol
marks termination. It failed before the final close-wakeup correction and passes
after it. The termination flag is also visible across selector/worker threads.

**9 Python integration test methods** run real processes/sockets. Seven exercise
both server modes; one repeats 20 reactor disconnects, and the delayed-frame client
check uses a controlled fake server.
The client workflow includes login, subscribe, JSON report, positive and negative
username summaries, unsubscribe, logout and EOF. Network checks assert final
receipts reach the client before closure and that disconnected usernames can log
in again. A malformed-login test verifies a clean client exit rather than abort.

Tests are intended for correctness of this coursework subset. They do not prove
full STOMP conformance, security under hostile input, or scalability under load.

## Known limits

- No TLS, durable users/messages, password hashing, transaction handling, ACK/NACK,
  binary content-length frames or escaped-header processing.
- Heartbeat-only lines are tolerated; heartbeat negotiation and dead-peer timers
  are not implemented.
- Incoming frame/command queues do not yet impose configurable size limits.
- Shared routing locks can let a slow recipient delay unrelated senders.
- Console parsing uses whitespace-separated arguments; use paths without spaces.
- Summary times use the client's local timezone; summaries retain arrival order.
- The terminal input thread remains available after logout/connection errors so a
  user can log in again; quit or EOF ends the client program.
- Unused framework examples (echo/newsfeed/RCI and the legacy echoClient source)
  are retained for provenance and are not covered by the STOMP integration suite.
- Starter-code/teammate attribution and coursework publication rights still need
  human review before changing this private repository to public.

Specification reference: https://stomp.github.io/stomp-specification-1.2.html
