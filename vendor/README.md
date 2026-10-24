# Vendored Ante crates

Source: https://github.com/rizkyandriawan/ante, commit 84b86c0.
`ante-core/src`, `ante-amqp/src`, and `ante-security/src` are copied unchanged;
Cargo metadata is standalone. License: MIT (see ../LICENSE). Keep updates
explicit and run the Rust tests plus the Python integration suite after updating.

`ante-security` provides TLS configuration, Argon2id credential verification, and
queue-scoped permissions. `ante-amqp` exposes both the legacy plaintext
`serve_until` and the secure `serve_secure_until_with_options`.
