## Engineering Philosophy

* **Break freely before 1.0.** Prefer the best architecture over backward compatibility.
* **No legacy by default.** Remove obsolete code, APIs, compatibility layers, and abstractions instead of preserving them.
* **Git is the archive.** Deleted code does not need to remain in the codebase.
* **Design for the future, not the past.** Ask how the system should be built today, not how to preserve yesterday's design.
* **Prefer modern technology.** Favor new, innovative, state-of-the-art approaches over conservative or outdated ones when they provide meaningful advantages.
* **Stay current.** Keep dependencies, tooling, language features, runtimes, and standards as up-to-date as practical.
* **Accept calculated risk.** Prefer experimentation and technological progress over stagnation.
* **Rewrite when justified.** Do not stack workarounds on top of fundamentally wrong abstractions.
* **Delete aggressively.** Dead code, deprecated paths, temporary shims, and unnecessary complexity should disappear.
* **Move fast without sacrificing rigor.** Breaking changes are acceptable; regressions, poor testing, and unjustified complexity are not.
* **Optimize pre-1.0 software for evolution, not preservation.**

## Rust Idioms

* **Use Rust’s type system instead of manual encodings.** Prefer enums, newtypes, and typed variants over `u8`/`bool` tags, magic constants, or structs that manually emulate enums. Make invalid states unrepresentable whenever practical.
