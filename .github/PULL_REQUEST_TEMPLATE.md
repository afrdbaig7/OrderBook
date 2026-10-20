## Description
Provide a brief summary of the changes and the rationale behind them.

## Related Issue
Fixes #(issue number)

## Changes Made
- 

## Verification Checklist
- [ ] `cargo fmt --all --check` passes
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes
- [ ] `cargo test --workspace` passes (including property tests)
- [ ] Frontend tests pass (`npm test` in `web/`, if frontend modified)
- [ ] Frontend builds cleanly (`npm run build` in `web/`, if frontend modified)
- [ ] No regression to matching engine invariants (FIFO, maker-price, conservation)
