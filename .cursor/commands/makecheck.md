---
makecheck: Run make check and type-check with validation
---

# Custom Cursor Commands

## /makecheck

Run both `make check` and `make type-check` to ensure code quality, then verify there are no errors or warnings.

Steps:
1. Run `make check` (linting, formatting, and tests)
2. Run `make type-check` (Pyright type checking)
3. Verify both commands completed successfully with no errors or warnings
4. Report any issues found or confirm all checks passed
