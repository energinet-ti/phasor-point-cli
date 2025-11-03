---
release: Create a new release with updated CHANGELOG
---

# Custom Cursor Commands

## /release

Create a new release by running the release script and ensuring the CHANGELOG contains only user-facing changes.

CRITICAL: The user MUST specify a version number (e.g., 0.3.1, 1.0.0).

Steps:
1. Find the latest release tag: `git describe --tags --abbrev=0`
2. Show commits since last release: `git log <last-tag>..HEAD --oneline`
3. Review these commits and identify user-facing changes
4. Run: `./scripts/create_release.sh <VERSION>`
   - Script creates release branch
   - Script creates new version section in CHANGELOG
   - Script commits and pushes
5. Checkout the release branch: `git checkout release/<VERSION>`
6. Update the NEW version section in CHANGELOG.md with ONLY user-facing changes from the git log:
   - What new features users can use
   - What behavior changed from user perspective
   - What bugs were fixed that users experienced
   - REMOVE any technical implementation details, refactoring notes, or code-level changes
7. Commit the updated CHANGELOG: `git add CHANGELOG.md && git commit --amend --no-edit`
8. Force push the updated release branch: `git push -f origin release/<VERSION>`

Example user-facing changes:
- Added support for exporting data in JSON format
- Changed default timeout from 30s to 60s
- Fixed bug where dates before 2020 were rejected

Example NON-user-facing changes (remove these):
- Refactored ConnectionManager to use dependency injection
- Added type hints to data_processor module
- Improved test coverage for extraction manager
- Updated CI/CD pipeline configuration
