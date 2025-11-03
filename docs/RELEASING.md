# Release Process

This document describes the complete release process for phasor-point-cli.

## Overview

Releases are managed using:
- **Git tags** for versioning (via setuptools-scm)
- **Release branches** for preparation (CHANGELOG, final docs)
- **GitHub Releases** for distribution
- **PyPI** for package distribution (manual trigger)

## Release Workflow

### 1. Prepare Release Branch

Create a release branch from `main`:

```bash
# Ensure you're on main and up to date
git checkout main
git pull origin main

# Run the release script
./scripts/create_release.sh 0.0.1 "Initial release"
```

The script will:
- Create a release branch (e.g., `release/0.0.1`)
- Update `CHANGELOG.md` with the version and current date
- Commit the changes
- Push the release branch to GitHub

### 2. Review Release Branch

Before creating the PR, you may want to:

```bash
# Switch to the release branch
git checkout release/0.0.1

# Review or add additional changes if needed
# - Update documentation
# - Add migration notes
# - Update examples

# Commit any additional changes
git add .
git commit -m "Update release documentation"
git push
```

### 3. Create Pull Request

Create a PR from the release branch to `main`:

1. Go to GitHub: https://github.com/energinet-ti/phasor-point-cli/compare
2. Base: `main` ← Compare: `release/0.0.1`
3. Title: `Release v0.0.1`
4. Description: Copy relevant sections from CHANGELOG.md

Example PR description:
```markdown
## Release v0.0.1

Initial release of PhasorPoint CLI.

### Added
- Data extraction from PhasorPoint databases
- Multiple output formats (Parquet, CSV)
- Automatic power calculations
- Data quality validation
- Batch extraction support
- Performance optimization features
- Comprehensive documentation

### Testing
- [ ] All CI checks pass
- [ ] Manual testing completed
- [ ] Documentation reviewed
```

### 4. Review and Merge

1. Wait for CI checks to pass
2. Get team review/approval
3. Merge the PR (use "Create a merge commit" or "Squash and merge")

### 5. Create Tag (GitHub Release Auto-Created)

After the PR is merged, create the tag on main. The GitHub Release will be automatically created.

#### Option A: Create Tag via GitHub UI (Recommended)

1. Go to: https://github.com/energinet-ti/phasor-point-cli/releases/new
2. Click "Choose a tag"
3. Type `v0.0.1` and select "Create new tag: v0.0.1 on publish"
4. Ensure target is `main`
5. Release title: `v0.0.1`
6. Description: Copy from CHANGELOG.md
7. Click "Publish release"

This creates the tag and immediately publishes the release.

#### Option B: Create Tag via Git CLI (Release Auto-Created)

```bash
# Switch to main and pull latest
git checkout main
git pull origin main

# Create and push the tag
git tag -a v0.0.1 -m "Release v0.0.1"
git push origin v0.0.1

# The release.yml workflow will automatically:
# - Build the package
# - Create a GitHub Release
# - Upload wheel and source distributions
```

Check the workflow progress at: https://github.com/energinet-ti/phasor-point-cli/actions

### 6. Publish to PyPI

After the GitHub release is created:

#### Test on TestPyPI First

1. Go to: https://github.com/energinet-ti/phasor-point-cli/actions
2. Click on "Publish to PyPI" workflow
3. Click "Run workflow"
4. Select branch: `main`
5. Select environment: `pypi-test`
6. Click "Run workflow"

Wait for completion and verify:
```bash
# Install from TestPyPI
pip install --index-url https://test.pypi.org/simple/ phasor-point-cli

# Test basic functionality
python -m phasor_point_cli --version
python -m phasor_point_cli --help
```

#### Publish to Production PyPI

If testing is successful:

1. Go back to Actions
2. Run "Publish to PyPI" workflow again
3. Select environment: `pypi`
4. Click "Run workflow"

Verify publication:
```bash
# Install from PyPI
pip install phasor-point-cli

# Verify version
python -m phasor_point_cli --version  # Should show 0.0.1
```

### 7. Announce Release

Optional: Announce the release to users/stakeholders
- Email notification
- Slack/Teams message
- Update internal documentation

## Maintaining CHANGELOG

The CHANGELOG follows [Keep a Changelog](https://keepachangelog.com/) format.

### During Development

Add entries to the `[Unreleased]` section as you work:

```markdown
## [Unreleased]

### Added
- New feature X
- New command Y

### Changed
- Modified behavior of Z

### Fixed
- Bug fix for issue #123
```

### Categories

Use these standard categories:
- **Added** - New features
- **Changed** - Changes in existing functionality
- **Deprecated** - Soon-to-be removed features
- **Removed** - Now removed features
- **Fixed** - Bug fixes
- **Security** - Security fixes

### During Release

The release script automatically:
- Converts `[Unreleased]` to `[VERSION] - DATE`
- Creates a new empty `[Unreleased]` section

## Version Numbers

This project follows [Semantic Versioning](https://semver.org/):

- **MAJOR.MINOR.PATCH** (e.g., 1.2.3)
- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

Examples:
- `0.0.1` - Initial release
- `0.1.0` - First stable release with core features
- `1.0.0` - First production-ready release
- `1.1.0` - New feature added
- `1.1.1` - Bug fix

## Hotfix Process

For urgent fixes to production:

```bash
# Create hotfix branch from main
git checkout main
git pull origin main
git checkout -b hotfix/1.0.1

# Make fixes
git commit -m "Fix critical bug"

# Update CHANGELOG
# Add entry under a new [1.0.1] section

# Create PR to main
# After merge, tag as v1.0.1
```

## Troubleshooting

### Release Script Fails

**Problem**: Script says "Branch already exists"
```bash
# Delete local and remote branch
git branch -D release/0.0.1
git push origin --delete release/0.0.1
```

**Problem**: Uncommitted changes
```bash
# Commit or stash changes first
git stash
./scripts/create_release.sh 0.0.1
git stash pop
```

### Tag Already Exists

**Problem**: Tag `v0.0.1` already exists

```bash
# Delete tag locally and remotely
git tag -d v0.0.1
git push origin --delete v0.0.1

# Recreate it
git tag -a v0.0.1 -m "Release v0.0.1"
git push origin v0.0.1
```

### PyPI Publishing Fails

**Problem**: Package already exists on PyPI
- You cannot overwrite published versions
- Must increment version number

**Problem**: Authentication error
- Check that `PYPI_API_TOKEN` secret is set correctly
- Verify token has correct permissions

**Problem**: Build fails
```bash
# Test build locally first
python -m build
twine check dist/*
```

### Wrong Version After Release

**Problem**: Version shows `0.0.2.devX` instead of `0.0.1`

This happens when:
- No tag exists
- Building from commits after the tag
- Tag wasn't pushed

Solution:
```bash
# Verify tag exists and is pushed
git tag -l
git ls-remote --tags origin

# If missing, create and push
git tag v0.0.1
git push origin v0.0.1
```

## Checklist

Use this checklist for each release:

- [ ] All features for this release are merged to main
- [ ] All CI checks pass on main
- [ ] CHANGELOG.md has all changes documented in [Unreleased]
- [ ] Run `./scripts/create_release.sh X.Y.Z "Description"`
- [ ] Create PR from release branch to main
- [ ] Get PR reviewed and approved
- [ ] Merge PR to main
- [ ] Create GitHub Release with tag vX.Y.Z on main
- [ ] Trigger PyPI workflow with pypi-test environment
- [ ] Test installation from TestPyPI
- [ ] Trigger PyPI workflow with pypi environment
- [ ] Verify installation from production PyPI
- [ ] Announce release

## References

- [Keep a Changelog](https://keepachangelog.com/)
- [Semantic Versioning](https://semver.org/)
- [setuptools-scm documentation](https://setuptools-scm.readthedocs.io/)
- [PyPI publishing guide](https://packaging.python.org/en/latest/tutorials/packaging-projects/)

