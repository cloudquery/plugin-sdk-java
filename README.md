# CloudQuery Plugin SDK for Java

This is the high-level package to use for developing CloudQuery plugins in Java.

## Setup

### Authenticate to GitHub Packages

```bash
# Set up authentication to GitHub Packages, more in https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-gradle-registry#authenticating-to-github-packages
export GITHUB_ACTOR=<your-github-username>
# Classic personal access token with `read:packages` scope
export GITHUB_TOKEN=<personal-access-token>
```

### Install pre-commit hooks

- Install `pre-commit` from <https://pre-commit.com/#install>
- Run `pre-commit install` to install the hooks

## Build

```bash
./gradlew build
```
