# GitHub Actions Security Guidelines

This document outlines the security practices and rules enforced for GitHub Actions workflows in this repository.

## Implemented Security Measures

### 1. Actionlint (Required Check)

All pull requests that modify workflow files must pass actionlint checks. This linter helps catch:
- Syntax errors in workflow files
- Invalid action references
- Incorrect YAML structure
- Common workflow misconfigurations

**Workflow:** `.github/workflows/actionlint.yml`

### 2. Harden Runner

All jobs include the `step-security/harden-runner` action as the first step. This provides:
- Network egress filtering and monitoring
- Audit logging of all network requests
- Detection of suspicious behavior
- Runtime security hardening

### 3. Pinned Actions

All actions are pinned to specific commit SHAs rather than tags or branches. This prevents:
- Supply chain attacks via compromised action tags
- Unexpected breaking changes from action updates
- Unauthorized modifications to dependencies

**Format:** `owner/repo@<full-commit-sha> # vX.Y.Z`

The comment includes the version tag for human readability.

### 4. Minimal Permissions

Workflows use the principle of least privilege:
- Default `permissions: contents: read` at the workflow level
- Explicit permissions only when needed (e.g., `pull-requests: write` for auto-approve)
- No use of `write-all` or overly broad permissions

### 5. Secure Trigger Events

- The repository avoids using `pull_request_target` where possible to prevent untrusted code execution
- When elevated permissions are needed, the `issue_comment` trigger is used with strict conditions

## Repository Rules (Recommended Configuration)

The following repository rules should be configured in GitHub Settings to enforce these security practices:

### Branch Protection Rules (main branch)

1. **Require status checks to pass**
   - ✅ Actionlint
   - ✅ Build and test jobs from CI workflow

2. **Require branches to be up to date**
   - Ensures changes are tested against the latest code

3. **Do not allow bypassing the above settings**
   - Prevents administrators from merging without checks

### Rulesets for Actions

Create a repository ruleset with the following restrictions:

1. **Block Actions Not From Trusted Sources**
   - Allow actions from: `actions/*`, `step-security/*`, verified creators
   - Block unverified or newly created actions

2. **Require Actions to be Pinned**
   - All action references must use commit SHAs
   - Tag-based references (e.g., `@v4`) should be rejected

3. **Restrict Workflow Permissions**
   - Default permissions: `read` for contents
   - Explicit approval required for workflows requesting write permissions

## Updating Pinned Actions

When updating actions to newer versions:

1. Identify the new version tag (e.g., `v4.2.0`)
2. Find the commit SHA for that tag on GitHub
3. Update the action reference with both SHA and version comment:
   ```yaml
   uses: actions/checkout@<new-sha> # v4.2.0
   ```
4. Test the workflow to ensure compatibility
5. Submit the change via pull request (will be validated by actionlint)

## Example: Finding Commit SHA for Action Version

```bash
# For actions/checkout@v4.2.0
# 1. Visit https://github.com/actions/checkout/releases/tag/v4.2.0
# 2. Click on the commit SHA shown for the release
# 3. Copy the full 40-character SHA
# 4. Update workflow: uses: actions/checkout@<sha> # v4.2.0
```

## Security Incident Response

If you discover a security issue in any of the workflows:

1. Do not create a public issue
2. Contact the repository maintainers privately
3. If the issue is in a third-party action, report it to that action's maintainers
4. Wait for a fix before disclosing publicly

## Additional Resources

- [GitHub Actions Security Best Practices](https://docs.github.com/en/actions/security-guides/security-hardening-for-github-actions)
- [Step Security Harden-Runner](https://github.com/step-security/harden-runner)
- [Actionlint](https://github.com/rhysd/actionlint)
- [OpenSSF Scorecard](https://github.com/ossf/scorecard)
