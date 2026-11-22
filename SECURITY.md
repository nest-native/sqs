# Security Policy

Thank you for helping keep `@nest-native/sqs` safe for NestJS and Amazon SQS
applications.

## Supported Versions

Security fixes target the current published package line.

| Package | Supported |
| --- | --- |
| `@nest-native/sqs` latest minor | Yes |
| Older unpublished branches | No |

## Reporting A Vulnerability

Please do not open a public issue for vulnerabilities or suspected secret
leakage.

Use GitHub's private vulnerability reporting for this repository when available:

<https://github.com/nest-native/sqs/security/advisories/new>

If private reporting is unavailable, contact the maintainer through the GitHub
profile and include only the minimum information needed to establish a private
channel. Do not send exploit details, AWS credentials, session tokens, queue URLs of
real systems, or customer data in public comments.

## What To Include

Private reports are most useful when they include:

- Affected package version or commit.
- NestJS, `@aws-sdk/client-sqs`, and Node.js versions.
- The smallest reproduction or vulnerable code path.
- Expected impact, such as authorization bypass through a consumer handler,
  input-validation gaps in message deserialization, secret leakage in payloads
  or attributes, dependency confusion, or a message deleted before it was
  handled.
- Whether the issue affects package code, samples, docs, CI, or release
  automation.

Please redact secrets, hostnames, AWS credentials, account IDs you consider
private, tokens, and private customer data.

## Project Security Boundaries

This package is a Nest transport layer around `@aws-sdk/client-sqs`.
Applications still own:

- AWS credentials and client configuration.
- IAM policies, queue policies, and encryption (SSE-KMS) settings.
- Message construction, serialization, and input handling.
- Authorization and tenant selection inside handlers.
- Concurrency, visibility timeouts, and redrive policies.
- Validation choices such as class-validator DTOs or app-owned Zod schemas.

Security fixes in this repository focus on package behavior — especially that
a message is deleted only after it was handled, that shutdown does not strand
in-flight messages, and that the transport never logs or leaks credentials or
message contents — alongside samples, docs, release automation, and patterns
that could encourage unsafe usage.

## Supply-Chain Notes

The published package keeps `"dependencies": {}`. Runtime requirements live in
`peerDependencies`; build tooling lives in `devDependencies`. Every dependency
addition or update is reviewed for legitimacy and inspected for install or
lifecycle scripts. `@aws-sdk/client-sqs` is a peer dependency owned by the
consuming application, so the application's tree holds exactly one SDK.

## Disclosure

The maintainer will acknowledge valid private reports as soon as practical,
coordinate a fix when the issue is in scope, and publish release notes or an
advisory when public disclosure is appropriate.
