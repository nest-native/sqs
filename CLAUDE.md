# CLAUDE.md

@GUIDELINES_NEST_SQS.md

The imported guidelines are binding. Two always-on rules:
- Full-mode infra (`infra:up` + `test:full`) is local-only — never wire it into the unit path. CI's real-backend job runs its own ElasticMQ service container.
- A change to how the package calls the SDK adds a case to the semantics contract (`packages/sqs/test/contract`), which runs against both `InMemorySqs` and the real endpoint, in the same change.
