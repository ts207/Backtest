# Product Guidelines

## Documentation Style

- Be precise and technical.
- Prefer explicit contracts over vague summaries.
- Keep run commands executable as written.

## Naming and Contracts

- Use `snake_case` for identifiers and spec keys.
- Keep event names microstructure-focused and stable.
- Include horizon/context explicitly in derived feature and candidate names.

## Integrity Expectations

- Preserve PIT correctness in all stages.
- Keep checklist/evaluation guards fail-closed in protected flows.
- Ensure run manifests remain auditable and complete.

## Reporting Expectations

- Reports should be traceable to run ID, config/spec hashes, and commit metadata.
- Stage outputs should be attributable to stage instances where applicable.
