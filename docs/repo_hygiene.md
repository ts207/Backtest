# Repo Hygiene

## Source-of-Truth Boundary

Commit:
- code
- tests
- docs
- deterministic configs/specs

Do not commit:
- generated run artifacts
- generated reports
- lake partitions

## Blocked Runtime Paths

- `data/runs/**`
- `data/reports/**`
- `data/lake/cleaned/**`
- `data/lake/features/**`
- `data/lake/runs/**`
- `data/lake/trades/**`

## Allowed Placeholders

- `.gitkeep` placeholders in empty runtime dirs are acceptable.

## Sidecar File Policy

Never keep these files:
- `*:Zone.Identifier`
- `*#Uf03aZone.Identifier`
- `*#Uf03aZone.Identifier:Zone.Identifier`

## Operational Checks

- Use `git status --short` before commit.
- Ensure no runtime outputs appear in staged files.
- Keep docs synchronized with current CLI defaults and artifact contracts.
