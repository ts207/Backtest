# Repo Hygiene Policy

## Source Of Truth
- Commit code, configs, tests, and concise docs.
- Do **not** commit generated runtime artifacts.

## Generated Artifact Rules
Blocked from tracking:
- `data/reports/**`
- `data/runs/**`
- `data/lake/cleaned/**`
- `data/lake/features/**`
- `data/lake/runs/**`

Tracked placeholders are allowed:
- `data/.gitkeep`
- `data/lake/.gitkeep`
- `data/runs/.gitkeep`
- `data/reports/.gitkeep`

## Sidecar Metadata Files
The following files must never be present:
- `*:Zone.Identifier`
- `*#Uf03aZone.Identifier`
- `*#Uf03aZone.Identifier:Zone.Identifier`

## Hygiene Commands
- Check: `make check-hygiene`
- Clean sidecars: `make clean-hygiene`

CI enforces the same checks in `.github/workflows/pytest.yml`.
