# Codex Cloud Setup

This repo is prepared for Codex Cloud with:

- environment scripts in `.codex/`
- an optional Codex PR review workflow in `.github/workflows/codex_pr_review.yml`

## 1. Connect Repo in Codex Cloud

1. Open Codex Cloud and connect this GitHub repository.
2. In the environment configuration:
   - setup script command: `bash .codex/setup.sh`
   - maintenance script command: `bash .codex/maintenance.sh`

These scripts provision `.venv` and ensure `BACKTEST_DATA_ROOT` defaults to `data/`.

## 2. Enable GitHub PR Reviews via Codex (Optional)

The workflow `.github/workflows/codex_pr_review.yml` triggers when someone comments `@codex` on a PR thread.

Required GitHub secret:

- `OPENAI_API_KEY`: an API key with access to Codex models.

After adding the secret, push to `main` and test by commenting `@codex review this PR`.

## 3. Local Validation

Run:

```bash
bash .codex/setup.sh
bash .codex/maintenance.sh
```

If these complete successfully, the Codex Cloud environment scripts are ready.
