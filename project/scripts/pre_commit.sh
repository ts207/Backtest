#!/bin/bash
set -e

# 1. Run Pipeline Governance Audit
echo "Running Pipeline Governance Audit..."
python3 project/scripts/pipeline_governance.py --audit

# 2. Run Repo Hygiene Check
echo "Running Repo Hygiene Check..."
bash project/scripts/check_repo_hygiene.sh

# 3. Sync Schemas (optional, can be part of commit if needed)
echo "Syncing Schemas..."
python3 project/scripts/pipeline_governance.py --sync

echo "Pre-commit checks passed!"
