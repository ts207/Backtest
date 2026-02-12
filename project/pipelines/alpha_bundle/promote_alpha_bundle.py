import argparse
import hashlib
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path


def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode('utf-8')).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class AlphaBundle:
    spec_patch_version: str
    strategy_version: str
    created_utc: str
    train_snapshot_hash: str
    code_hash: str
    dependency_hashes: dict

    universe_def_path: str
    universe_def_hash: str

    label_def_id: str
    horizon_bars: int

    orth_spec_path: str
    orth_spec_hash: str

    comb_model_path: str
    comb_model_hash: str

    score_to_mu_path: str | None
    score_to_mu_hash: str | None

    signal_cols: list[str]

    artifact_hash: str


def canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def main() -> int:
    p = argparse.ArgumentParser(description='Promote a frozen AlphaBundle artifact for production use.')
    p.add_argument('--strategy_version', required=True)
    p.add_argument('--train_snapshot_hash', required=True)
    p.add_argument('--code_hash', default='UNKNOWN')
    p.add_argument('--dependency_hashes_json', default='{}')

    p.add_argument('--universe_def_path', required=True)
    p.add_argument('--label_def_id', default='LBL_LOGRET_H')
    p.add_argument('--horizon_bars', type=int, required=True)

    p.add_argument('--orth_spec_path', required=True)
    p.add_argument('--comb_model_path', required=True)
    p.add_argument('--score_to_mu_path', default='')

    p.add_argument('--signal_cols', required=True, help='Comma-separated signal columns used by the model')
    p.add_argument('--out_dir', default='data/model_registry')
    p.add_argument('--spec_patch_version', default='1.0')

    args = p.parse_args()

    universe_def = Path(args.universe_def_path)
    orth_spec = Path(args.orth_spec_path)
    comb_model = Path(args.comb_model_path)
    score_to_mu = Path(args.score_to_mu_path) if args.score_to_mu_path else None

    dep_hashes = json.loads(args.dependency_hashes_json)

    bundle_dict = {
        'spec_patch_version': args.spec_patch_version,
        'strategy_version': args.strategy_version,
        'created_utc': datetime.now(timezone.utc).isoformat(),
        'train_snapshot_hash': args.train_snapshot_hash,
        'code_hash': args.code_hash,
        'dependency_hashes': dep_hashes,

        'universe_def_path': str(universe_def),
        'universe_def_hash': sha256_file(universe_def),

        'label_def_id': args.label_def_id,
        'horizon_bars': args.horizon_bars,

        'orth_spec_path': str(orth_spec),
        'orth_spec_hash': sha256_file(orth_spec),

        'comb_model_path': str(comb_model),
        'comb_model_hash': sha256_file(comb_model),

        'score_to_mu_path': str(score_to_mu) if score_to_mu else None,
        'score_to_mu_hash': sha256_file(score_to_mu) if score_to_mu else None,

        'signal_cols': [c.strip() for c in args.signal_cols.split(',') if c.strip()],
    }

    artifact_hash = sha256_str(canonical_json(bundle_dict))

    bundle = AlphaBundle(
        artifact_hash=artifact_hash,
        **bundle_dict,
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f'AlphaBundle_{artifact_hash[:12]}.json'
    out_path.write_text(canonical_json(asdict(bundle)), encoding='utf-8')

    print(str(out_path))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
