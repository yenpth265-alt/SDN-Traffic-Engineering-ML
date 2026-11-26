"""Run from repository root: python -m ml --help."""
import argparse
from pathlib import Path
import sys

from ml.common import ContractError, new_directory, read_json, require, write_json

ROOT = Path(__file__).resolve().parents[1]


def parser():
    p = argparse.ArgumentParser(description="SDN demand ML: offline training and causal prediction")
    cmds = p.add_subparsers(dest="command", required=True)
    prep = cmds.add_parser("prepare", help="Validate Data CSV export and create IF04 file bundle")
    prep.add_argument("--history", required=True)
    prep.add_argument("--runs", required=True)
    prep.add_argument("--catalog", default=str(ROOT / "configs/network/flow_groups.csv"))
    prep.add_argument("--max-lookback", type=int, default=60)
    prep.add_argument("--max-horizon", type=int, default=10)
    prep.add_argument("--stride", type=int, default=5)
    prep.add_argument("--out", required=True)
    fit = cmds.add_parser("train", help="Fit on train; select only on validation")
    fit.add_argument("--bundle", required=True)
    fit.add_argument("--config", default=str(ROOT / "configs/ml/training.yaml"))
    fit.add_argument("--out", required=True)
    ev = cmds.add_parser("evaluate", help="Evaluate frozen artifact on test, without tuning")
    ev.add_argument("--bundle", required=True)
    ev.add_argument("--artifact", required=True)
    ev.add_argument("--out", required=True)
    run = cmds.add_parser("run", help="One command: prepare, train/validate, then evaluate test")
    run.add_argument("--history", required=True)
    run.add_argument("--runs", required=True)
    run.add_argument("--catalog", default=str(ROOT / "configs/network/flow_groups.csv"))
    run.add_argument("--config", default=str(ROOT / "configs/ml/training.yaml"))
    run.add_argument("--stride", type=int, default=5)
    run.add_argument("--out", required=True)
    schedule = cmds.add_parser("from-schedule", help="OFFLINE ONLY: import Network CSV/ZIP, train and evaluate")
    schedule.add_argument("--schedule", required=True)
    schedule.add_argument("--seed-manifest", default=str(ROOT / "configs/experiments/runner_manifest.json"))
    schedule.add_argument("--catalog", default=str(ROOT / "configs/network/flow_groups.csv"))
    schedule.add_argument("--warmup-seconds", type=int, required=True)
    schedule.add_argument("--measurement-seconds", type=int, required=True)
    schedule.add_argument("--config", default=str(ROOT / "configs/ml/training.yaml"))
    schedule.add_argument("--stride", type=int, default=5)
    schedule.add_argument("--out", required=True)
    pred = cmds.add_parser("predict", help="Read SDN snapshot supplied by Data; write seven-field rows")
    pred.add_argument("--artifact", required=True)
    pred.add_argument("--snapshot", required=True)
    pred.add_argument("--context", required=True)
    pred.add_argument("--method", choices=["rf", "persistence", "moving_average"], default="rf")
    pred.add_argument("--out", required=True)
    pred.add_argument("--audit", required=True, help="Separate ML-local diagnostic JSON")
    demo = cmds.add_parser("demo", help="Synthetic end-to-end smoke run; NOT benchmark data")
    demo.add_argument("--out", required=True)
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    predictor = None
    try:
        if args.command == "prepare":
            from ml.preprocessing.bundle import prepare_bundle
            result = prepare_bundle(args.history, args.runs, args.catalog, args.out,
                                    args.max_lookback, args.max_horizon, args.stride)
            print(f"Bundle ready: {result['counts']}")
        elif args.command == "train":
            from ml.training.train import train
            result = train(args.bundle, args.config, args.out)
            print(f"Selected L={result['lookback_L']} H={result['horizon_H']} epsilon={result['epsilon']}")
        elif args.command == "evaluate":
            from ml.validation.evaluate import evaluate
            result = evaluate(args.bundle, args.artifact, args.out)
            print(f"Test evaluation written to {args.out}/evaluation.json")
        elif args.command in ("run", "from-schedule"):
            import yaml
            from ml.preprocessing.bundle import prepare_bundle
            from ml.training.train import train
            from ml.validation.evaluate import evaluate
            with open(args.config, encoding="utf-8") as f:
                search = yaml.safe_load(f)
            out = new_directory(args.out)
            if args.command == "from-schedule":
                from ml.preprocessing.schedule import import_schedule
                report = import_schedule(args.schedule, args.seed_manifest, args.catalog,
                                         out / "input", args.warmup_seconds, args.measurement_seconds)
                print(f"OFFLINE schedule import: {report['schedules']} schedules, {report['rows']} rows", flush=True)
                history, runs = out / "input/history.csv", out / "input/runs.json"
            else:
                history, runs = args.history, args.runs
            prepare_bundle(history, runs, args.catalog, out / "bundle",
                           max(search["lookback_candidates"]), max(search["horizon_candidates"]),
                           args.stride)
            train(out / "bundle", args.config, out / "model")
            evaluate(out / "bundle", out / "model", out / "evaluation")
            print(f"Pipeline complete. Read: {out / 'evaluation/REPORT.md'}")
        elif args.command == "predict":
            from ml.inference.predict import Predictor
            require(Path(args.out).resolve() != Path(args.audit).resolve(), "Rows and audit must be separate")
            require(not Path(args.out).exists() and not Path(args.audit).exists(),
                    "Output exists: use unique filenames per tick; do not reuse stale predictions")
            predictor = Predictor(args.artifact)
            rows = predictor.predict_batch(read_json(args.snapshot), read_json(args.context), args.method)
            write_json(args.audit, predictor.last_audit)
            write_json(args.out, rows)
            print(f"Published {len(rows)} rows, source={rows[0]['source']}")
        else:
            from ml.demo import create_demo_inputs, synthetic_snapshot
            from ml.inference.predict import Predictor
            from ml.preprocessing.bundle import prepare_bundle
            from ml.training.train import train
            from ml.validation.evaluate import evaluate
            out = new_directory(args.out)
            (out / "DEMO_ONLY.txt").write_text(
                "Synthetic pipeline verification only. Not project/network benchmark results.\n")
            create_demo_inputs(out / "input", ROOT / "configs/network/flow_groups.csv")
            prepare_bundle(out / "input/history.csv", out / "input/runs.json",
                           ROOT / "configs/network/flow_groups.csv", out / "bundle", 5, 5, 5)
            train(out / "bundle", out / "input/training.yaml", out / "model")
            evaluate(out / "bundle", out / "model", out / "evaluation")
            predictor = Predictor(out / "model")
            snapshot, context = synthetic_snapshot(predictor.config, predictor.metadata)
            write_json(out / "snapshot.json", snapshot)
            write_json(out / "context.json", context)
            rows = predictor.predict_batch(snapshot, context)
            write_json(out / "predictions.json", rows)
            write_json(out / "audit.json", predictor.last_audit)
            print(f"SYNTHETIC DEMO passed: {len(rows)} prediction rows in {out}")
        return 0
    except (ContractError, KeyError, ValueError, OSError) as exc:
        if (args.command == "predict" and not Path(args.audit).exists()
                and Path(args.audit).resolve() != Path(args.out).resolve()):
            # Never write an invalid/partial PredictionRows file.
            write_json(args.audit, predictor.last_audit if predictor and predictor.last_audit else
                       {"status": "INVALID", "reason": str(exc)})
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
