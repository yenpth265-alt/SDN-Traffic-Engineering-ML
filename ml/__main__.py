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
    freeze = cmds.add_parser("export-config", help="Freeze an offline-selected artifact as online deployment YAML")
    freeze.add_argument("--artifact", required=True)
    freeze.add_argument("--out", required=True)
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
    pred.add_argument("--artifact", "--deployment-config", dest="artifact", required=True,
                      help="Model directory or frozen deployment YAML")
    pred.add_argument("--snapshot", required=True)
    pred.add_argument("--context", required=True)
    pred.add_argument("--method", choices=["rf", "persistence", "moving_average"], default="rf")
    pred.add_argument("--out", required=True)
    pred.add_argument("--audit", required=True, help="Separate ML-local diagnostic JSON")
    online = cmds.add_parser("online", help="Fetch Snapshot v1.4 from Data API, predict, and POST ForecastBatch")
    online.add_argument("--artifact", "--deployment-config", dest="artifact", required=True,
                        help="Model directory or frozen deployment YAML")
    online.add_argument("--base-url", required=True)
    online.add_argument("--run-id", required=True)
    online.add_argument("--snapshot-id")
    online.add_argument("--forecast-batch-id")
    online.add_argument("--watch-stdin", action="store_true",
                        help="Keep model loaded and read snapshot IDs or JSON ticks line-by-line from stdin")
    online.add_argument("--method", choices=["auto", "rf", "persistence", "moving_average"], default="auto")
    online.add_argument("--ml-api-key", default=None, help="Defaults to SDN_ML_KEY env var")
    online.add_argument("--reader-api-key", default=None, help="Defaults to SDN_READER_KEY env var")
    online.add_argument("--max-snapshot-age-s", type=float, default=2.0)
    online.add_argument("--forecast-timeout-ms", type=float, default=500.0)
    online.add_argument("--fallback-policy", choices=["skip", "persistence"], default="skip")
    online.add_argument("--fallback-epsilon", type=float, default=0.0)
    online.add_argument("--http-timeout-s", type=float, default=5.0)
    online.add_argument("--max-retries", type=int, default=2)
    online.add_argument("--emit-event", action="store_true")
    online.add_argument("--dry-run", action="store_true")
    online.add_argument("--out", default=None, help="Optional file for ForecastBatch JSON in single-tick mode")
    online.add_argument("--audit", default=None, help="Optional file for audit JSON in single-tick mode")
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
        elif args.command == "export-config":
            from ml.deployment import export_deployment
            result = export_deployment(args.artifact, args.out)
            print(f"Frozen L={result['lookback_L']} H={result['horizon_H']} in {args.out}")
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
        elif args.command == "online":
            import json
            from ml.inference.online import DataApiClient, DataApiError, OnlineForecastRunner
            require(bool(args.snapshot_id) != bool(args.watch_stdin),
                    "Specify exactly one of --snapshot-id or --watch-stdin")
            client = DataApiClient(
                args.base_url,
                ml_api_key=args.ml_api_key,
                reader_api_key=args.reader_api_key,
                timeout_s=args.http_timeout_s,
                max_retries=args.max_retries,
            )
            runner = OnlineForecastRunner(args.artifact, client)
            if args.watch_stdin:
                for raw_line in sys.stdin:
                    line = raw_line.strip()
                    if not line:
                        continue
                    tick = json.loads(line) if line.startswith("{") else {"snapshot_id": line}
                    tick_run = tick.get("run_id", args.run_id)
                    tick_snap = tick["snapshot_id"]
                    tick_batch = tick.get("forecast_batch_id")
                    try:
                        res = runner.process_snapshot(
                            tick_run,
                            tick_snap,
                            forecast_batch_id=tick_batch,
                            method=tick.get("method", args.method),
                            max_snapshot_age_s=tick.get("max_snapshot_age_s", args.max_snapshot_age_s),
                            forecast_timeout_ms=tick.get("forecast_timeout_ms", args.forecast_timeout_ms),
                            fallback_policy=tick.get("fallback_policy", args.fallback_policy),
                            fallback_epsilon=tick.get("fallback_epsilon", args.fallback_epsilon),
                            emit_event=args.emit_event,
                            dry_run=args.dry_run,
                        )
                    except DataApiError as exc:
                        res = {"status": "API_ERROR", "run_id": tick_run, "snapshot_id": tick_snap,
                               "status_code": exc.status_code, "reason": str(exc)}
                    except ContractError as exc:
                        res = {"status": "SKIPPED", "run_id": tick_run, "snapshot_id": tick_snap,
                               "reason": str(exc)}
                    print(json.dumps({k: v for k, v in res.items() if k != "batch"}, ensure_ascii=False),
                          flush=True)
                return 0
            try:
                res = runner.process_snapshot(
                    args.run_id,
                    args.snapshot_id,
                    forecast_batch_id=args.forecast_batch_id,
                    method=args.method,
                    max_snapshot_age_s=args.max_snapshot_age_s,
                    forecast_timeout_ms=args.forecast_timeout_ms,
                    fallback_policy=args.fallback_policy,
                    fallback_epsilon=args.fallback_epsilon,
                    emit_event=args.emit_event,
                    dry_run=args.dry_run,
                )
            except DataApiError as exc:
                if args.audit:
                    write_json(args.audit, {"status": "API_ERROR", "status_code": exc.status_code,
                                            "reason": str(exc)})
                print(f"ERROR: {exc}", file=sys.stderr)
                return 2
            if args.audit:
                write_json(args.audit, res["audit"])
            if res["status"] != "OK":
                print(f"SKIPPED: {res['reason']}", file=sys.stderr)
                return 2
            if args.out and res.get("batch") is not None:
                write_json(args.out, res["batch"])
            print(f"Published {res['row_count']} rows, batch={res['forecast_batch_id']}, "
                  f"inserted={res['inserted']}")
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
