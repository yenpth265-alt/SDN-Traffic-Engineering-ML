"""Evaluate frozen models, without tuning, on the held-out test split."""
import platform
import time
import numpy as np

from ml.baselines import moving_average, persistence
from ml.common import digest, new_directory, require, write_json
from ml.model_registry import load_artifact
from ml.preprocessing.bundle import TrainingBundle
from ml.preprocessing.features import build_features
from ml.training.train import rf_predict
from ml.validation.metrics import calibrate, errors


def evaluate(bundle_path, artifact_path, out):
    model, cfg, artifact_meta = load_artifact(artifact_path)
    b = TrainingBundle(bundle_path)
    require(b.sha256 == artifact_meta["bundle_sha256"], "Evaluation must use original held-out bundle")
    history, labels, meta = b.load("test")
    L, H = cfg["lookback_L"], cfg["horizon_H"]
    y = labels[:, :H]
    x = build_features(history, meta.flow_group_id.tolist(), cfg["flow_group_ids"], L)
    raw = rf_predict(model, x, H)
    pred = {
        "rf": calibrate(raw, cfg["epsilon"]),
        "persistence": calibrate(persistence.predict(history[:, -L:], H), cfg["baseline_epsilon"]),
        "moving_average": calibrate(moving_average.predict(history[:, -L:], H, cfg["ma_window"]),
                                    cfg["baseline_epsilon"]),
    }
    report = {"split": "test", "phase": "measurement",
              "common_sample_ids_sha256": digest(meta.sample_id.tolist()),
              "ml_config_sha256": artifact_meta["ml_config_sha256"],
              "data_provenance": b.info.get("data_provenance", {"kind": "unspecified"}),
              "selected_config": {key: cfg[key] for key in
                                  ("lookback_L", "horizon_H", "epsilon", "ma_window")},
              "run_count": int(meta.run_id.nunique()),
              "test_seeds": sorted(meta.seed.unique().tolist(), key=int),
              "fit_sample_count": artifact_meta["fit_sample_count"],
              "train_sample_count": artifact_meta["train_sample_count"],
              "rf_raw_metrics": errors(y, raw),
              "metrics": {name: errors(y, p) for name, p in pred.items()},
              "by_scenario": {}, "by_run": {}, "by_flow": {}}
    for scenario, indexes in meta.groupby("scenario_id").groups.items():
        ix = np.asarray(indexes)
        report["by_scenario"][scenario] = {name: errors(y[ix], p[ix]) for name, p in pred.items()}
    for run_id, indexes in meta.groupby("run_id").groups.items():
        ix = np.asarray(indexes)
        record = meta.iloc[ix[0]]
        report["by_run"][run_id] = {
            "scenario_id": record.scenario_id, "seed": record.seed,
            "schedule_id": record.schedule_id,
            "metrics": {name: errors(y[ix], p[ix]) for name, p in pred.items()},
        }
    for flow_id, indexes in meta.groupby("flow_group_id").groups.items():
        ix = np.asarray(indexes)
        report["by_flow"][flow_id] = {name: errors(y[ix], p[ix]) for name, p in pred.items()}
    latencies = []
    for _, indexes in list(meta.groupby(["run_id", "cutoff"]).groups.items())[:100]:
        ix = np.asarray(indexes)
        start = time.perf_counter()
        xf = build_features(history[ix], meta.flow_group_id.iloc[ix].tolist(), cfg["flow_group_ids"], L)
        calibrate(rf_predict(model, xf, H), cfg["epsilon"])
        latencies.append((time.perf_counter() - start) * 1000)
    report["latency"] = {
        "scope": "48-flow feature+RF+calibration, model already loaded; excludes transport",
        "batches": len(latencies), "p50_ms": float(np.percentile(latencies, 50)),
        "p95_ms": float(np.percentile(latencies, 95)), "max_ms": max(latencies),
        "platform": platform.platform(), "processor": platform.processor(),
        "rf_native_coverage": 1.0, "fallback_count": 0,
        "note": "Offline evaluation aborts on RF failure; online fallback reported separately.",
    }
    out = new_directory(out)
    write_json(out / "evaluation.json", report)
    # ML-local audit, not the seven-field online wire payload.
    np.savez_compressed(out / "predictions.npz", sample_ids=meta.sample_id.to_numpy(dtype=str),
                        y_vector=y, rf_raw=raw, **pred)
    meta.to_csv(out / "sample_metadata.csv", index=False)
    (out / "REPORT.md").write_text(readable_report(report), encoding="utf-8")
    return report


def readable_report(report):
    """Human-readable companion to the machine-readable evaluation, no tuning."""
    cfg = report["selected_config"]
    names = {"rf": "Random Forest", "persistence": "Persistence", "moving_average": "Moving Average"}
    lines = [
        "# Kết quả dự báo ML trên tập test\n",
        f"Nguồn dữ liệu: **{report['data_provenance']['kind']}**. "
        "Đây là đánh giá dự báo offline, chưa phải kết quả benchmark mạng Mininet/OVS.\n",
        f"Cấu hình đã chọn bằng validation: **L={cfg['lookback_L']}, H={cfg['horizon_H']}, "
        f"epsilon={cfg['epsilon']}, W={cfg['ma_window']}**. Test không được dùng để chọn lại cấu hình.\n",
        f"RF học trên {report['fit_sample_count']:,}/{report['train_sample_count']:,} cửa sổ train. "
        f"Test có {report['run_count']} runs, seeds {', '.join(report['test_seeds'])}; "
        "các mô hình dùng cùng mẫu và horizon.\n",
        "MAE là sai số tuyệt đối trung bình; RMSE nhấn mạnh lỗi lớn. Đơn vị Mbps, thấp hơn tốt hơn. "
        "Bias âm nghĩa là dự báo thấp hơn nhu cầu.\n",
        "| Mô hình | MAE | RMSE | Bias | Tỷ lệ dự báo thấp |",
        "|---|---:|---:|---:|---:|",
    ]
    for name, score in report["metrics"].items():
        lines.append(f"| {names[name]} | {score['pooled_mae_mbps']:.6f} | "
                     f"{score['pooled_rmse_mbps']:.6f} | {score['bias_mbps']:.6f} | "
                     f"{score['underprediction_fraction']:.2%} |")
    rf_mae = report["metrics"]["rf"]["pooled_mae_mbps"]
    lines += ["", "So sánh trên test chỉ để báo cáo; không tự thay mô hình đã chọn:", ""]
    for name in ("persistence", "moving_average"):
        baseline = report["metrics"][name]["pooled_mae_mbps"]
        delta = baseline - rf_mae
        relation = "thấp hơn" if delta > 0 else "cao hơn" if delta < 0 else "bằng"
        lines.append(f"- MAE của RF {relation} {names[name]}: chênh lệch {abs(delta):.6f} Mbps.")
    lines += ["", "## Theo scenario", "",
              "| Scenario | RF MAE | Persistence MAE | MA MAE |",
              "|---|---:|---:|---:|"]
    for scenario, scores in report["by_scenario"].items():
        lines.append(f"| {scenario} | " + " | ".join(
            f"{scores[name]['pooled_mae_mbps']:.6f}" for name in names) + " |")
    lines += ["", "## Theo bước dự báo", "",
              "Bước 1 là bin ngay tại cutoff; bước H là bin cuối của vector dự báo.", "",
              "| Bước | RF MAE | Persistence MAE | MA MAE |", "|---|---:|---:|---:|"]
    for step in range(cfg["horizon_H"]):
        lines.append(f"| {step + 1} | " + " | ".join(
            f"{report['metrics'][name]['per_step_mae_mbps'][step]:.6f}" for name in names) + " |")
    latency = report["latency"]
    lines += ["", "## Latency và cách đọc các file", "",
              f"Đo {latency['batches']} batch × 48 flow: p50={latency['p50_ms']:.3f} ms, "
              f"p95={latency['p95_ms']:.3f} ms. Chỉ gồm tạo feature + RF + epsilon khi model đã nạp; "
              "chưa gồm Data, snapshot validation, transport và SDN.", "",
              "- `evaluation.json`: chi tiết pooled, từng bước, scenario, run/seed, flow và provenance.",
              "- `predictions.npz`: nhãn thật và dự báo để Testing tính lại sai số.",
              "- `sample_metadata.csv`: run/flow/cutoff ứng với từng sample_id trong NPZ.",
              "- `../model/ml_config.json`: L/H/epsilon/W và feature order đã chọn.",
              "- `../model/MODEL_CARD.md`: mô tả mô hình và giới hạn.", "",
              "Các cửa sổ cùng run có tương quan; không coi từng cửa sổ là một lần chạy độc lập. "
              "Sai số dự báo thấp hơn chưa chứng minh giảm nghẽn, packet loss hoặc tăng goodput.", ""]
    return "\n".join(lines)
