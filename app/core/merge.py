from pathlib import Path
import csv


def merge_job_results(job_id: str) -> Path:
    """
    Merge measured (train.csv) and predicted (predictions.csv)
    into a single ordered final.csv for a job.

    Order: by d_along ascending
    """

    job_dir = Path("data") / job_id

    train_path = job_dir / "train.csv"
    predict_path = job_dir / "predict.csv"
    predictions_path = job_dir / "predictions.csv"
    final_path = job_dir / "final.csv"

    if not train_path.exists():
        raise FileNotFoundError(train_path)

    if not predict_path.exists():
        raise FileNotFoundError(predict_path)

    if not predictions_path.exists():
        raise FileNotFoundError(predictions_path)

    merged_rows = []

    # -----------------------------
    # 1. Load measured data
    # -----------------------------
    with open(train_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            merged_rows.append({
                "x": float(r["x"]),
                "y": float(r["y"]),
                "d_along": float(r["d_along"]),
                "value": float(r["value"]),
                "source": "measured",
            })

    # -----------------------------
    # 2. Load generated geometry
    # -----------------------------
    predict_rows = []
    with open(predict_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            predict_rows.append({
                "x": float(r["x"]),
                "y": float(r["y"]),
                "d_along": float(r["d_along"]),
            })

    # -----------------------------
    # 3. Load predictions
    # -----------------------------
    predictions = []
    with open(predictions_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            predictions.append(float(r["predicted_value"]))

    if len(predict_rows) != len(predictions):
        raise ValueError("predict.csv and predictions.csv row count mismatch")

    # -----------------------------
    # 4. Attach predictions
    # -----------------------------
    for geom, pred in zip(predict_rows, predictions):
        merged_rows.append({
            "x": geom["x"],
            "y": geom["y"],
            "d_along": geom["d_along"],
            "value": pred,
            "source": "predicted",
        })

    # -----------------------------
    # 5. Sort by d_along
    # -----------------------------
    merged_rows.sort(key=lambda r: r["d_along"])

    # -----------------------------
    # 6. Write final.csv
    # -----------------------------
    with open(final_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["x", "y", "d_along", "value", "source"],
        )
        writer.writeheader()
        writer.writerows(merged_rows)

    return final_path
