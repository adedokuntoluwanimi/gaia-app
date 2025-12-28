from typing import List, Dict
from math import hypot


def merge_measured_and_generated(
    measured_rows: List[Dict],
    generated_rows: List[Dict],
) -> List[Dict]:
    """
    Merge measured (train) rows with generated (predicted) rows.

    Assumptions:
    - All rows contain x, y, value
    - measured_rows are ground truth
    - generated_rows contain inferred values
    """

    output = []

    # ----------------------------------
    # Add measured rows
    # ----------------------------------
    for r in measured_rows:
        row = dict(r)
        row["measured"] = 1
        row["d_nearest"] = 0.0
        row["d_along"] = None
        output.append(row)

    # ----------------------------------
    # Add generated rows
    # ----------------------------------
    for r in generated_rows:
        gx = float(r["x"])
        gy = float(r["y"])

        nearest_dist = min(
            hypot(
                gx - float(m["x"]),
                gy - float(m["y"]),
            )
            for m in measured_rows
        )

        row = dict(r)
        row["measured"] = 0
        row["d_nearest"] = nearest_dist
        row["d_along"] = None
        output.append(row)

    # ----------------------------------
    # Optional: sort by x then y
    # ----------------------------------
    output.sort(
        key=lambda r: (float(r["x"]), float(r["y"]))
    )

    return output
