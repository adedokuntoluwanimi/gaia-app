# app/core/geometry.py
from typing import List, Dict, Optional
from math import hypot

TOLERANCE = 1e-3  # meters

Station = Dict[str, Optional[float]]


def order_points_along_traverse(points):
    if len(points) < 2:
        return points

    xs = [p["x"] for p in points]
    ys = [p["y"] for p in points]

    dx = max(xs) - min(xs)
    dy = max(ys) - min(ys)

    if dx >= dy:
        return sorted(points, key=lambda p: p["x"])
    return sorted(points, key=lambda p: p["y"])


def compute_cumulative_distance(points):
    if not points:
        return []

    cumulative = []
    d = 0.0

    prev = points[0]
    cumulative.append({**prev, "d_along": 0.0})

    for p in points[1:]:
        d += hypot(p["x"] - prev["x"], p["y"] - prev["y"])
        cumulative.append({**p, "d_along": d})
        prev = p

    return cumulative


def interpolate_point(p1, p2, target_d):
    d1 = p1["d_along"]
    d2 = p2["d_along"]

    if abs(d2 - d1) < TOLERANCE:
        return p1["x"], p1["y"]

    ratio = (target_d - d1) / (d2 - d1)
    x = p1["x"] + ratio * (p2["x"] - p1["x"])
    y = p1["y"] + ratio * (p2["y"] - p1["y"])

    return x, y


def generate_target_stations(points_with_distance, spacing):
    if not points_with_distance:
        return []

    total_length = points_with_distance[-1]["d_along"]

    targets = []
    current_d = 0.0
    i = 0

    while current_d < total_length:
        while (
            i < len(points_with_distance) - 2
            and points_with_distance[i + 1]["d_along"] < current_d
        ):
            i += 1

        p1 = points_with_distance[i]
        p2 = points_with_distance[i + 1]

        x, y = interpolate_point(p1, p2, current_d)

        targets.append({
            "x": x,
            "y": y,
            "d_along": current_d,
        })

        current_d += spacing

    # Always include last measured point
    last = points_with_distance[-1]
    targets.append({
        "x": last["x"],
        "y": last["y"],
        "d_along": last["d_along"],
    })

    return targets


def classify_stations(stations, measured_points):
    canonical = []

    for idx, s in enumerate(stations):
        matched = None

        for m in measured_points:
            if abs(m["d_along"] - s["d_along"]) <= TOLERANCE:
                matched = m
                break

        canonical.append({
            "station_index": idx,
            "x": s["x"],
            "y": s["y"],
            "d_along": s["d_along"],
            "measured": 1 if matched else 0,
            "value": matched["value"] if matched else None,
        })

    return canonical


def build_canonical_stations_sparse(
    measured_points: List[Dict[str, float]],
    spacing: float,
) -> List[Station]:
    ordered = order_points_along_traverse(measured_points)
    with_distance = compute_cumulative_distance(ordered)
    stations = generate_target_stations(with_distance, spacing)
    return classify_stations(stations, with_distance)


def split_train_predict(canonical_stations):
    train = []
    predict = []

    for s in canonical_stations:
        row = {
            "station_index": s["station_index"],
            "x": s["x"],
            "y": s["y"],
            "d_along": s["d_along"],
            "value": s["value"],
        }

        if s["measured"] == 1:
            train.append(row)
        else:
            predict.append(row)

    return train, predict
