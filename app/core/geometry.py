# app/core/geometry.py
from typing import List, Dict, Optional
from math import hypot

TOLERANCE = 1e-3  # meters

Station = Dict[str, Optional[float]]

def order_points_along_traverse(points):
    """
    Orders points along the dominant traverse direction.

    points: [{x, y, value}, ...]

    Returns:
        Ordered list of points
    """
    if len(points) < 2:
        return points

    xs = [p["x"] for p in points]
    ys = [p["y"] for p in points]

    dx = max(xs) - min(xs)
    dy = max(ys) - min(ys)

    if dx >= dy:
        return sorted(points, key=lambda p: p["x"])
    else:
        return sorted(points, key=lambda p: p["y"])

def compute_cumulative_distance(points):
    """
    Adds cumulative distance (d_along) to ordered points.

    points: ordered [{x, y, value}, ...]

    Returns:
        List of dicts with d_along added
    """
    if not points:
        return []

    cumulative = []
    d = 0.0

    prev = points[0]
    cumulative.append({
        **prev,
        "d_along": 0.0
    })

    for p in points[1:]:
        segment = hypot(p["x"] - prev["x"], p["y"] - prev["y"])
        d += segment
        cumulative.append({
            **p,
            "d_along": d
        })
        prev = p

    return cumulative

def interpolate_point(p1, p2, target_d):
    """
    Linearly interpolate a point between p1 and p2
    for a given target distance along the traverse.

    p1, p2: consecutive points with d_along
    target_d: desired d_along between p1 and p2

    Returns:
        (x, y)
    """
    d1 = p1["d_along"]
    d2 = p2["d_along"]

    if d2 == d1:
        return p1["x"], p1["y"]

    ratio = (target_d - d1) / (d2 - d1)

    x = p1["x"] + ratio * (p2["x"] - p1["x"])
    y = p1["y"] + ratio * (p2["y"] - p1["y"])

    return x, y

def generate_target_stations(points_with_distance, spacing):
    """
    Generates stations at fixed spacing along the traverse.

    points_with_distance: ordered measured points with d_along
    spacing: desired station spacing

    Returns:
        List of dicts: {x, y, d_along}
    """
    if not points_with_distance:
        return []

    total_length = points_with_distance[-1]["d_along"]

    targets = []
    current_d = 0.0
    i = 0

    while current_d <= total_length:
        # Advance to the segment containing current_d
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
            "d_along": current_d
        })

        current_d += spacing

    return targets

def index_measured_by_distance(points_with_distance):
    """
    Creates a lookup of measured points keyed by d_along.

    points_with_distance: measured points with d_along

    Returns:
        List of measured points with d_along
    """
    return points_with_distance
def classify_stations(stations, measured_points):
    """
    Classifies stations as measured or predicted
    and assigns a sequential station_index.
    """
    canonical = []

    for idx, s in enumerate(stations):
        matched = None

        for m in measured_points:
            if abs(m["d_along"] - s["d_along"]) <= TOLERANCE:
                matched = m
                break

        if matched:
            canonical.append({
                "station_index": idx,
                "x": s["x"],
                "y": s["y"],
                "d_along": s["d_along"],
                "measured": 1,
                "value": matched["value"],
            })
        else:
            canonical.append({
                "station_index": idx,
                "x": s["x"],
                "y": s["y"],
                "d_along": s["d_along"],
                "measured": 0,
                "value": None,
            })

    return canonical


def build_canonical_stations_sparse(
    measured_points: List[Dict[str, float]],
    spacing: float,
) -> List[Station]:
    """
    measured_points: [{x, y, value}, ...] for one traverse
    spacing: desired station spacing in meters

    Returns:
        Ordered list of canonical station records
    """
    ordered = order_points_along_traverse(measured_points)
    with_distance = compute_cumulative_distance(ordered)
    stations = generate_target_stations(with_distance, spacing)
    canonical = classify_stations(stations, with_distance)
    return canonical

def split_train_predict(canonical_stations):
    """
    Splits canonical stations into train and predict sets.

    canonical_stations: list of canonical station dicts

    Returns:
        (train, predict)
    """
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



