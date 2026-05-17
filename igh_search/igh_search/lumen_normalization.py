import re
from dataclasses import dataclass
from typing import Dict, List, Optional


VALID_UNITS = {"lm", "lm/m", "lm/w", "lm/led", "lm/pcs", "lm/module"}
INVALID_MARKERS = {"", "na", "n/a", "null", "none", "-"}


@dataclass
class LumenParseResult:
    raw: str
    min: Optional[float]
    max: Optional[float]
    unit: Optional[str]
    values: List[float]
    status: str

    def as_dict(self) -> Dict:
        return {
            "raw": self.raw,
            "min": self.min,
            "max": self.max,
            "unit": self.unit,
            "values": self.values,
            "status": self.status,
        }


def _normalize_unit(raw: str) -> Optional[str]:
    if not raw:
        return None
    value = raw.strip().lower().replace(" ", "")
    value = value.replace("lm/watt", "lm/w")
    value = value.replace("lm\\w", "lm/w")
    if value in VALID_UNITS:
        return value
    return None


def _extract_unit(text: str) -> Optional[str]:
    patterns = [
        r"lm\s*/\s*module",
        r"lm\s*/\s*pcs",
        r"lm\s*/\s*led",
        r"lm\s*/\s*w",
        r"lm\s*/\s*m",
        r"lm\b",
    ]
    lowered = text.lower()
    for pattern in patterns:
        match = re.search(pattern, lowered)
        if match:
            return _normalize_unit(match.group(0))
    return None


def _safe_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _parse_multiplication(raw: str) -> Optional[float]:
    normalized = raw.lower().replace("×", "x").replace(" ", "")
    match = re.fullmatch(r"(\d+(?:\.\d+)?)x(\d+(?:\.\d+)?)(?:lm(?:/[^ ]+)?)?", normalized)
    if match:
        left = _safe_float(match.group(1))
        right = _safe_float(match.group(2))
        if left is not None and right is not None:
            return left * right

    match_rev = re.fullmatch(r"(\d+(?:\.\d+)?)(?:lm(?:/[^ ]+)?)?x(\d+(?:\.\d+)?)", normalized)
    if match_rev:
        left = _safe_float(match_rev.group(1))
        right = _safe_float(match_rev.group(2))
        if left is not None and right is not None:
            return left * right
    return None


def _extract_numbers(raw: str) -> List[float]:
    cleaned = raw.strip()
    cleaned = re.sub(r"(?<=\d),(?=\d)", "", cleaned)
    numbers = re.findall(r"\d+(?:\.\d+)?", cleaned)
    parsed = []
    for item in numbers:
        numeric = _safe_float(item)
        if numeric is not None:
            parsed.append(numeric)
    return parsed


def parse_lumen(raw_value) -> Dict:
    raw = "" if raw_value is None else str(raw_value).strip()
    if raw.lower() in INVALID_MARKERS:
        return LumenParseResult(raw=raw, min=None, max=None, unit=None, values=[], status="invalid").as_dict()

    unit = _extract_unit(raw)
    multiplied = _parse_multiplication(raw)
    if multiplied is not None:
        status = "parsed" if unit else "partial"
        return LumenParseResult(
            raw=raw,
            min=multiplied,
            max=multiplied,
            unit=unit,
            values=[multiplied],
            status=status,
        ).as_dict()

    values = _extract_numbers(raw)
    if not values:
        return LumenParseResult(raw=raw, min=None, max=None, unit=unit, values=[], status="invalid").as_dict()

    status = "parsed" if unit else "partial"
    if unit is None:
        status = "unsupported"

    return LumenParseResult(
        raw=raw,
        min=min(values),
        max=max(values),
        unit=unit,
        values=values,
        status=status,
    ).as_dict()


def normalize_lumen_fields(raw_value) -> Dict:
    parsed = parse_lumen(raw_value)
    return {
        "lumen_raw": parsed["raw"],
        "lumen_min": parsed["min"],
        "lumen_max": parsed["max"],
        "lumen_unit": parsed["unit"],
        "lumen_values": parsed["values"],
        "lumen_parse_status": parsed["status"],
    }


def build_lumen_overlap_filter(lumen_unit: str, lumen_min, lumen_max) -> Optional[str]:
    unit = _normalize_unit(lumen_unit or "")
    if not unit:
        return None

    min_value = _safe_float(str(lumen_min)) if lumen_min not in (None, "") else None
    max_value = _safe_float(str(lumen_max)) if lumen_max not in (None, "") else None
    if min_value is None and max_value is None:
        return None

    low = min_value if min_value is not None else 0.0
    high = max_value if max_value is not None else 1_000_000_000.0
    if high < low:
        low, high = high, low

    return f'(lumen_unit:="{unit}" && lumen_min:<={high} && lumen_max:>={low})'
