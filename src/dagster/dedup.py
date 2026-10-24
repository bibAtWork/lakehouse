"""Deterministic cross-portal property matching over screener source records."""

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from itertools import combinations
from statistics import mean, median
import re
import unicodedata


# A 1 m² "living area" on a million-euro home is a source-data error, not a
# useful asking-price-per-m² observation. Keep the listing in Silver.
MIN_PRICE_PER_SQM_AREA_M2 = 10


def _text(value):
    if value is None:
        return None
    german = str(value).casefold().translate(str.maketrans({
        "ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss",
    }))
    folded = unicodedata.normalize("NFKD", german)
    result = "".join(char for char in folded if char.isalnum() and not unicodedata.combining(char))
    return result or None


def _street(value):
    street = _text(value)
    return street + "asse" if street and street.endswith("str") else street


_FULL_ADDRESS = re.compile(
    r"^\s*(?P<street>[^,\d]+?)\s+(?P<number>\d+\s*[a-zA-Z]?)\s*,\s*"
    r"(?P<postal>\d{5})\s+(?P<city>[^,]+)(?:,|$)"
)


def clean_record(row):
    """Fill structured address gaps only from an unambiguous German full address."""
    result = row.copy()
    for field in ("title", "city", "postal_code", "street", "house_number"):
        if isinstance(result.get(field), str):
            result[field] = result[field].strip() or None
    match = _FULL_ADDRESS.match(result.get("address_text") or "")
    if match and (result.get("postal_code") is None or
                  _text(result["postal_code"]) == _text(match["postal"])) and (
                      result.get("city") is None or _text(result["city"]) == _text(match["city"])):
        result["street"] = result.get("street") or match["street"].strip()
        result["house_number"] = result.get("house_number") or match["number"].replace(" ", "")
        result["postal_code"] = result.get("postal_code") or match["postal"]
        result["city"] = result.get("city") or match["city"].strip()
    return result


def _number(value):
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def _timestamp(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except ValueError:
        return None


def _place(left, right):
    city_left, city_right = _text(left.get("city")), _text(right.get("city"))
    post_left, post_right = _text(left.get("postal_code")), _text(right.get("postal_code"))
    if city_left and city_right and city_left != city_right:
        return False
    if post_left and post_right and post_left != post_right:
        return False
    return bool((city_left and city_left == city_right) or
                (post_left and post_left == post_right))


def match_reason(left, right):
    """Exact price AND full logical address OR at least three populated equal attributes."""
    price = _number(left.get("price"))
    if price is None or price != _number(right.get("price")):
        return None
    if all(normalizer(left.get(field)) is not None and
           normalizer(left.get(field)) == normalizer(right.get(field))
           for field, normalizer in (("city", _text), ("street", _street),
                                     ("house_number", _text))):
        return "address"
    equal = (
        _text(left.get("title")) is not None and _text(left.get("title")) == _text(right.get("title")),
        _number(left.get("living_area_m2")) is not None and
        _number(left.get("living_area_m2")) == _number(right.get("living_area_m2")),
        _number(left.get("plot_area_m2")) is not None and
        _number(left.get("plot_area_m2")) == _number(right.get("plot_area_m2")),
        _place(left, right),
        _number(left.get("build_year")) is not None and
        _number(left.get("build_year")) == _number(right.get("build_year")),
    )
    return "attributes" if sum(equal) >= 3 else None


def _tokens(row):
    price = _number(row.get("price"))
    if price is None:
        return []
    city, street, house = (_text(row.get("city")), _street(row.get("street")),
                           _text(row.get("house_number")))
    tokens = [(price, "address", city, street, house)] if all((city, street, house)) else []
    attributes = {
        "title": [_text(row.get("title"))],
        "living": [_number(row.get("living_area_m2"))],
        "plot": [_number(row.get("plot_area_m2"))],
        "place": [_text(row.get("city")), _text(row.get("postal_code"))],
        "year": [_number(row.get("build_year"))],
    }
    present = {key: set(value) - {None} for key, value in attributes.items()}
    for keys in combinations((key for key in attributes if present[key]), 3):
        for first in present[keys[0]]:
            for second in present[keys[1]]:
                for third in present[keys[2]]:
                    tokens.append((price, "attributes", keys, first, second, third))
    return tokens


def deduplicate(rows):
    rows = [clean_record(row) for row in rows]
    parents = list(range(len(rows)))

    def root(index):
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    seen = defaultdict(list)
    evidence = []
    for index, row in enumerate(rows):
        for token in _tokens(row):
            for previous in seen[token]:
                if root(previous) == root(index):
                    continue
                reason = match_reason(rows[previous], row)
                if reason:
                    parents[root(index)] = root(previous)
                    evidence.append({"left": rows[previous]["listing_id"],
                                     "right": row["listing_id"], "reason": reason})
            # Full address equality is already sufficient; one representative is enough.
            if token[1] == "address" and seen[token]:
                continue
            seen[token].append(index)
    groups = defaultdict(list)
    for index, row in enumerate(rows):
        groups[root(index)].append(row)
    properties = []
    for members in groups.values():
        def priority(row):
            complete = sum(row.get(field) not in (None, "") for field in
                           ("title", "living_area_m2", "plot_area_m2", "city",
                            "postal_code", "street", "house_number", "build_year"))
            return (complete, row.get("last_detail_success_at") or "", -int(row["listing_id"]))
        canonical = max(members, key=priority).copy()
        first_seen = [(seen, row["first_seen_at"]) for row in members
                      if (seen := _timestamp(row.get("first_seen_at")))]
        if first_seen:
            canonical["first_seen_at"] = min(first_seen)[1]
        canonical["group_id"] = min(int(row["listing_id"]) for row in members)
        canonical["duplicate_count"] = len(members) - 1
        canonical["sources"] = [{field: row.get(field) for field in
                                 ("listing_id", "provider", "source_id", "url")}
                                for row in members]
        properties.append(canonical)
    return {"listings": properties, "match_evidence": evidence,
            "source_count": len(rows), "unique_count": len(properties),
            "duplicate_count": len(rows) - len(properties)}


def split_current_and_history(rows):
    """Imported Fredy rows contribute to cohorts, never to active inventory KPIs."""
    return [row for row in rows if not row.get("manual_import")], rows


def removed_cohorts(previous, current, group, labels):
    """Find old Gold series that need a zero sample when dedup changes a cohort."""
    latest = {tuple(row[name] for name in labels) for row in current.get(group, [])}
    return [row for row in (previous or {}).get(group, [])
            if tuple(row[name] for name in labels) not in latest]


def summary(dataset, as_of=None):
    properties = dataset["listings"]
    now = _timestamp(as_of) if as_of else datetime.now(timezone.utc)
    if now is None:
        raise ValueError("as_of must be an ISO 8601 timestamp with timezone")
    priced = [row for row in properties if _number(row.get("price")) is not None]
    per_sqm = [float(_number(row["price"]) / _number(row["living_area_m2"]))
               for row in priced if _number(row.get("living_area_m2")) and
               _number(row["living_area_m2"]) >= MIN_PRICE_PER_SQM_AREA_M2 and
               _number(row["price"]) > 0]
    implausible_area = sum(0 < _number(row.get("living_area_m2")) < MIN_PRICE_PER_SQM_AREA_M2
                           for row in priced if _number(row.get("living_area_m2")) is not None)
    by_plz3 = defaultdict(list)
    by_month = defaultdict(list)
    by_quarter = defaultdict(list)
    by_plz3_quarter = defaultdict(list)
    new_properties_24h = 0
    for row in properties:
        seen = _timestamp(row.get("first_seen_at"))
        if seen and now - timedelta(days=1) <= seen <= now:
            new_properties_24h += 1
        price, area = _number(row.get("price")), _number(row.get("living_area_m2"))
        if price is None or area is None or price <= 0 or area < MIN_PRICE_PER_SQM_AREA_M2:
            continue
        value = float(price / area)
        postal = str(row.get("postal_code") or "")
        plz3 = postal[:3] if re.fullmatch(r"\d{5}", postal) else None
        if plz3:
            by_plz3[plz3].append(value)
        if seen:
            month = seen.strftime("%Y-%m")
            quarter = f"{seen.year}-Q{(seen.month - 1) // 3 + 1}"
            by_month[month].append(value)
            by_quarter[quarter].append(value)
            if plz3:
                by_plz3_quarter[(plz3, quarter)].append(value)

    def grouped(groups, labels):
        return [{**dict(zip(labels, key if isinstance(key, tuple) else (key,))),
                 "count": len(values), "average_eur": float(mean(values))}
                for key, values in sorted(groups.items())]
    cities = Counter((row.get("city") or "Unknown").strip() for row in properties)
    providers = Counter(source.get("provider") or "Unknown" for row in properties
                        for source in row["sources"])
    match_reasons = Counter(edge["reason"] for edge in dataset["match_evidence"])
    return {
        "source_listings": dataset["source_count"],
        "unique_properties": dataset["unique_count"],
        "duplicate_listings": dataset["duplicate_count"],
        "duplicate_rate": dataset["duplicate_count"] / dataset["source_count"]
        if dataset["source_count"] else 0,
        "llm_quota_skipped_properties": dataset.get("llm_quota_skipped_properties", 0),
        "llm_budget_skipped_properties": dataset.get("llm_budget_skipped_properties", 0),
        "llm_time_skipped_properties": dataset.get("llm_time_skipped_properties", 0),
        "llm_cache_pending_properties": dataset.get("llm_cache_pending_properties", 0),
        "llm_eligible_properties": dataset.get("llm_eligible_properties", 0),
        "llm_cache_hits": dataset.get("llm_cache_hits", 0),
        "llm_successful_calls": dataset.get("llm_successful_calls", 0),
        "llm_failed_calls": dataset.get("llm_failed_calls", 0),
        "llm_completed_properties": dataset.get("llm_completed_properties", 0),
        "llm_accepted_fact_properties": dataset.get("llm_accepted_fact_properties", 0),
        "priced_properties": len(priced),
        "median_price_eur": float(median([_number(row["price"]) for row in priced])) if priced else None,
        "median_price_per_sqm_eur": float(median(per_sqm)) if per_sqm else None,
        "average_price_per_sqm_eur": float(mean(per_sqm)) if per_sqm else None,
        "price_per_sqm_implausible_area_properties": implausible_area,
        "new_properties_24h": new_properties_24h,
        "average_price_per_sqm_by_plz3": grouped(by_plz3, ("plz3",)),
        "average_price_per_sqm_by_month": grouped(by_month, ("month",)),
        "average_price_per_sqm_by_quarter": grouped(by_quarter, ("quarter",)),
        "average_price_per_sqm_by_plz3_quarter": grouped(by_plz3_quarter, ("plz3", "quarter")),
        "by_city": [{"city": city, "count": count} for city, count in cities.most_common(10)],
        "sources_by_provider": [{"provider": provider, "count": count}
                                for provider, count in providers.most_common()],
        "matches_by_reason": dict(match_reasons),
    }
