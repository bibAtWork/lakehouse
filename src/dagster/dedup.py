"""Deterministic cross-portal property matching over screener source records."""

from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from itertools import combinations
from statistics import median
import unicodedata


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


def _number(value):
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
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
        canonical["group_id"] = min(int(row["listing_id"]) for row in members)
        canonical["duplicate_count"] = len(members) - 1
        canonical["sources"] = [{field: row.get(field) for field in
                                 ("listing_id", "provider", "source_id", "url")}
                                for row in members]
        properties.append(canonical)
    return {"listings": properties, "match_evidence": evidence,
            "source_count": len(rows), "unique_count": len(properties),
            "duplicate_count": len(rows) - len(properties)}


def summary(dataset):
    properties = dataset["listings"]
    priced = [row for row in properties if _number(row.get("price")) is not None]
    per_sqm = [float(_number(row["price"]) / _number(row["living_area_m2"]))
               for row in priced if _number(row.get("living_area_m2")) and
               _number(row["living_area_m2"]) > 0]
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
        "priced_properties": len(priced),
        "median_price_eur": float(median([_number(row["price"]) for row in priced])) if priced else None,
        "median_price_per_sqm_eur": float(median(per_sqm)) if per_sqm else None,
        "by_city": [{"city": city, "count": count} for city, count in cities.most_common(10)],
        "sources_by_provider": [{"provider": provider, "count": count}
                                for provider, count in providers.most_common()],
        "matches_by_reason": dict(match_reasons),
    }
