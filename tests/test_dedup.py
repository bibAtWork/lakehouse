import importlib.util
from pathlib import Path
import unittest


module_path = Path(__file__).resolve().parents[1] / "src" / "dagster" / "dedup.py"
spec = importlib.util.spec_from_file_location("dedup", module_path)
dedup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dedup)


def row(listing_id, **changes):
    result = {"listing_id": listing_id, "provider": "immoscout", "price": "400000.00",
              "title": "Haus am Park", "living_area_m2": "120", "plot_area_m2": "500",
              "city": "Waldenbuch", "postal_code": "71111", "build_year": 1990}
    result.update(changes)
    return result


class DedupTests(unittest.TestCase):
    def test_full_logical_address_and_exact_price(self):
        left = row(1, street="Hauptstraße", house_number="12 A", title=None)
        right = row(2, street="Hauptstrasse", house_number="12a", title=None,
                    living_area_m2=None, plot_area_m2=None, build_year=None)
        self.assertEqual(dedup.match_reason(left, right), "address")
        self.assertIsNone(dedup.match_reason(left, {**right, "price": "399999"}))
        self.assertEqual(dedup.match_reason(
            row(3, city="Düsseldorf", street="Hauptstr.", house_number="12"),
            row(4, city="Duesseldorf", street="Hauptstraße", house_number="12")),
            "address")

    def test_three_populated_attributes_and_location_conflicts(self):
        self.assertEqual(dedup.match_reason(row(1), row(2, title="Other", plot_area_m2=None)),
                         "attributes")
        self.assertIsNone(dedup.match_reason(row(1), row(2, title="Other", plot_area_m2=None,
                                                 build_year=None)))
        self.assertIsNone(dedup.match_reason(row(1), row(2, title="Other", plot_area_m2=None,
                                                 city="Berlin", postal_code="10115")))

    def test_transitive_groups_and_gold_kpis(self):
        rows = [row(1, street="Hauptstr.", house_number="3", build_year=None),
                row(2, street="Hauptstr.", house_number="3", title="Other", plot_area_m2=None),
                row(3, street=None, house_number=None, title="Other", plot_area_m2=None),
                row(4, price="450000")]
        result = dedup.deduplicate(rows)
        self.assertEqual((result["source_count"], result["unique_count"],
                          result["duplicate_count"]), (4, 2, 2))
        self.assertEqual(len(next(item for item in result["listings"]
                                  if item["group_id"] == 1)["sources"]), 3)
        self.assertEqual(dedup.summary(result)["median_price_eur"], 425000)
        result["llm_quota_skipped_properties"] = 2
        self.assertEqual(dedup.summary(result)["llm_quota_skipped_properties"], 2)

    def test_missing_price_does_not_merge(self):
        result = dedup.deduplicate([row(1, price=None), row(2, price=None)])
        self.assertEqual(result["unique_count"], 2)

    def test_silver_extracts_only_consistent_complete_addresses(self):
        raw = row(1, city="Berlin", postal_code="12247", street=None, house_number=None,
                  address_text="Brucknerstraße 49, 12247 Berlin, Lankwitz (Steglitz)")
        cleaned = dedup.clean_record(raw)
        self.assertEqual((cleaned["street"], cleaned["house_number"]),
                         ("Brucknerstraße", "49"))
        self.assertIsNone(dedup.clean_record({**raw, "postal_code": "10115"})["street"])
        self.assertIsNone(dedup.clean_record({**raw, "address_text": "12247 Berlin"})["street"])
        other = row(2, city="Berlin", postal_code="12247", street="Brucknerstr.",
                    house_number="49", title="Other", living_area_m2=None,
                    plot_area_m2=None, build_year=None)
        self.assertEqual(dedup.deduplicate([raw, other])["unique_count"], 1)


if __name__ == "__main__":
    unittest.main()
