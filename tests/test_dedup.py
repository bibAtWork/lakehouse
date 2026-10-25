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
    def test_historical_cohort_median_resists_small_area_outlier(self):
        rows = [
            row(1, price="400000", living_area_m2="100", city="A",
                first_seen_at="2026-02-01T10:00:00Z"),
            row(2, price="500000", living_area_m2="100", city="B",
                first_seen_at="2026-02-02T10:00:00Z"),
            row(3, price="900000", living_area_m2="10", city="C",
                first_seen_at="2026-02-03T10:00:00Z"),
        ]
        result = dedup.summary(dedup.deduplicate(rows), as_of="2026-10-25T00:00:00Z")
        self.assertEqual(result["average_price_per_sqm_by_month"], [
            {"month": "2026-02", "count": 3, "average_eur": 33000,
             "median_eur": 5000},
        ])
        self.assertEqual(result["average_price_per_sqm_by_quarter"][0]["median_eur"], 5000)

    def test_given_manual_history_when_summarizing_then_current_inventory_stays_current(self):
        archive = row(1, first_seen_at="2026-01-02T10:00:00Z", manual_import=True,
                      price="300000", living_area_m2="100")
        current = row(2, first_seen_at="2026-10-19T10:00:00Z", manual_import=False,
                      title="Another", price="600000", living_area_m2="100")
        active, history = dedup.split_current_and_history([archive, current])
        current_kpis = dedup.summary(dedup.deduplicate(active))
        self.assertEqual((current_kpis["source_listings"], current_kpis["unique_properties"]),
                         (1, 1))
        cohorts = dedup.summary(dedup.deduplicate(history))
        self.assertEqual((cohorts["source_listings"], cohorts["unique_properties"]), (2, 2))
        self.assertEqual(cohorts["average_price_per_sqm_eur"], 4500)
        self.assertEqual([part["month"] for part in cohorts["average_price_per_sqm_by_month"]],
                         ["2026-01", "2026-10"])

    def test_given_a_cohort_disappears_when_gold_refreshes_then_it_gets_a_zero_sample(self):
        previous = {"average_price_per_sqm_by_plz3_quarter": [
            {"plz3": "751", "quarter": "2026-Q4", "average_eur": 4957},
            {"plz3": "727", "quarter": "2026-Q4", "average_eur": 3900},
        ]}
        current = {"average_price_per_sqm_by_plz3_quarter": [
            {"plz3": "727", "quarter": "2026-Q4", "average_eur": 4000},
        ]}
        self.assertEqual(dedup.removed_cohorts(previous, current,
            "average_price_per_sqm_by_plz3_quarter", ("plz3", "quarter")),
            [previous["average_price_per_sqm_by_plz3_quarter"][0]])

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
        result["llm_successful_calls"] = 3
        result["llm_failed_calls"] = 1
        self.assertEqual((dedup.summary(result)["llm_successful_calls"],
                          dedup.summary(result)["llm_failed_calls"]), (3, 1))

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

    def test_metabase_style_price_and_release_cohorts_use_unique_properties(self):
        rows = [
            row(1, price="300000", living_area_m2="100", postal_code="72764",
                first_seen_at="2026-09-10T10:00:00Z"),
            row(2, price="300000", living_area_m2="100", postal_code="72764",
                first_seen_at="2026-10-19T10:00:00Z", provider="sparkasse"),
            row(3, price="600000", living_area_m2="100", postal_code="72070",
                title="Other", city="Tübingen", first_seen_at="2026-10-19T12:00:00Z"),
            row(4, price="1820000", living_area_m2="1", postal_code="72764",
                title="Bad area", first_seen_at="2026-10-19T13:00:00Z"),
        ]
        dataset = dedup.deduplicate(rows)
        self.assertEqual(dataset["unique_count"], 3)
        self.assertEqual(next(p for p in dataset["listings"] if p["group_id"] == 1)
                         ["first_seen_at"], "2026-09-10T10:00:00Z")
        result = dedup.summary(dataset, as_of="2026-10-20T04:00:00Z")
        self.assertEqual(result["average_price_per_sqm_eur"], 4500)
        self.assertEqual(result["new_properties_24h"], 2)
        self.assertEqual(result["price_per_sqm_implausible_area_properties"], 1)
        self.assertEqual(result["average_price_per_sqm_by_plz3"], [
            {"plz3": "720", "count": 1, "average_eur": 6000, "median_eur": 6000},
            {"plz3": "727", "count": 1, "average_eur": 3000, "median_eur": 3000},
        ])
        self.assertEqual(result["average_price_per_sqm_by_month"], [
            {"month": "2026-09", "count": 1, "average_eur": 3000, "median_eur": 3000},
            {"month": "2026-10", "count": 1, "average_eur": 6000, "median_eur": 6000},
        ])
        self.assertEqual(result["average_price_per_sqm_by_quarter"], [
            {"quarter": "2026-Q3", "count": 1, "average_eur": 3000, "median_eur": 3000},
            {"quarter": "2026-Q4", "count": 1, "average_eur": 6000, "median_eur": 6000},
        ])
        self.assertEqual(result["average_price_per_sqm_by_plz3_quarter"], [
            {"plz3": "720", "quarter": "2026-Q4", "count": 1, "average_eur": 6000, "median_eur": 6000},
            {"plz3": "727", "quarter": "2026-Q3", "count": 1, "average_eur": 3000, "median_eur": 3000},
        ])
        self.assertEqual(result["average_price_per_sqm_by_plz3_month"][0]["month"], "2026-10")

    def test_recent_house_price_excludes_apartments_and_old_listings(self):
        rows = [
            row(1, title="Detached", property_type="detached_house", price="400000",
                first_seen_at="2026-10-01T00:00:00Z"),
            row(2, title="Apartment", property_type="apartment", price="200000",
                first_seen_at="2026-10-01T00:00:00Z"),
            row(3, title="Old house", property_type="detached_house", price="900000",
                first_seen_at="2025-01-01T00:00:00Z"),
        ]
        result = dedup.summary(dedup.deduplicate(rows), as_of="2026-10-25T00:00:00Z")
        self.assertEqual(result["average_asking_house_price_last_12m_eur"], 400000)
        self.assertEqual(result["asking_house_price_last_12m_properties"], 1)
        self.assertEqual(result["classified_properties"], 3)
        self.assertEqual(result["classified_house_properties"], 2)
        self.assertEqual(result["average_asking_price_by_plz3_last_12m"][0]["count"], 2)

    def test_monthly_median_uses_deduplicated_properties(self):
        rows = [
            row(1, price="100000", living_area_m2="100", first_seen_at="2026-10-01T00:00:00Z"),
            row(2, price="100000", living_area_m2="100", first_seen_at="2026-10-02T00:00:00Z",
                provider="sparkasse"),
            row(3, title="Second", price="200000", living_area_m2="100",
                first_seen_at="2026-10-03T00:00:00Z"),
            row(4, title="Third", price="900000", living_area_m2="100",
                first_seen_at="2026-10-04T00:00:00Z"),
        ]
        result = dedup.summary(dedup.deduplicate(rows), as_of="2026-10-25T00:00:00Z")
        monthly = result["average_price_per_sqm_by_month"][0]
        self.assertEqual((monthly["count"], monthly["median_eur"]), (3, 2000))
        self.assertEqual(result["average_price_per_sqm_by_plz3_month"][0]["median_eur"], 2000)

    def test_postal_region_baskets_use_one_property_per_listing_group(self):
        rows = [
            row(1, postal_code="72764", price="300000", living_area_m2="100",
                first_seen_at="2026-10-01T00:00:00Z"),
            row(2, postal_code="72070", title="Tübingen", price="500000",
                living_area_m2="100", first_seen_at="2026-10-02T00:00:00Z"),
            row(3, postal_code="71034", title="Böblingen", price="700000",
                living_area_m2="100", first_seen_at="2026-10-03T00:00:00Z"),
        ]
        result = dedup.summary(dedup.deduplicate(rows), as_of="2026-10-25T00:00:00Z")
        baskets = {item["region"]: item for item in result["price_per_sqm_by_region_month"]}
        self.assertEqual(baskets["Reutlingen"]["median_eur"], 3000)
        self.assertEqual(baskets["Tübingen"]["median_eur"], 5000)
        self.assertEqual(baskets["Stuttgarter Speckgürtel"]["median_eur"], 7000)
        self.assertEqual(len(result["price_per_sqm_by_region_quarter"]), 3)


if __name__ == "__main__":
    unittest.main()
