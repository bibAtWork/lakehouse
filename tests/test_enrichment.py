import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.error import HTTPError


module_path = Path(__file__).resolve().parents[1] / "src" / "dagster" / "enrichment.py"
spec = importlib.util.spec_from_file_location("enrichment", module_path)
enrichment = importlib.util.module_from_spec(spec)
spec.loader.exec_module(enrichment)


DESCRIPTION = (
    "Objektadresse: Hauptstraße 4, 12345 Berlin. "
    "Dieses Einfamilienhaus hat Baujahr: 1998, Wohnfläche: 120 m², "
    "Grundstücksfläche: 450 m² und 5 Zimmer. Energieklasse: C. "
    "Endenergieverbrauch: 98,5 kWh/(m² a). Primärer Energieträger: Gas."
)


def fact(field, value, quote):
    return {"field": field, "value": value, "quote": quote}


class MemoryS3:
    def __init__(self):
        self.objects = {}

    def get_object(self, Bucket, Key):
        from io import BytesIO
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[(Bucket, Key)] = Body


class EnrichmentTests(unittest.TestCase):
    def test_cache_only_silver_applies_completed_results_without_calling_gemini(self):
        s3 = MemoryS3()
        row = {"description": "Baujahr: 2001"}
        calls = []
        silver = enrichment.PropertyEnricher(
            s3, "bucket", api_key="", cache_only=True,
            model_call=lambda *_: calls.append(True),
        )
        self.assertEqual(silver.enrich(row), row)
        self.assertEqual(silver.cache_pending, 1)
        llm = enrichment.PropertyEnricher(
            s3, "bucket", api_key="test",
            model_call=lambda *_: {"facts": [fact("build_year", "2001", "Baujahr: 2001")]},
        )
        llm.enrich(row)
        refreshed_silver = enrichment.PropertyEnricher(
            s3, "bucket", api_key="", cache_only=True,
            model_call=lambda *_: calls.append(True),
        )
        result = refreshed_silver.enrich(row)
        self.assertEqual(result["build_year"], 2001)
        self.assertEqual(result["llm_enrichment"]["status"], "completed")
        self.assertEqual(refreshed_silver.cache_hits, 1)
        self.assertEqual(calls, [])

    def test_quota_exhaustion_keeps_silver_available_and_reuses_cached_facts(self):
        s3 = MemoryS3()
        cached_row = {"description": DESCRIPTION, "last_detail_success_at": "2026-10-19T17:00:00Z"}
        enrichment.PropertyEnricher(
            s3, "bucket", api_key="test",
            model_call=lambda *_: {"facts": [fact("build_year", "1998", "Baujahr: 1998")]},
        ).enrich(cached_row)
        calls = []

        def quota_call(*args):
            calls.append(args)
            raise enrichment.GeminiQuotaExceeded("Gemini property extraction failed (HTTP 429)")

        enricher = enrichment.PropertyEnricher(s3, "bucket", api_key="test", model_call=quota_call)
        first = {"description": "Baujahr: 2001", "last_detail_success_at": "2026-10-20T04:00:00Z"}
        second = {"description": "Baujahr: 2002", "last_detail_success_at": "2026-10-20T04:00:00Z"}
        self.assertEqual(enricher.enrich(first), first)
        self.assertEqual(enricher.enrich(second), second)
        self.assertEqual(enricher.enrich(cached_row)["build_year"], 1998)
        self.assertEqual(len(calls), 1)
        self.assertEqual(enricher.quota_skipped, 2)
        self.assertEqual(enricher.new_calls, 1)
        retry = enrichment.PropertyEnricher(s3, "bucket", api_key="test",
            model_call=lambda *_: {"facts": [fact("build_year", "2001", "Baujahr: 2001")]})
        self.assertEqual(retry.enrich(first)["build_year"], 2001)
        self.assertEqual(retry.successful_calls, 1)

    def test_http_429_has_a_distinct_error_after_bounded_retries(self):
        error = HTTPError("https://example.test", 429, "quota", {}, None)
        with patch.object(enrichment, "urlopen", side_effect=error), patch.object(enrichment.time, "sleep") as sleep:
            with self.assertRaises(enrichment.GeminiQuotaExceeded):
                enrichment._gemini_facts("description", "test-key", "gemini-3.5-flash-lite")
        self.assertEqual(sleep.call_count, 3)

    def test_explicit_values_require_verbatim_evidence_and_units(self):
        payload = {"facts": [
            fact("property_type", "detached_house", "Einfamilienhaus"),
            fact("build_year", "1998", "Baujahr: 1998"),
            fact("living_area_m2", "120", "Wohnfläche: 120 m²"),
            fact("plot_area_m2", "450", "Grundstücksfläche: 450 m²"),
            fact("rooms", "5", "5 Zimmer"),
            fact("energy_class", "C", "Energieklasse: C"),
            fact("end_energy_consumption_kwh_m2a", "98.5",
                 "Endenergieverbrauch: 98,5 kWh/(m² a)"),
            fact("primary_energy_carrier", "Gas", "Primärer Energieträger: Gas"),
            fact("street", "Hauptstraße", "Objektadresse: Hauptstraße 4, 12345 Berlin"),
            fact("house_number", "4", "Objektadresse: Hauptstraße 4, 12345 Berlin"),
            fact("postal_code", "12345", "Objektadresse: Hauptstraße 4, 12345 Berlin"),
            fact("city", "Berlin", "Objektadresse: Hauptstraße 4, 12345 Berlin"),
            fact("build_year", "2025", "Baujahr: 2025"),  # fabricated quote
        ]}
        values = enrichment.validated_facts(DESCRIPTION, payload)
        self.assertEqual(values["build_year"]["value"], 1998)
        self.assertEqual(values["end_energy_consumption_kwh_m2a"]["value"], 98.5)
        self.assertEqual(values["property_type"]["value"], "detached_house")
        self.assertEqual(values["street"]["value"], "Hauptstraße")
        self.assertEqual(len(values), 12)

    def test_ambiguous_or_inferred_facts_remain_unknown(self):
        text = ("Baujahr: 1980. Baujahr: 1990. Heizkosten: 100 kWh. "
                "Endenergieverbrauch: 100 kWh/m². "
                "Primärer Energieträger: Gas und Öl. Vor der Wohnung steht ein Haus.")
        values = enrichment.validated_facts(text, {"facts": [
            fact("build_year", "1980", "Baujahr: 1980"),
            fact("build_year", "1990", "Baujahr: 1990"),
            fact("end_energy_consumption_kwh_m2a", "100", "Heizkosten: 100 kWh"),
            fact("end_energy_consumption_kwh_m2a", "100", "Endenergieverbrauch: 100 kWh/m²"),
            fact("primary_energy_carrier", "Gas", "Primärer Energieträger: Gas und Öl"),
            fact("property_type", "detached_house", "Wohnung"),
            fact("street", "Hauptstraße", "Vor der Wohnung steht ein Haus"),
        ]})
        self.assertEqual(values, {})

    def test_source_fields_win_and_cache_avoids_repeat_model_calls(self):
        s3 = MemoryS3()
        calls = []

        def model_call(description, key, model):
            calls.append((description, model))
            return {"facts": [fact("build_year", "1998", "Baujahr: 1998"),
                              fact("property_type", "detached_house", "Einfamilienhaus")]}

        enricher = enrichment.PropertyEnricher(s3, "lakehouse-data", api_key="test-key",
                                               model_call=model_call)
        row = {"description": DESCRIPTION, "last_detail_success_at": "2026-10-19T17:00:00Z",
               "build_year": 1999}
        first = enricher.enrich(row)
        second = enricher.enrich(row)
        self.assertEqual(first["build_year"], 1999)
        self.assertEqual(first["property_type"], "detached_house")
        self.assertNotIn("build_year", first["llm_evidence"]["quotes"])
        self.assertEqual(first, second)
        self.assertEqual(len(calls), 1)
        self.assertEqual(enricher.new_calls, 1)

    def test_missing_key_fails_but_budget_skips_uncached_descriptions(self):
        s3 = MemoryS3()
        without_key = enrichment.PropertyEnricher(s3, "bucket", api_key="", model_call=lambda *_: {})
        with self.assertRaisesRegex(RuntimeError, "GEMINI_API_KEY"):
            without_key.enrich({"description": DESCRIPTION, "last_detail_success_at": "2026-10-19T17:00:00Z"})
        calls = []
        limited = enrichment.PropertyEnricher(s3, "bucket", api_key="test", max_new_calls=0,
                                              model_call=lambda *_: calls.append(True))
        row = {"description": DESCRIPTION, "last_detail_success_at": "2026-10-19T17:00:00Z"}
        self.assertEqual(limited.enrich(row), row)
        self.assertEqual(limited.budget_skipped, 1)
        self.assertEqual(calls, [])

    def test_wall_clock_budget_bounds_daily_enrichment_latency(self):
        calls = []
        enricher = enrichment.PropertyEnricher(
            MemoryS3(), "bucket", api_key="test", max_elapsed_seconds=0,
            model_call=lambda *_: calls.append(True),
        )
        row = {"description": DESCRIPTION, "last_detail_success_at": "2026-10-19T17:00:00Z"}
        self.assertEqual(enricher.enrich(row), row)
        self.assertEqual(enricher.time_skipped, 1)
        self.assertEqual(calls, [])

    def test_any_description_is_enriched_once_even_without_detail_fetch(self):
        calls = []

        def model_call(description, *_):
            calls.append(description)
            return {"facts": [fact("build_year", "2001", "Baujahr: 2001")]}

        enricher = enrichment.PropertyEnricher(MemoryS3(), "bucket", api_key="test",
                                               model_call=model_call)
        row = {"description": "Baujahr: 2001", "last_detail_success_at": None}
        first = enricher.enrich(row)
        second = enricher.enrich(row)
        self.assertEqual(first["build_year"], 2001)
        self.assertEqual(second["build_year"], 2001)
        self.assertEqual(first["llm_enrichment"]["status"], "completed")
        self.assertEqual(calls, ["Baujahr: 2001"])
        self.assertEqual(enricher.enrich(first), first)
        self.assertEqual(calls, ["Baujahr: 2001"])

    def test_changed_description_is_enriched_again(self):
        calls = []

        def model_call(description, *_):
            calls.append(description)
            year = description.split(": ")[1]
            return {"facts": [fact("build_year", year, description)]}

        enricher = enrichment.PropertyEnricher(MemoryS3(), "bucket", api_key="test",
                                               model_call=model_call)
        first = enricher.enrich({"description": "Baujahr: 2001"})
        second = enricher.enrich({"description": "Baujahr: 2002"})
        self.assertEqual((first["build_year"], second["build_year"]), (2001, 2002))
        self.assertEqual(len(calls), 2)

    def test_failed_call_is_retryable_in_the_next_run(self):
        s3 = MemoryS3()
        row = {"description": "Baujahr: 2001"}

        def fail(*_):
            raise RuntimeError("Gemini unavailable")

        first = enrichment.PropertyEnricher(s3, "bucket", api_key="test", model_call=fail)
        self.assertEqual(first.enrich(row), row)
        self.assertEqual(first.failed_calls, 1)
        self.assertEqual(s3.objects, {})
        second = enrichment.PropertyEnricher(s3, "bucket", api_key="test",
            model_call=lambda *_: {"facts": [fact("build_year", "2001", "Baujahr: 2001")]})
        self.assertEqual(second.enrich(row)["build_year"], 2001)
        self.assertEqual(second.successful_calls, 1)

    def test_successful_empty_result_is_cached(self):
        calls = []

        def model_call(*_):
            calls.append(True)
            return {"facts": []}

        enricher = enrichment.PropertyEnricher(MemoryS3(), "bucket", api_key="test",
                                               model_call=model_call)
        row = {"description": "No explicit property facts"}
        result = enricher.enrich(row)
        self.assertEqual(result["llm_enrichment"]["status"], "completed")
        self.assertEqual(result["llm_enrichment"]["accepted_fields"], 0)
        self.assertEqual(enricher.enrich(row), result)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
