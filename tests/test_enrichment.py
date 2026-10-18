import importlib.util
from pathlib import Path
import unittest


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
        text = "Baujahr: 1980. Baujahr: 1990. Heizkosten: 100 kWh. Vor der Wohnung steht ein Haus."
        values = enrichment.validated_facts(text, {"facts": [
            fact("build_year", "1980", "Baujahr: 1980"),
            fact("build_year", "1990", "Baujahr: 1990"),
            fact("end_energy_consumption_kwh_m2a", "100", "Heizkosten: 100 kWh"),
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
        row = {"description": DESCRIPTION, "build_year": 1999}
        first = enricher.enrich(row)
        second = enricher.enrich(row)
        self.assertEqual(first["build_year"], 1999)
        self.assertEqual(first["property_type"], "detached_house")
        self.assertNotIn("build_year", first["llm_evidence"]["quotes"])
        self.assertEqual(first, second)
        self.assertEqual(len(calls), 1)
        self.assertEqual(enricher.new_calls, 1)

    def test_missing_key_and_budget_fail_before_partial_silver_write(self):
        s3 = MemoryS3()
        without_key = enrichment.PropertyEnricher(s3, "bucket", api_key="", model_call=lambda *_: {})
        with self.assertRaisesRegex(RuntimeError, "GEMINI_API_KEY"):
            without_key.enrich({"description": DESCRIPTION})
        limited = enrichment.PropertyEnricher(s3, "bucket", api_key="test", max_new_calls=0,
                                              model_call=lambda *_: {})
        with self.assertRaisesRegex(RuntimeError, "new-call budget"):
            limited.enrich({"description": DESCRIPTION})


if __name__ == "__main__":
    unittest.main()
