import json
import unittest

from worker.singleflight import canonical_flight_identity, derive_flight_key


class SingleFlightIdentityTests(unittest.TestCase):
    def fields(self, **overrides):
        fields = {
            "prompt": "Explain Redis locks exactly.  ",
            "provider": "fake",
            "model_id": "demo-model",
            "model_revision": "revision-1",
            "generation_config": {"temperature": 0.2, "stop": ["END"]},
            "tenant_id": "tenant-a",
            "knowledge_namespace": "kb-v1",
        }
        fields.update(overrides)
        return fields

    def test_canonical_identity_is_stable_across_mapping_order(self):
        first = self.fields(generation_config={"temperature": 0.2, "top_p": 0.9})
        second = self.fields(generation_config={"top_p": 0.9, "temperature": 0.2})

        self.assertEqual(canonical_flight_identity(**first), canonical_flight_identity(**second))
        self.assertEqual(derive_flight_key(**first), derive_flight_key(**second))

    def test_key_is_sha256_and_does_not_disclose_prompt(self):
        fields = self.fields()
        key = derive_flight_key(**fields)

        self.assertRegex(key, r"^inference_flight:[0-9a-f]{64}$")
        self.assertNotIn(fields["prompt"], key)
        identity = json.loads(canonical_flight_identity(**fields))
        self.assertEqual(identity["prompt"], fields["prompt"])
        self.assertEqual(identity["schema_version"], 1)

    def test_every_response_affecting_namespace_changes_key(self):
        baseline = derive_flight_key(**self.fields())
        variants = [
            {"prompt": "explain redis locks exactly.  "},
            {"provider": "openai"},
            {"model_id": "other-model"},
            {"model_revision": "revision-2"},
            {"generation_config": {"temperature": 0.3, "stop": ["END"]}},
            {"tenant_id": "tenant-b"},
            {"knowledge_namespace": "kb-v2"},
        ]

        for override in variants:
            with self.subTest(override=override):
                self.assertNotEqual(baseline, derive_flight_key(**self.fields(**override)))

    def test_prompt_is_not_trimmed_or_case_normalized(self):
        baseline = derive_flight_key(**self.fields(prompt="Prompt"))
        self.assertNotEqual(baseline, derive_flight_key(**self.fields(prompt="prompt")))
        self.assertNotEqual(baseline, derive_flight_key(**self.fields(prompt="Prompt ")))

    def test_rejects_non_finite_and_non_json_generation_values(self):
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    derive_flight_key(**self.fields(generation_config={"temperature": value}))

        with self.assertRaises(TypeError):
            derive_flight_key(**self.fields(generation_config={"unsupported": {1, 2}}))
        with self.assertRaises(TypeError):
            derive_flight_key(**self.fields(generation_config={1: "invalid-key"}))
        with self.assertRaises(TypeError):
            derive_flight_key(**self.fields(generation_config=[]))

    def test_rejects_non_string_identity_fields(self):
        with self.assertRaises(TypeError):
            derive_flight_key(**self.fields(prompt=b"bytes"))


if __name__ == "__main__":
    unittest.main()
