import unittest
from unittest.mock import patch

from roundtrip import append_requests, sample_event
from tcp_client import MAX_REQUEST_BYTES, encode_json


class BatchingTests(unittest.TestCase):
    def test_large_batch_size_splits_by_bytes_without_losing_events(self):
        requests = list(append_requests("roundtrip-test", 10_000, 100_000_000))
        self.assertGreater(len(requests), 1)
        expected_start = 0
        for start, request in requests:
            self.assertEqual(start, expected_start)
            self.assertLessEqual(len(encode_json(request)), MAX_REQUEST_BYTES)
            self.assertEqual(request["expectedRevision"], "noStream" if start == 0 else start - 1)
            self.assertEqual(request["events"], [
                sample_event("roundtrip-test", i) for i in range(start, start + len(request["events"]))
            ])
            expected_start += len(request["events"])
        self.assertEqual(expected_start, 10_000)

    def test_exact_byte_limit_with_unicode_and_escaped_characters(self):
        stream = '注文-"\\'
        first_two = next(append_requests(stream, 2, 2))[1]
        limit = len(encode_json(first_two))
        with patch("roundtrip.MAX_REQUEST_BYTES", limit):
            requests = list(append_requests(stream, 5, 100))
        self.assertEqual(len(encode_json(requests[0][1])), limit)
        self.assertEqual(len(requests[0][1]["events"]), 2)
        self.assertTrue(all(len(encode_json(request)) <= limit for _, request in requests))
        self.assertEqual(sum(len(request["events"]) for _, request in requests), 5)

    def test_event_count_limit_and_partial_final_batch(self):
        requests = list(append_requests("test", 7, 3))
        self.assertEqual([start for start, _ in requests], [0, 3, 6])
        self.assertEqual([len(request["events"]) for _, request in requests], [3, 3, 1])

    def test_single_oversized_event_is_rejected(self):
        with patch("roundtrip.MAX_REQUEST_BYTES", 100):
            with self.assertRaisesRegex(ValueError, "One event"):
                next(append_requests("test", 1, 1))


if __name__ == "__main__":
    unittest.main()
