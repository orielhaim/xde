import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('h2_analysis', Path(__file__).with_name('h2-analysis.py'))
analysis = importlib.util.module_from_spec(spec)
spec.loader.exec_module(analysis)


class TimelineTests(unittest.TestCase):
    def test_overlapping_requests_do_not_create_idle_time(self):
        def request(start, finish):
            return dict(payload_start_seconds=start, payload_finish_seconds=finish,
                        bytes=1048576, receive_seconds=1, destination_seconds=2)
        result = analysis.timeline([request(1, 4), request(2, 3), request(5, 6)], 7)
        self.assertEqual(result['no_payload_interval_seconds'], 1)
        self.assertEqual(result['payload_span_seconds'], 5)
        self.assertEqual(result['after_final_sink_seconds'], 1)
        self.assertEqual(result['requests'], 3)

    def test_reversed_interval_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'reversed payload interval'):
            analysis.timeline([dict(payload_start_seconds=2, payload_finish_seconds=1)], 3)


if __name__ == '__main__':
    unittest.main()
