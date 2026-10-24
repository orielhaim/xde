import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('build_probe', Path(__file__).with_name('build.py'))
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)


class BuildIsolationTests(unittest.TestCase):
    def test_target_cannot_be_reused_by_another_checkout_or_source(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'target'
            first = {'source': 'first', 'manifest_sha256': 'one'}
            build.bind_target(target, first)
            build.bind_target(target, first)
            for different in [{'source': 'second', 'manifest_sha256': 'one'},
                              {'source': 'first', 'manifest_sha256': 'two'}]:
                with self.assertRaisesRegex(RuntimeError, 'different source'):
                    build.bind_target(target, different)
            build.bind_target(target, first)

    def test_unbound_existing_artifacts_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            (target / 'release').mkdir()
            with self.assertRaisesRegex(RuntimeError, 'without source provenance'):
                build.bind_target(target, {'source': 'first'})


if __name__ == '__main__':
    unittest.main()
