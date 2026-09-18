import unittest
class HealthTest(unittest.TestCase):
    def test_module_imports(self):
        import service.main
        self.assertTrue(hasattr(service.main, 'run'))
