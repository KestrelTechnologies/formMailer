"""
see http://docs.python.org/2/library/unittest.html for details, and this test won't work as written
"""
import unittest
from recordFilter import recordFilter

class recordFilterTestCase(unittest.TestCase):
    def setUp(self):
        self.recordFilter = recordFilter()

    def tearDown(self):
        self.recordFilter = None

    def test_default_size(self):
        self.assertEqual(self.recordFilter.size(), (50,50),
                         'incorrect default size')

    def test_resize(self):
        self.recordFilter.resize(100,150)
        self.assertEqual(self.recordFilter.size(), (100,150),
                         'wrong size after resize')
