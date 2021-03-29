import unittest
import calc

class TestCalc(unittest.TestCase):

    def test_add(self):
        self.assertEqual(calc.add(10, 5),15)
        self.assertEqual(calc.add(-5, 5),0)
        self.assertEqual(calc.add(-8, -8),-16)

    def test_sub(self):
        self.assertEqual(calc.sub(10, 5),5)
        self.assertEqual(calc.sub(-5, 5),-10)
        self.assertEqual(calc.sub(-8, -8),0)

    def test_mul(self):
        self.assertEqual(calc.mul(10, 5),50)
        self.assertEqual(calc.mul(-5, 5),-25)
        self.assertEqual(calc.mul(-8, -8),64)

    def test_div(self):
        self.assertEqual(calc.div(10, 5),2)
        self.assertEqual(calc.div(-5, 5),-1)
        self.assertEqual(calc.div(-8, -8),1)

        # testing value error
        self.assertRaises(ValueError, calc.div, 10, 0)

if __name__ == '__main__':
    unittest.main()