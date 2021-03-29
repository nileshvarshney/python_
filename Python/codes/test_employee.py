import unittest
from unittest.mock import patch
from employee import Employee

class TestEmployee(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('setUpClass')

    @classmethod
    def tearDownClass(cls):
        print('tearDownClass')

    def setUp(self):
        print('setup')
        self.emp_1 = Employee('Hello','World', 100)
        self.emp_2 = Employee('Hello','India', 200)

    def tearDown(self):
        print('tearDown\n')
        pass

    def test_email(self):  
        print('test_email')
        self.assertEqual(self.emp_1.email, 'Hello.World@email.com')
        self.assertEqual(self.emp_2.email, 'Hello.India@email.com') 

        self.emp_1.first = 'Hi'
        self.emp_2.first = 'Namste'  

        self.assertEqual(self.emp_1.email, 'Hi.World@email.com')
        self.assertEqual(self.emp_2.email, 'Namste.India@email.com') 

    def test_fullname(self):  
        print('test_fullname')
        self.assertEqual(self.emp_1.fullname, 'Hello World')
        self.assertEqual(self.emp_2.fullname, 'Hello India') 

        self.emp_1.first = 'Hi'
        self.emp_2.first = 'Namste'  

        self.assertEqual(self.emp_1.fullname, 'Hi World')
        self.assertEqual(self.emp_2.fullname, 'Namste India') 

    def test_apply_raise(self):
        print('test_apply_raise')
        self.emp_1.apply_raise()
        self.emp_2.apply_raise()

        self.assertEqual(self.emp_1.pay, 105)
        self.assertEqual(self.emp_2.pay, 210) 

    def test_monthly_schedule(self):
        with path('employee.requests.get') as mocked_get:



if __name__ == "__main__":
    unittest.main()