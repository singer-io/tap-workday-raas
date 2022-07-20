import unittest
from tap_workday_raas.transform import WorkdayTransformer as Transformer

class TestBooleanTransform(unittest.TestCase):
    """Test Case to Check Valid Boolean Transformation"""
    transformer = getattr(Transformer(),"_transform")

    def test_transform_eval_false(self,):
        """Check if `0` & `False` is evaluated as False """
        self.assertEqual(self.transformer("0","boolean",None,None),(True,False))
        self.assertEqual(self.transformer("False","boolean",None,None),(True,False))


    def test_transform_eval_true(self):
        """Check if `1` & `True` is evaluated as True"""
        self.assertEqual(self.transformer("1","boolean",None,None),(True,True))
        self.assertEqual(self.transformer("True","boolean",None,None),(True,True))
