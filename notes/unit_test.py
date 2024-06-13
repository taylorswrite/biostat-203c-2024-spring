import unittest

def reverse_lookup(D, val):
    """
    Find all keys in a dictionary D with the specified value. 
    """
    if type(D) != dict:
        raise TypeError("First argument must be a dictionary")

    return [key for key in D.keys() if D[key] == val]

class TestReverseLookUp(unittest.TestCase): # class called TestCase defined in module unittest
    
    def test_standard_lookup(self):
        D = {"Potter": "student",
                "Dumbledore": "professor",
                "Malfoy": "student", 
                "Snape": "professor"}
        res = reverse_lookup(D, "student") # expect to see ["Potter", "Malfoy"]
        self.assertEqual(len(res), 2) # assert that len(res) == 2
        
    def test_no_match(self):
        D = {"Potter": "student",
                "Dumbledore": "professor",
                "Malfoy": "student", 
                "Snape": "professor"}
        
        res = reverse_lookup(D, "owl") 
        self.assertEqual(len(res), 0)
        
    def test_type_error(self):
        D = ["Potter", "Dumbledore", "Malfoy", "Snape"]
        
        with self.assertRaises(TypeError):
            reverse_lookup(D, "student") # assert that this line raises a TypeError
    
    # purposefully wrong test case to show you what happens when a test fails
    def test_prof_incorrect(self):
        D = {"Potter": "student",
                "Dumbledore": "professor",
                "Malfoy": "student", 
                "Snape": "professor"}
        res = reverse_lookup(D, "professor")
        self.assertEqual(len(res), 3)

if __name__ == "__main__":
    unittest.main()