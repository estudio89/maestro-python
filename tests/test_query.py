from maestro.core.query import Filter, Comparison, Comparator, Connector
import unittest


class FilterTest(unittest.TestCase):
    def test_combine(self):
        """Simulates combining filters"""

        comparison1 = Comparison(
            field_name="name", comparator=Comparator.EQUALS, value="test"
        )
        filter1 = Filter(children=[comparison1])

        comparison2 = Comparison(
            field_name="version", comparator=Comparator.EQUALS, value="2"
        )
        filter2 = Filter(children=[comparison2])

        comparison3 = Comparison(
            field_name="number", comparator=Comparator.EQUALS, value=5
        )
        filter3 = Filter(children=[comparison3])


        combined_filter1 = filter1 & (filter2 | filter3)
        self.assertEqual(combined_filter1.connector, Connector.AND)
        self.assertEqual(combined_filter1.children[0], comparison1)
        self.assertEqual(combined_filter1.children[1].connector, Connector.OR)
        self.assertEqual(combined_filter1.children[1].children[0], comparison2)
        self.assertEqual(combined_filter1.children[1].children[1], comparison3)

        combined_filter2 = (filter1 & filter2) | filter3
        self.assertEqual(combined_filter2.connector, Connector.OR)
        self.assertEqual(combined_filter2.children[0].connector, Connector.AND)
        self.assertEqual(combined_filter2.children[0].children[0], comparison1)
        self.assertEqual(combined_filter2.children[0].children[1], comparison2)
        self.assertEqual(combined_filter2.children[1], comparison3)

        combined_filter3 = filter1 & filter2 & filter3
        self.assertEqual(combined_filter3.connector, Connector.AND)
        self.assertEqual(combined_filter3.children[0], comparison1)
        self.assertEqual(combined_filter3.children[1], comparison2)
        self.assertEqual(combined_filter3.children[2], comparison3)

        combined_filter4 = filter1 & filter2 | filter3
        self.assertEqual(combined_filter4.connector, Connector.OR)
        self.assertEqual(combined_filter4.children[0].connector, Connector.AND)
        self.assertEqual(combined_filter4.children[0].children[0], comparison1)
        self.assertEqual(combined_filter4.children[0].children[1], comparison2)
        self.assertEqual(combined_filter4.children[1], comparison3)
