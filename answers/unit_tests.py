"""
Corteva Coding Exercise

CREATED BY: Jesse Fimbres
LAST MODIFIED: 12/29/2022
"""

import json
import unittest

import app

class TestAPI(unittest.TestCase):
    """
    Class tests API endpoints in app.py

    Attributes:
        app: Flask test client for app
    """

    def setUp(self):
        app.app.testing = True
        self.app = app.app.test_client()

    def test_index_resp(self):
        """ Tests for 200 in index base url """
        resp = self.app.get('/')
        json_resp = json.loads(resp.get_data())
        self.assertEqual(resp.status_code,200)
        self.assertEqual(json_resp['Greeting'],'Hello Corteva!')

    def test_weather_status(self):
        """ Tests for 200 in weather base url """
        resp = self.app.get('/api/weather')
        self.assertEqual(resp.status_code,200)

    def test_weather_count(self):
        """ Tests for correct count with query params in weather """
        resp = self.app.get('/api/weather?date=1997-06-09&station_id=USC00110072')
        json_resp = json.loads(resp.get_data())
        self.assertEqual(json_resp['payload']['count'],1)
        self.assertEqual(json_resp['payload']['next'],'')
        self.assertEqual(json_resp['payload']['previous'],'')

    def test_weather_pagination(self):
        """ Tests for correct pagination with query params in weather """
        resp = self.app.get('/api/weather?date=1997-06-09&offset=0&limit=20')
        json_resp = json.loads(resp.get_data())
        self.assertEqual(len(json_resp['payload']['results']),20)

        next_url = json_resp['payload']['next'].partition('/api')
        next_url = next_url[1] + next_url[2]
        resp = self.app.get(next_url)
        self.assertEqual(resp.status_code,200)

    def test_yield_status(self):
        """ Tests for 200 in yield base url """
        resp = self.app.get('/api/yield')
        self.assertEqual(resp.status_code,200)

    def test_yield_count(self):
        """ Tests for correct count with query params in yield """
        resp = self.app.get('/api/yield?year=2000')
        json_resp = json.loads(resp.get_data())
        self.assertEqual(json_resp['payload']['count'],1)
        self.assertEqual(json_resp['payload']['next'],'')
        self.assertEqual(json_resp['payload']['previous'],'')

    def test_stats_status(self):
        """ Tests for 200 in stats base url """
        resp = self.app.get('/api/weather/stats')
        self.assertEqual(resp.status_code,200)

    def test_stats_count(self):
        """ Tests for correct count with query params in stats """
        resp = self.app.get('/api/weather/stats?year=1987&station_id=USC00110338')
        json_resp = json.loads(resp.get_data())
        self.assertEqual(json_resp['payload']['count'],1)
        self.assertEqual(json_resp['payload']['next'],'')
        self.assertEqual(json_resp['payload']['previous'],'')

    def test_stats_pagination(self):
        """ Tests for correct pagination with query params in stats """
        resp = self.app.get('/api/weather/stats?year=1987&offset=0&limit=20')
        json_resp = json.loads(resp.get_data())
        self.assertEqual(len(json_resp['payload']['results']),20)

        next_url = json_resp['payload']['next'].partition('/api')
        next_url = next_url[1] + next_url[2]
        resp = self.app.get(next_url)
        self.assertEqual(resp.status_code,200)


if __name__ == '__main__':
    unittest.main()
