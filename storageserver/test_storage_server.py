import unittest
# import storageserver.app as tested_app
import json
# import requests
from storage_server import app


class FlaskAppTests(unittest.TestCase):

    def setUp(self):
        # self.IP = "0.0.0.0"
        # self.PORT = 8083
        # self.URL = "http://" + self.IP + ":" + str(self.PORT)
        app.config['TESTING'] = True
        self.app = app.test_client()

    def test_get_ping_endpoint(self):
        rv = self.app.get("/ping")
        data = json.loads(rv.data)

        # convert to string
        # s = json.dumps(data)
        self.assertEqual(data['response'], "echo")

    # def login(self, username, password):
    #     return self.app.post('/login', data=dict(
    #         username=username,
    #         password=password
    #     ), follow_redirects=True)


if __name__ == '__main__':
    unittest.main()


# https://temofeev.ru/info/articles/stroim-domashniy-ci-cd-pri-pomoshchi-github-actions-i-python/
