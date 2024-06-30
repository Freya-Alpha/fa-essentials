import unittest
from unittest.mock import patch, Mock

from faessentials.database import is_ksqldb_available

class TestKsqlDBAvailability(unittest.TestCase):

    @patch('faessentials.database.httpx.get')
    def test_ksqldb_available(self, mock_httpx_get):
        # Mocking a successful response from ksqlDB
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'KsqlServerInfo': {'serverStatus': 'RUNNING'}}
        mock_httpx_get.return_value = mock_response

        result = is_ksqldb_available()
        print(result)
        self.assertTrue(result)

    @patch('faessentials.database.httpx.get')
    def test_ksqldb_unavailable(self, mock_httpx_get):
        # Mocking a response with NOT_RUNNING status from ksqlDB
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'KsqlServerInfo': {'serverStatus': 'NOT_RUNNING'}}
        mock_httpx_get.return_value = mock_response

        result = is_ksqldb_available()
        print(result)
        self.assertFalse(result)

    @patch('faessentials.database.httpx.get')
    def test_ksqldb_no_response(self, mock_httpx_get):
        # Mocking no response from ksqlDB
        mock_response = Mock()
        mock_response.status_code = 500
        mock_httpx_get.return_value = mock_response

        result = is_ksqldb_available()
        print(result)
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()
