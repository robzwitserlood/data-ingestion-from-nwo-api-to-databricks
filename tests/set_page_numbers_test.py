from unittest.mock import patch
from set_page_numbers import get_page_numbers

@patch('set_page_numbers.fetch_data')
def test_get_page_numbers(mock_fetch_data):
    # Arrange
    mock_fetch_data.return_value = {'meta': {'pages': 5}}

    # Act
    result = get_page_numbers()

    # Assert
    assert result == [1, 2, 3, 4, 5]