import json
from unittest.mock import patch, mock_open
from nwo_projects_api import fetch_data, write_json_file


@patch('nwo_projects_api.requests.get')
def test_fetch_data(mock_get):
    # Arrange
    mock_response = mock_get.return_value
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {'projects': 'test_data', 'meta': {'page': 2}}

    # Act
    result = fetch_data(page_nr=2)
    
    # Assert
    mock_get.assert_called_once_with(
        'https://nwopen-api.nwo.nl/NWOpen-API/api/Projects',
        params={'page': 2})
    #mock_get.raise_for_status.assert_called_once()
    assert result['projects'] == 'test_data'
    assert result['meta']['page'] == 2

@patch('nwo_projects_api.os.makedirs')
@patch('nwo_projects_api.os.path.exists')
@patch('nwo_projects_api.open', new_callable=mock_open)    
def test_write_json_file(mock_file, mock_exists, mock_mkdirs):
    # Arrange
    mock_exists.return_value = False
    data = "{'projects': 'test_data'}"
    prefix = 'test_run'
    catalog = 'test_catalog'
    schema = 'test_schema'
    page_nr = 1
    expected_dir = f'/Volumes/{catalog}/{schema}/landing/nwo_projects/'
    expected_file_path = f'{expected_dir}{prefix}_page{page_nr}.json'

    # Act
    write_json_file(data, prefix, catalog, schema, page_nr)

    # Assert
    mock_mkdirs.assert_called_once_with(expected_dir, exist_ok=True)
    mock_exists.assert_called_once_with(expected_file_path)
    mock_file().write.assert_called_once_with(json.dumps(data))