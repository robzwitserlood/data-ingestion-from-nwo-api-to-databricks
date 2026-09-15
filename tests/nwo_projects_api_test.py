import json
import pytest
from unittest.mock import patch, mock_open
from nwo_projects_api import fetch_data, write_json_file, validate_response_shape, NWOpenAPIShapeError


@patch('nwo_projects_api.requests.get')
def test_fetch_data(mock_get):
    # Arrange
    mock_response = mock_get.return_value
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {'projects': ['test_data'], 'meta': {'pages': 2}}

    # Act
    result = fetch_data(page_nr=2)

    # Assert
    mock_get.assert_called_once_with(
        'https://nwopen-api.nwo.nl/NWOpen-API/api/Projects',
        params={'page': 2})
    assert result['projects'] == ['test_data']
    assert result['meta']['pages'] == 2

def test_validate_response_shape_missing_projects():
    with pytest.raises(NWOpenAPIShapeError):
        validate_response_shape({'meta': {'pages': 1}})

def test_validate_response_shape_projects_not_list():
    with pytest.raises(NWOpenAPIShapeError):
        validate_response_shape({'projects': 'not_a_list', 'meta': {'pages': 1}})

def test_validate_response_shape_missing_meta():
    with pytest.raises(NWOpenAPIShapeError):
        validate_response_shape({'projects': []})

def test_validate_response_shape_meta_not_dict():
    with pytest.raises(NWOpenAPIShapeError):
        validate_response_shape({'projects': [], 'meta': 'not_a_dict'})

def test_validate_response_shape_missing_pages():
    with pytest.raises(NWOpenAPIShapeError):
        validate_response_shape({'projects': [], 'meta': {}})

def test_validate_response_shape_pages_not_int():
    with pytest.raises(NWOpenAPIShapeError):
        validate_response_shape({'projects': [], 'meta': {'pages': 'two'}})

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