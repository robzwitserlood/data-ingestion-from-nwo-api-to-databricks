import json
import pytest
from unittest.mock import patch
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

@pytest.mark.parametrize("data", [
    pytest.param({'meta': {'pages': 1}}, id="missing_projects"),
    pytest.param({'projects': 'not_a_list', 'meta': {'pages': 1}}, id="projects_not_list"),
    pytest.param({'projects': []}, id="missing_meta"),
    pytest.param({'projects': [], 'meta': 'not_a_dict'}, id="meta_not_dict"),
    pytest.param({'projects': [], 'meta': {}}, id="missing_pages"),
    pytest.param({'projects': [], 'meta': {'pages': 'two'}}, id="pages_not_int"),
])
def test_validate_response_shape_raises_on_bad_shape(data):
    with pytest.raises(NWOpenAPIShapeError):
        validate_response_shape(data)

def test_write_json_file(tmp_path):
    # Arrange
    data = {'projects': [{'project_id': '001'}], 'meta': {'pages': 1}}
    expected_file = tmp_path / 'test_catalog' / 'test_schema' / 'landing' / 'nwo_projects' / 'test_run_page1.json'

    # Act
    write_json_file(data, 'test_run', 'test_catalog', 'test_schema', 1, base_dir=str(tmp_path))

    # Assert
    assert json.loads(expected_file.read_text()) == data

def test_write_json_file_skips_existing_file(tmp_path):
    # Arrange
    write_json_file({'projects': [], 'meta': {'pages': 1}}, 'test_run', 'cat', 'sch', 1, base_dir=str(tmp_path))
    existing_file = tmp_path / 'cat' / 'sch' / 'landing' / 'nwo_projects' / 'test_run_page1.json'
    original = existing_file.read_text()

    # Act
    write_json_file({'projects': ['new'], 'meta': {'pages': 1}}, 'test_run', 'cat', 'sch', 1, base_dir=str(tmp_path))

    # Assert
    assert existing_file.read_text() == original
