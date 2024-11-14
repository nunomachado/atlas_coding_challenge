import pytest
from unittest.mock import Mock, patch, call, mock_open
from io import StringIO
from electric_vehicle_data_loader import ElectricVehicleDataLoader

@pytest.fixture
def mock_db_connection():
    return Mock()

@pytest.fixture
def sample_csv_content():
    return """VIN (1-10),County,City,State,Postal Code,Model Year,Make,Model,Electric Vehicle Type,Clean Alternative Fuel Vehicle (CAFV) Eligibility,Electric Range,Base MSRP,Legislative District,DOL Vehicle ID,Vehicle Location,Electric Utility,2020 Census Tract
5YJ3E1EB4L,Yakima,Yakima,WA,98908,2020,TESLA,MODEL 3,Battery Electric Vehicle (BEV),Clean Alternative Fuel Vehicle Eligible,322,0,14,127175366,POINT (-120.56916 46.58514),PACIFICORP,53077000904
5YJ3E1EA7K,San Diego,San Diego,CA,92101,2019,TESLA,MODEL 3,Battery Electric Vehicle (BEV),Clean Alternative Fuel Vehicle Eligible,220,0,,266614659,POINT (-117.16171 32.71568),,06073005102
7JRBR0FL9M,Lane,Eugene,OR,97404,2021,VOLVO,S60,Plug-in Hybrid Electric Vehicle (PHEV),Not eligible due to low battery range,22,0,,144502018,POINT (-123.12802 44.09573),,41039002401
"""

@pytest.fixture
def data_loader(mock_db_connection, tmp_path, sample_csv_content):
    # Create temporary CSV file
    csv_path = tmp_path / "test_ev_data.csv"
    with open(csv_path, 'w') as f:
        f.write(sample_csv_content)

    return ElectricVehicleDataLoader(db_connection=mock_db_connection, csv_path=str(csv_path), batch_size=1)

def test_create_table(data_loader, mock_db_connection):
    # Arrange - fixtures handle setup

    # Act
    data_loader.create_table()

    # Assert
    mock_db_connection.execute.assert_called_once_with(data_loader.CREATE_TABLE_SQL)

def test_validate_csv_path_success(data_loader, tmp_path):
    # Arrange
    data_loader.csv_path = str(tmp_path / "test_ev_data.csv")

    # Act & Assert - no exception should be raised
    data_loader._validate_csv_path()  

def test_validate_csv_path_missing_file(data_loader, tmp_path):
    # Arrange
    data_loader.csv_path = str(tmp_path / "missing.csv")

    # Act & Assert
    with pytest.raises(FileNotFoundError):
        data_loader._validate_csv_path()

def test_convert_value(data_loader):
    # Arrange - fixture handles setup

    # Act & Assert - numeric conversions
    assert data_loader._convert_value("Model_Year", "2021") == 2021
    assert data_loader._convert_value("Electric_Range", "300") == 300
    assert data_loader._convert_value("Base_MSRP", "79999") == 79999
    assert data_loader._convert_value("DOL_Vehicle_ID", "123456789") == 123456789

    # Act & Assert - invalid data
    assert data_loader._convert_value("Model_Year", "invalid") is None

    # Act & Assert - non-convertible column
    assert data_loader._convert_value("Make", "Tesla") == "Tesla"

@patch("builtins.open", new_callable=mock_open)
def test_load_data(mock_open_file, data_loader, mock_db_connection, sample_csv_content):
    # Arrange
    mock_open_file.return_value = StringIO(sample_csv_content)
    data_loader.batch_size = 1

    # Act
    data_loader.load_data()

    # Assert
    mock_db_connection.execute.assert_has_calls([call("BEGIN TRANSACTION"), call("COMMIT")])
    
    # Check the correct number of rows were inserted
    insert_query = f"INSERT INTO {data_loader.TABLE_NAME} ({', '.join(data_loader.CSV_TO_DB_COLUMNS.values())}) VALUES ({', '.join(['?'] * len(data_loader.CSV_TO_DB_COLUMNS))})"
    expected_calls = [
        call(insert_query, [[
            '5YJ3E1EB4L', 'Yakima', 'Yakima', 'WA', '98908', 2020, 'TESLA', 'MODEL 3', 
            'Battery Electric Vehicle (BEV)', 'Clean Alternative Fuel Vehicle Eligible', 322, 0, '14', 127175366, 
            'POINT (-120.56916 46.58514)', 'PACIFICORP', '53077000904'
        ]]),
        call(insert_query, [[
            '5YJ3E1EA7K', 'San Diego', 'San Diego', 'CA', '92101', 2019, 'TESLA', 'MODEL 3', 
            'Battery Electric Vehicle (BEV)', 'Clean Alternative Fuel Vehicle Eligible', 220, 0, None, 266614659, 
            'POINT (-117.16171 32.71568)', None, '06073005102'
        ]]),
        call(insert_query, [[
            '7JRBR0FL9M', 'Lane', 'Eugene', 'OR', '97404', 2021, 'VOLVO', 'S60', 
            'Plug-in Hybrid Electric Vehicle (PHEV)', 'Not eligible due to low battery range', 22, 0, None, 144502018, 
            'POINT (-123.12802 44.09573)', None, '41039002401'
        ]]),
    ]
    mock_db_connection.executemany.assert_has_calls(expected_calls, any_order=False)

@patch("builtins.open", new_callable=mock_open)
def test_validate_data_load_success(mock_open_file, data_loader, mock_db_connection, sample_csv_content):
    # Arrange
    mock_open_file.return_value = StringIO(sample_csv_content)
    mock_db_connection.execute.return_value.fetchone.return_value = [3]

    # Act
    data_loader.validate_data_load()

    # Assert
    mock_db_connection.execute.assert_called_with("SELECT COUNT(*) FROM electric_vehicles;")

@patch("builtins.open", new_callable=mock_open)
def test_validate_data_load_mismatch(mock_open_file, data_loader, mock_db_connection, sample_csv_content):
    # Arrange
    mock_open_file.return_value = StringIO(sample_csv_content)
    mock_db_connection.execute.return_value.fetchone.return_value = [2]

    # Act & Assert
    with pytest.raises(AssertionError):
        data_loader.validate_data_load()

@patch("builtins.open", new_callable=mock_open)
def test_run(mock_open_file, data_loader, mock_db_connection, sample_csv_content):
    # Arrange
    mock_open_file.side_effect = [
        StringIO(sample_csv_content),  # First call in load_data()
        StringIO(sample_csv_content)   # Second call in validate_data_load()
    ]
    mock_db_connection.execute.return_value.fetchone.return_value = [3]

    # Act
    data_loader.run()

    # Assert
    mock_db_connection.execute.assert_any_call(data_loader.CREATE_TABLE_SQL)
    mock_db_connection.execute.assert_any_call("BEGIN TRANSACTION")
    mock_db_connection.execute.assert_any_call("COMMIT")
    mock_db_connection.execute.assert_called_with("SELECT COUNT(*) FROM electric_vehicles;")

    # Verify executemany was called 3 times for batch inserts
    assert mock_db_connection.executemany.call_count == 3
    assert mock_open_file.call_count == 2
