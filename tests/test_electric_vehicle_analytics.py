import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from io import StringIO
from pathlib import Path
from electric_vehicle_analytics import ElectricVehicleAnalytics

@pytest.fixture
def mock_db_connection():
    return Mock()

@pytest.fixture
def analytics(mock_db_connection, tmp_path):
    return ElectricVehicleAnalytics(db_connection=mock_db_connection, output_dir=str(tmp_path))

@pytest.fixture
def mock_cars_per_city():
    return pd.DataFrame({
        "City": ["Seattle", "Tacoma", "Bellevue"],
        "num_electric_cars": [150, 120, 90]
    })

@pytest.fixture
def mock_top_vehicles():
    return pd.DataFrame({
        "Make": ["Tesla", "Nissan", "Chevrolet"],
        "Model": ["Model S", "Leaf", "Bolt"],
        "popularity": [50, 45, 40]
    })

@pytest.fixture
def mock_popular_by_postal():
    return pd.DataFrame({
        "Postal_Code": ["98101", "98402", "98004"],
        "Make": ["Tesla", "Nissan", "Chevrolet"],
        "Model": ["Model S", "Leaf", "Bolt"],
        "popularity": [50, 45, 40]
    })

@pytest.fixture
def mock_cars_by_model_year():
    return pd.DataFrame({
        "Model_Year": [2021, 2020, 2021],
        "num_cars": [100, 150, 50]
    })

def test_count_cars_per_city(analytics, mock_db_connection, mock_cars_per_city):
    # Arrange
    mock_db_connection.execute.return_value.fetchdf.return_value = mock_cars_per_city
    
    # Act
    result = analytics.count_cars_per_city()
    
    # Assert
    pd.testing.assert_frame_equal(result, mock_cars_per_city)
    mock_db_connection.execute.assert_called_once()
    assert 'GROUP BY City' in mock_db_connection.execute.call_args[0][0]

@pytest.mark.skip
def test_top_3_most_popular_vehicles(analytics, mock_db_connection, mock_top_vehicles):
    # Arrange
    mock_db_connection.execute.return_value.fetchdf.return_value = mock_top_vehicles
    
    # Act
    result = analytics.top_3_most_popular_vehicles()
    
    # Assert
    pd.testing.assert_frame_equal(result, mock_top_vehicles)
    mock_db_connection.execute.assert_called_once()
    assert 'LIMIT 3' in mock_db_connection.execute.call_args[0][0]


@pytest.mark.skip
def test_most_popular_vehicle_by_postal_code(analytics, mock_db_connection, mock_popular_by_postal):
    # Arrange
    mock_db_connection.execute.return_value.fetchdf.return_value = mock_popular_by_postal
    
    # Act
    result = analytics.most_popular_vehicle_by_postal_code()
    
    # Assert
    pd.testing.assert_frame_equal(result, mock_popular_by_postal)
    mock_db_connection.execute.assert_called_once()
    assert 'QUALIFY' in mock_db_connection.execute.call_args[0][0]

@pytest.mark.skip
def test_count_cars_by_model_year(analytics, mock_db_connection, mock_cars_by_model_year):
    # Arrange
    mock_db_connection.execute.return_value.fetchdf.return_value = mock_cars_by_model_year
    
    # Act
    result = analytics.count_cars_by_model_year()
    
    # Assert
    pd.testing.assert_frame_equal(result, mock_cars_by_model_year)
    mock_db_connection.execute.assert_called_once()
    assert 'GROUP BY Model_Year' in mock_db_connection.execute.call_args[0][0]

def test_run(analytics, mock_db_connection, mock_cars_per_city, mock_top_vehicles, mock_popular_by_postal, mock_cars_by_model_year):
    # Arrange
    mock_db_connection.execute.return_value.fetchdf.side_effect = [
        mock_cars_per_city,
        mock_top_vehicles,
        mock_popular_by_postal,
        mock_cars_by_model_year
    ]
    
    # Create required directories
    (analytics.output_dir / "model_year=2020").mkdir(parents=True, exist_ok=True)
    (analytics.output_dir / "model_year=2021").mkdir(parents=True, exist_ok=True)
    
    # Act
    analytics.run()

    # Assert
    assert (analytics.output_dir / "electric_cars_per_city.parquet").exists()
    assert (analytics.output_dir / "top_3_most_popular_vehicles.parquet").exists()
    assert (analytics.output_dir / "popular_vehicle_by_postal_code.parquet").exists()

    assert (analytics.output_dir / "model_year=2020" / "electric_cars_2020.parquet").exists()
    assert (analytics.output_dir / "model_year=2021" / "electric_cars_2021.parquet").exists()
