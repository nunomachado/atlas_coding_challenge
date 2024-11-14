import os
import duckdb
import logging

from electric_vehicle_data_loader import ElectricVehicleDataLoader
from electric_vehicle_analytics import ElectricVehicleAnalytics

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)

def main():
    os.makedirs(os.path.join(os.path.dirname(__file__), "db"), exist_ok=True)
    db_connection = duckdb.connect(database="db/electric_vehicles.duckdb")
    csv_path = "data/Electric_Vehicle_Population_Data.csv"

    # Run the data loader
    data_loader = ElectricVehicleDataLoader(db_connection, csv_path)
    data_loader.run()

    # Run analytics
    analytics = ElectricVehicleAnalytics(db_connection)
    analytics.run()

    db_connection.close()

if __name__ == "__main__":
    main()
