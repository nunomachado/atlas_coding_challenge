import pandas as pd
import pyarrow
import logging

from pathlib import Path

class ElectricVehicleAnalytics:
    """
    Responsible for performing analytics on the electric_vehicles data and saving results.
    """
    def __init__(self, db_connection, output_dir: str = "analytics_output"):
        self.logger = logging.getLogger(__name__)
        self.db_connection = db_connection
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def count_cars_per_city(self) -> pd.DataFrame:
        query = """
        SELECT City, COUNT(*) AS num_electric_cars
        FROM electric_vehicles
        GROUP BY City
        ORDER BY num_electric_cars DESC;
        """
        return self.db_connection.execute(query).fetchdf()

    def top_3_most_popular_vehicles(self) -> pd.DataFrame:
        query = """
        SELECT Make, Model, COUNT(*) AS popularity
        FROM electric_vehicles
        GROUP BY Make, Model
        ORDER BY popularity DESC
        LIMIT 3;
        """
        return self.db_connection.execute(query).fetchdf()

    def most_popular_vehicle_by_postal_code(self) -> pd.DataFrame:
        query = """
        SELECT Postal_Code, Make, Model, COUNT(*) AS popularity
        FROM electric_vehicles
        GROUP BY Postal_Code, Make, Model
        QUALIFY ROW_NUMBER() OVER (PARTITION BY Postal_Code ORDER BY popularity DESC) = 1;
        """
        return self.db_connection.execute(query).fetchdf()

    def count_cars_by_model_year(self) -> pd.DataFrame:
        query = """
        SELECT Model_Year, COUNT(*) AS num_cars
        FROM electric_vehicles
        GROUP BY Model_Year
        ORDER BY num_cars DESC;
        """
        return self.db_connection.execute(query).fetchdf()

    def _debug_df(self, df: pd.DataFrame) -> None:
        self.logger.info(f"Result DataFrame:\n{df.shape}\n{df.head()}\n\n")

    def run(self):
        self.logger.info("Counting the number of electric cars per city...")
        df_city_counts = self.count_cars_per_city()
        self._debug_df(df_city_counts)
        df_city_counts.to_parquet(self.output_dir / "electric_cars_per_city.parquet", index=False, engine='pyarrow', compression='snappy')
        
        self.logger.info("Finding the top 3 most popular electric vehicles...")
        df_top_vehicles = self.top_3_most_popular_vehicles()
        self._debug_df(df_top_vehicles)
        df_top_vehicles.to_parquet(self.output_dir / "top_3_most_popular_vehicles.parquet", index=False)
        
        self.logger.info("Finding the most popular electric vehicle in each postal code...")
        df_popular_by_postal = self.most_popular_vehicle_by_postal_code()
        self._debug_df(df_popular_by_postal)
        df_popular_by_postal.to_parquet(self.output_dir / "popular_vehicle_by_postal_code.parquet", index=False)
        
        self.logger.info("Counting the number of electric cars by model year...")
        df_cars_by_model_year = self.count_cars_by_model_year()
        self._debug_df(df_cars_by_model_year)
        # Write to partitioned parquet files by Model_Year
        for year, year_df in df_cars_by_model_year.groupby('Model_Year'):
            year_dir = self.output_dir / f"model_year={year}"
            year_dir.mkdir(parents=True, exist_ok=True)
            year_df.to_parquet(year_dir / f"electric_cars_{year}.parquet", index=False)
