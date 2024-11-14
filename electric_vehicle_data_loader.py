import csv
import duckdb
import os
import time
import logging

from itertools import islice
from typing import Dict, List, Any, Optional, Union

class ElectricVehicleDataLoader:
    """
    Responsible for creating the table structure for electric vehicles and loading data from a CSV file.
    """

    TABLE_NAME = "electric_vehicles"
    CREATE_TABLE_SQL = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            VIN VARCHAR,
            County VARCHAR,
            City VARCHAR,
            State VARCHAR,
            Postal_Code VARCHAR,
            Model_Year INTEGER,
            Make VARCHAR,
            Model VARCHAR,
            Electric_Vehicle_Type VARCHAR,
            CAFV_Eligibility VARCHAR,
            Electric_Range INTEGER,
            Base_MSRP INTEGER,
            Legislative_District VARCHAR,
            DOL_Vehicle_ID BIGINT,
            Vehicle_Location VARCHAR,
            Electric_Utility VARCHAR,
            Census_Tract VARCHAR
        );
    """

    # Map CSV headers to column names in the database
    CSV_TO_DB_COLUMNS = {
        "VIN (1-10)": "VIN",
        "County": "County",
        "City": "City",
        "State": "State",
        "Postal Code": "Postal_Code",
        "Model Year": "Model_Year",
        "Make": "Make",
        "Model": "Model",
        "Electric Vehicle Type": "Electric_Vehicle_Type",
        "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "CAFV_Eligibility",
        "Electric Range": "Electric_Range",
        "Base MSRP": "Base_MSRP",
        "Legislative District": "Legislative_District",
        "DOL Vehicle ID": "DOL_Vehicle_ID",
        "Vehicle Location": "Vehicle_Location",
        "Electric Utility": "Electric_Utility",
        "2020 Census Tract": "Census_Tract"
    }

    def __init__(self, db_connection: duckdb.DuckDBPyConnection, csv_path: str, batch_size: int = 5000) -> None:
        """
        Initialize the data loader with database connection and file parameters.

        Args:
            db_connection: Database connection instance
            csv_path: Path to the CSV file to load
            batch_size: Number of records to process in each batch
        """
        self.logger = logging.getLogger(__name__)
        self.db_connection = db_connection
        self.csv_path = csv_path
        self.batch_size = batch_size
        self._validate_csv_path()

    def _validate_csv_path(self) -> None:
        """
        Validate that the CSV file exists at the specified path.

        Raises:
            FileNotFoundError: If the CSV file does not exist
        """
        if not os.path.isfile(self.csv_path):
            raise FileNotFoundError(f"The file {self.csv_path} does not exist.")

    def create_table(self) -> None:
        """Create the electric vehicles table if it doesn't exist."""
        self.logger.info("Creating electric vehicles table...")
        self.db_connection.execute(self.CREATE_TABLE_SQL)
        self.logger.info("Table creation completed")

    def _convert_value(self, col: str, val: Any) -> Optional[Union[int, str]]:
        """
        Convert values to appropriate types for database insertion.

        Args:
            col: Database column name
            val: Value to convert

        Returns:
            Converted value or None if conversion fails
        """
        if not val:
            return None
        
        # Define converters for specific columns
        converters = {
            "Model_Year": int,
            "Electric_Range": int,
            "Base_MSRP": int,
            "DOL_Vehicle_ID": int
        }
        
        # Apply conversion if a converter is defined for the column
        if col in converters:
            try:
                return converters[col](val)
            except (ValueError, TypeError):
                return None  
        else:
            return val

    def load_data_built_in(self) -> None:
        """Load data using DuckDB's built-in COPY command. Used for performance comparison."""
        self.logger.info("Starting built-in data load process...")
        start_time = time.perf_counter()
        query = f"""
        COPY electric_vehicles FROM '{self.csv_path}' 
        (AUTO_DETECT TRUE);
        """
        self.db_connection.execute(query)
        end_time = time.perf_counter()

        self.logger.info(f"Data loaded in {end_time - start_time:.2f} seconds")

    def load_data(self) -> None:
        """
        Load data from CSV file into database using batch processing.
        Uses transactions and type conversion for better performance.

        Raises:
            Exception: If any error occurs during data loading
        """
        self.logger.info(f"Starting data load process for table: {self.TABLE_NAME}")
        
        db_columns = list(self.CSV_TO_DB_COLUMNS.values())
        placeholders = ", ".join(["?"] * len(db_columns))
        insert_query = f"INSERT INTO {self.TABLE_NAME} ({', '.join(db_columns)}) VALUES ({placeholders})"
        
        start_time = time.perf_counter()
        rows_processed = 0

        # Use transactions for ensure data integrity
        self.db_connection.execute("BEGIN TRANSACTION")
        try:
            with open(self.csv_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)

                # Process rows in batches to reduce number of database calls
                while True:
                    batch = [
                        # Type conversion for numeric fields
                        [self._convert_value(self.CSV_TO_DB_COLUMNS[csv_col], row.get(csv_col)) for csv_col in self.CSV_TO_DB_COLUMNS]
                        for row in islice(reader, self.batch_size)
                    ]
                    if not batch:
                        break 

                    self.db_connection.executemany(insert_query, batch)
                    rows_processed += len(batch)
                    self.logger.info(f"Processed {rows_processed:,} rows")
            
            self.db_connection.execute("COMMIT")
            end_time = time.perf_counter()
            self.logger.info(f"Data loaded in {end_time - start_time:.2f} seconds, {rows_processed:,} rows total.")

            
        except Exception as e:
            self.db_connection.execute("ROLLBACK")
            self.logger.error(f"Error loading data: {str(e)}")
            raise

    def validate_data_load(self) -> None:
        """
        Validates that the number of rows loaded into the database matches
        the number of rows in the source CSV file, excluding the header row.
        
        Raises:
            AssertionError: If the row counts don't match or if the table is empty.
        """
        self.logger.info("Validating data load...")
        
        with open(self.csv_path, 'r', encoding='utf-8') as csv_file:
            csv_row_count = sum(1 for _ in csv_file) - 1  # Subtract 1 for header row
        
        db_row_count = self.db_connection.execute("SELECT COUNT(*) FROM electric_vehicles;").fetchone()[0]
        
        if db_row_count != csv_row_count:
            raise AssertionError(f"Row count mismatch: (CSV: {csv_row_count}, DB: {db_row_count})\n\n")
        
        self.logger.info(f"Validation successful. Row count matches (CSV: {csv_row_count:,}, DB: {csv_row_count:,})\n\n")

    def run(self) -> None:
        self.create_table()
        #self.load_data_built_in()
        self.load_data()
        self.validate_data_load()
