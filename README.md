# Atlas Coding Challenge

This repository contains a solution to the Atlas Coding Challenge, based on **danielbench's data engineering exercise 8** (see details below).

## Additional Requirements
- Implement data loading into DuckDB in Python, avoiding built-in DuckDB import functions.
- Include unit tests


## Data Schema

Based on an analysis of the [sample CSV file](./data/Electric_Vehicle_Population_Data.csv), the following schema was designed for the database table:

```sql
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
```

**Note:** Column names were simplified for readability. The mapping between CSV columns and database columns is defined in the variable `CSV_TO_DB_COLUMNS` in [electric_vehicle_data_loader.py](./electric_vehicle_data_loader.py).


## Solution Overview

### Assumptions
- The CSV file is well-formed (i.e. has consistent columns, properly quoted values, correct delimiter, valid encoding).
- Input format is CSV by default. To extend this solution to other formats (e.g. JSON), the Strategy Pattern could be used:
    1. Define a `DataLoaderStrategy` interface with a `load_data` method for format-specific data loading.
    2. Implement `CSVDataLoader` and `JSONDataLoader` classes to handle respective formats.
    3. Update `ElectricVehicleDataLoader` to accept a `DataLoaderStrategy` instance in its constructor, allowing format-agnostic loading based on file type.

### Data Loading Implementation

Data loading into DuckDB was implemented in Python with the following aspects in mind:
- **Batch Processing:** Data is loaded in batches to improve performance over single-row insertions.
- **Type Conversion:** Custom type conversion functions ensure accurate parsing and storage of numeric fields.
- **Transactions:** Database transactions are employed to guarantee atomicity and data consistency during the loading process.
- **Validation:** A validation step checks that the number of loaded rows matches the source CSV, ensuring data integrity.

### Benchmarking

To assess the performance of this custom loading process, a built-in DuckDB approach using the `COPY` feature was also implemented in [electric_vehicle_data_loader.py](./electric_vehicle_data_loader.py) as `load_data_built_in`. Benchmark results show:
- `load_data_built_in`: Loads data in less than one second.
- `load_data`: Loads data in approximately 16 seconds.

The performance of the `load_data` function could be further optimized with the following approaches:
- **Parallel processing:** Leverage multithreading to split the CSV file into chunks and process them concurrently.
- **Optimal batch size:** Measure performance with different batch sizes to find an optimal trade-off between memory usage and insert performance.


--- 

## Exercise 8 Description [(original repo)](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-8)

In this exercise we are going to have some problems to solve that will require us to 
use various DuckDB functions and functionality. You can read through the documentation
here https://duckdb.org/docs/

#### Setup
1. Change directories at the command line 
   to be inside the `Exercise-8` folder `cd Exercises/Exercise-8`
   
2. Run `docker build --tag=exercise-8 .` to build the `Docker` image.

3. There is a file called `main.py` in the `Exercise-8` directory, this
is where your `DuckDB` code to complete the exercise should go.
   
4. Once you have finished the project or want to test run your code,
   run the following command `docker-compose up run` from inside the `Exercises/Exercise-8` directory

#### Problems Statement
There is a folder called `data` in this current directory, `Exercises/Exercise-8`. Inside this
folder there is a `csv` file. The file is called `electric-cars.csv`. This is an open source
data set about electric vehicles in the state of Washington.

Generally the files look like this ...
```
VIN (1-10),County,City,State,Postal Code,Model Year,Make,Model,Electric Vehicle Type,Clean Alternative Fuel Vehicle (CAFV) Eligibility,Electric Range,Base MSRP,Legislative District,DOL Vehicle ID,Vehicle Location,Electric Utility,2020 Census Tract
5YJ3E1EB4L,Yakima,Yakima,WA,98908,2020,TESLA,MODEL 3,Battery Electric Vehicle (BEV),Clean Alternative Fuel Vehicle Eligible,322,0,14,127175366,POINT (-120.56916 46.58514),PACIFICORP,53077000904
5YJ3E1EA7K,San Diego,San Diego,CA,92101,2019,TESLA,MODEL 3,Battery Electric Vehicle (BEV),Clean Alternative Fuel Vehicle Eligible,220,0,,266614659,POINT (-117.16171 32.71568),,06073005102
7JRBR0FL9M,Lane,Eugene,OR,97404,2021,VOLVO,S60,Plug-in Hybrid Electric Vehicle (PHEV),Not eligible due to low battery range,22,0,,144502018,POINT (-123.12802 44.09573),,41039002401
```

Your job is to complete each one of the tasks listed below, in order, as they depend on each other.

1. create a DuckDB Table including DDL and correct data types that will hold the data in this CSV file.
 - inspect data types and make DDL that makes sense. Don't just `String` everything.

2. Read the provided `CSV` file into the table you created.

3. Calculate the following analytics.
 - Count the number of electric cars per city.
 - Find the top 3 most popular electric vehicles.
 - Find the most popular electric vehicle in each postal code.
 - Count the number of electric cars by model year. Write out the answer as parquet files partitioned by year.


Note: Your `DuckDB` code should be encapsulated inside functions or methods.

Extra Credit: Unit test your `DuckDB` code.
