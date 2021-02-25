# COVID-19 Data Engineering

## Purpose

The purpose of this project is to implement an ETL pipeline of United States COVID-19, population, and poverty data to a columnar-formatted data lake. The resulting columnar database can be used to run analytical queries against in order to find insights about the spread of and fatalities caused by COVID-19 as they relate to time, location, population size, ruralness/urban-ness, economic dependence type, and poverty. This project implements the ETL pipeline using Python and the PySpark API to read datasets into Spark dataframes, clean and transform data, and write out the results in parquet columnar format as a set of fact and dimension tables in a star schema.

## Datasets

The `datasets/us-counties.csv` and `datasets/us-states.csv` files contain historical COVID-19 case and death data [from The New York Times](https://github.com/nytimes/covid-19-data), based on reports from state and local health agencies. They provide daily case and death data up to but not including the current day for US counties and states, respectively. See [here](https://github.com/nytimes/covid-19-data#methodology-and-definitions) for more information about their methodology and definitions.

The `datasets/PopulationEstimates.csv` and `datasets/PovertyEstimates.csv` files contain estimates of US population and poverty [from the Economic Research Service at the US Department of Agriculture](https://www.ers.usda.gov/data-products/county-level-data-sets/download-data/), based on data from the US Census Bureau. See [here](https://www.ers.usda.gov/data-products/county-level-data-sets/documentation/) for documentation and further information about methodology.

### Dataset Dictionary

`datasets/us-counties.csv`
- **date**: The date the record is for in `YYYY-MM-DD` format
- **county**: The name of the county
- **state**: The full name of the state that the county is in
- **fips**: The 5-digit [Federal Information Processing Series](https://www.census.gov/quickfacts/fact/note/US/fips) code for the state and county, in `SSCCC` format
- **cases**: The cumulative number of laboratory-confirmed and probable cases of COVID-19 in the county to date, according to [this standardized criteria](https://int.nyt.com/data/documenthelper/6908-cste-interim-20-id-01-covid-19/85d47e89b637cd643d50/optimized/full.pdf)
- **deaths**: The cumulative number of deaths related to COVID-19 in the county to date, according to the above criteria

`datasets/us-counties.csv`
- **date**: The date the record is for in `YYYY-MM-DD` format
- **state**: The full name of the state
- **fips**: The 2-digit [Federal Information Processing Series](https://www.census.gov/quickfacts/fact/note/US/fips) code for the state, in `SS` format
- **cases**: The cumulative number of laboratory-confirmed and probable cases of COVID-19 in the state to date, according to [this standardized criteria](https://int.nyt.com/data/documenthelper/6908-cste-interim-20-id-01-covid-19/85d47e89b637cd643d50/optimized/full.pdf)
- **deaths**: The cumulative number of deaths related to COVID-19 in the state to date, according to the above criteria

`datasets/PopulationEstimates.csv`
- **FIPStxt**: The 1-, 4-, or 5-digit [Federal Information Processing Series](https://www.census.gov/quickfacts/fact/note/US/fips) code for the location, in `SCCC` or `SSCCC` format except for the 1-digit national data record