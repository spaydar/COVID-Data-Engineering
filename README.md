# COVID-19 Data Engineering

## Purpose

The purpose of this project is to implement an ETL pipeline of United States COVID-19, population, and poverty data to a columnar-formatted data lake. The resulting columnar database can be used to run analytical queries against in order to find insights about the spread of and fatalities caused by COVID-19 as they relate to time, location, population size, ruralness/urban-ness, economic dependence type, and poverty. This project implements the ETL pipeline using Python and the PySpark API to read datasets into Spark dataframes, clean and transform data, and write out the results in parquet columnar format as a set of fact and dimension tables in a star schema.

## Datasets

The `datasets/us-counties.csv` and `datasets/us-states.csv` files contain historical COVID-19 case and death data [from The New York Times](https://github.com/nytimes/covid-19-data), based on reports from state and local health agencies. They provide daily case and death data up to but not including the current day for US counties and states, respectively. See [here](https://github.com/nytimes/covid-19-data#methodology-and-definitions) for more information about their methodology and definitions.

The `datasets/PopulationEstimates.csv` and `datasets/PovertyEstimates.csv` files contain estimates of US population and poverty [from the Economic Research Service at the US Department of Agriculture](https://www.ers.usda.gov/data-products/county-level-data-sets/download-data/), based on data from the US Census Bureau. See [here](https://www.ers.usda.gov/data-products/county-level-data-sets/documentation/) for documentation and further information about methodology.

### Dataset Dictionary