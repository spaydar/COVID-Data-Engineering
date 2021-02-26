# COVID-19 Data Engineering

## Purpose

The purpose of this project is to implement an ETL pipeline of United States COVID-19, population, and poverty data to a columnar-formatted data lake. The resulting columnar database can be used to run analytical queries against in order to find insights about the spread of and fatalities caused by COVID-19 as they relate to time, location, population size, ruralness/urban-ness, economic dependence type, and poverty. This project implements the ETL pipeline using Python and the PySpark API to read datasets into Spark dataframes, clean and transform data, and write out the results in parquet columnar format as a set of fact and dimension tables in a star schema.

## Datasets

The `datasets/us-counties.csv` and `datasets/us-states.csv` files contain historical COVID-19 case and death data [from The New York Times](https://github.com/nytimes/covid-19-data), based on reports from state and local health agencies. They provide daily case and death data up to but not including the current day for US counties and states, respectively. See [here](https://github.com/nytimes/covid-19-data#methodology-and-definitions) for more information about their methodology and definitions.

The `datasets/PopulationEstimates.csv` and `datasets/PovertyEstimates.csv` files contain annual estimates of US population and poverty [from the Economic Research Service at the US Department of Agriculture](https://www.ers.usda.gov/data-products/county-level-data-sets/download-data/), based on data from the US Census Bureau. See [here](https://www.ers.usda.gov/data-products/county-level-data-sets/documentation/) for documentation and further information about methodology.

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
- **State**: The 2-letter abbreviation for the state
- **Area_Name**: The full name of the county or state
- **Rural-urban_Continuum Code_2003/2013**: A 1-digit rating on a 9-point scale that distinguishes metropolitan counties by the population size of their metro area, and nonmetropolitan counties by degree of urbanization and adjacency to a metro area. See [here](https://www.ers.usda.gov/data-products/rural-urban-continuum-codes/documentation/) for documentation and methodology
- **Urban_Influence_Code_2003/2013**: A 1-2 digit rating on a 12-point scale that distinguishes metropolitan counties by population size of their metro area, and nonmetropolitan counties by size of the largest city or town and proximity to metro and micropolitan areas. See [here](https://www.ers.usda.gov/data-products/urban-influence-codes/documentation/) for documentation and methodology
- **Economic_typology_2015**: A 1-digit rating on a 6-point scale that classify all U.S. counties according to six mutually exclusive categories of economic dependence and six overlapping categories of policy-relevant themes. See [here](https://www.ers.usda.gov/data-products/county-typology-codes/documentation/) for documentation and methodology
- **CENSUS_2010_POP**: Resident Census 2010 Population as of 4/1/2010
- **ESTIMATES_BASE_2010**: Resident total population estimate base for 4/1/2010
- **POP_ESTIMATE_2010-2019**: Resident total population estimate for July 1st of each given year
- **N_POP_CHG_2010-2019**: Numeric change in resident total population over the full year prior to July 1st of the given year
- **Births_2010-2019**: Births over the full year prior to July 1st of the given year
- **Deaths_2010-2019**: Deaths over the full year prior to July 1st of the given year
- **NATURAL_INC_2010-2019**: Natural increase in population over the full year prior to July 1st of the given year
- **INTERNATIONAL_MIG_2010-2019**: Net international migration over the full year prior to July 1st of the given year
- **DOMESTIC_MIG_2010-2019**: Net domestic migration over the full year prior to July 1st of the given year
- **NET_MIG_2010-2019**: Net migration over the full year prior to July 1st of the given year
- **RESIDUAL_2010-2019**: Residual over the full year prior to July 1st of the given year
- **GQ_ESTIMATES_BASE_2010**: Group quarters total population estimate base for 4/1/2010
- **GQ_ESTIMATES_2010-2019**: Group quarters total population estimate for July 1st of each given year
- **R_birth_2011-2019**: Birth rate over the full year prior to July 1st of the given year
- **R_death_2011-2019**: Death rate over the full year prior to July 1st of the given year
- **R_NATURAL_INC_2011-2019**: Natural increase rate over the full year prior to July 1st of the given year
- **R_INTERNATIONAL_MIG_2011-2019**: Net international migration rate over the full year prior to July 1st of the given year
- **R_DOMESTIC_MIG_2011-2019**: Net domestic migration rate over the full year prior to July 1st of the given year
- **R_NET_MIG_2011-2019**: Net migration rate over the full year prior to July 1st of the given year

`datasets/PovertyEstimates.csv`
- **FIPStxt**: The 1-, 4-, or 5-digit [Federal Information Processing Series](https://www.census.gov/quickfacts/fact/note/US/fips) code for the location, in `SCCC` or `SSCCC` format except for the 1-digit national data record
- **Stabr**: The 2-letter abbreviation for the state
- **Area_name**: The full name of the county or state
- **Attribute**: The type of data in the "Value" column. Either an estimate of people in poverty in a given demographic, a percentage of a population estimated to be in poverty, or a 90% confidence interval lower/upper bound of an estimate
- **Value**: An estimate of one of the types described above

## Data Lake Model

The data lake consists of 5 parquet files modeled according to the following star schema:

###### Fact Table
1. **covid_facts** - facts about cumulative COVID-19 cases and deaths in a FIPS-defined county or state, to a given date
    - *date*: The timestamp, in `YYYY-MM-DD` format, to which cases and deaths are cumulative
    - *fips*: The 5-digit [Federal Information Processing Series](https://www.census.gov/quickfacts/fact/note/US/fips) code for the state and county, in `SSCCC` format. String type
    - *cases*: The cumulative number of laboratory-confirmed and probable cases of COVID-19 in the county to date, according to [this standardized criteria](https://int.nyt.com/data/documenthelper/6908-cste-interim-20-id-01-covid-19/85d47e89b637cd643d50/optimized/full.pdf). Integer type
    - *deaths*: The cumulative number of deaths related to COVID-19 in the county to date, according to the above criteria. Integer type

###### Dimension Tables
2. **date_dim** - date timestamps of records in **covid_facts** broken down into specific units
    - *date*: A timestamp in `YYYY-MM-DD` format
    - *year*: An integer representation of the year
    - *month*: An integer representation of the month
    - *dayOfWweek*: An integer representation of the day of the week
    - *dayOfMonth*: An integer representation of the day of the month
    - *dayOfYear*: An integer representation of the day of the year
    - *weekOfYear*: An integer representation of the week of the year
    
3. **location_dim** - location descriptions associated with FIPS codes in **covid_facts**
    - *fips*: The 5-digit [Federal Information Processing Series](https://www.census.gov/quickfacts/fact/note/US/fips) code for the state and county, in `SSCCC` format. String type
    - *state*: The full name of the state. String type
    - *county*: The full name of the county, if applicable. String type
    - *state_abrv*: he 2-letter abbreviation for the state. String type
    
4. **population_dim** - the most recent population data for a given FIPS-defined county or state
    - *fips*: The 5-digit [Federal Information Processing Series](https://www.census.gov/quickfacts/fact/note/US/fips) code for the state and county, in `SSCCC` format. String type
    - *Rural-urban_Continuum_Code_2013*: A 1-digit rating on a 9-point scale that distinguishes metropolitan counties by the population size of their metro area, and nonmetropolitan counties by degree of urbanization and adjacency to a metro area. Integer type
    - *Urban_Influence_Code_2013*: A 1-2 digit rating on a 12-point scale that distinguishes metropolitan counties by population size of their metro area, and nonmetropolitan counties by size of the largest city or town and proximity to metro and micropolitan areas. Integer type
    - *Economic_typology_2015*: A 1-digit rating on a 6-point scale that classify all U.S. counties according to six mutually exclusive categories of economic dependence and six overlapping categories of policy-relevant themes. Integer Type
    - *POP_ESTIMATE_2019*: Resident total population estimate for July 1st 2019. Integer type
    - *N_POP_CHG_2019*: Numeric change in resident total population over the full year prior to July 1st 2019. Integer type
    - *Births_2019*: Births over the full year prior to July 1st 2019. Integer type
    - *Deaths_2019*: Deaths over the full year prior to July 1st 2019. Integer type
    - *NATURAL_INC_2019*: Natural increase in population over the full year prior to July 1st 2019. Integer type
    - *INTERNATIONAL_MIG_2019*: Net international migration over the full year prior to July 1st 2019. Integer type
    - *DOMESTIC_MIG_2019*: Net domestic migration over the full year prior to July 1st 2019. Integer type
    - *NET_MIG_2019*: Net migration over the full year prior to July 1st 2019. Integer type
    - *RESIDUAL_2019*: Residual over the full year prior to July 1st 2019. Integer type
    - *GQ_ESTIMATES_2019*: Group quarters total population estimate for July 1st 2019. Integer type
    - *R_birth_2019*: Birth rate over the full year prior to July 1st 2019. Float type
    - *R_death_2019*: Death rate over the full year prior to July 1st 2019. Float type
    - *R_NATURAL_INC_2019*: Natural increase rate over the full year prior to July 1st 2019. Float type
    - *R_INTERNATIONAL_MIG_2019*: Net international migration rate over the full year prior to July 1st 2019. Float type
    - *R_DOMESTIC_MIG_2019*: Net domestic migration rate over the full year prior to July 1st 2019. Float type
    - *R_NET_MIG_2019*: Net migration rate over the full year prior to July 1st 2019. Float type
    
5. **poverty_dim** - the most recent poverty data for a given FIPS-defined county or state
    - *fips*: The 5-digit [Federal Information Processing Series](https://www.census.gov/quickfacts/fact/note/US/fips) code for the state and county, in `SSCCC` format. String type
    - *POV_ALL_2019*: Estimate of people of all ages in poverty 2019. Integer type
    - *PCT_POV_ALL_2019*: Estimated percent of people of all ages in poverty 2019. Float type
    - *POV_0-17_2019*: Estimate of people age 0-17 in poverty 2019. Integer type
    - *PCT_POV_0-17_2019*: Estimated percent of people age 0-17 in poverty 2019. Float type
    - *POV_5-17_2019*: Estimate of related children age 5-17 in families in poverty 2019. Integer type
    - *PCT_POV_5-17_2019*: Estimated percent of related children age 5-17 in families in poverty 2019. Float type
    - *MED_HH_INC_2019*: Estimate of median household income 2019. Integer type
    - *POV_0-4_2019*: Estimate of children ages 0 to 4 in poverty 2019 (available for the U.S. and State total only). Integer type
    - *PCT_POV_0-4_2019*: Estimated percent of children ages 0 to 4 in poverty 2019. Float type

The `covid_facts` parquet file is partitioned by date. The `date_dim` parquet file is partitioned by year and month. The `location_dim` is partitioned by state.

### Data Model Justification



### Data Cleaning


### Data Quality Checks