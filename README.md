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

`datasets/us-states.csv`
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

The unique identifier for the fact table is the combination of `date` and `fips` as the NYT COVID datasets provide data per location per day, meaning that there are multiple entries per `fips` (location). The date dimension table is uniquely identified by `date`, and the rest of the dimension tables are uniquely identified by `fips`.

### Data Model Justification

I chose to model the data as a star schema because the purpose of the database is to find insights about the spread of and fatalities caused by COVID-19 as they relate to time, location, population size, ruralness/urban-ness, economic dependence type, and poverty. Since the main factor considered is COVID spread and fatality, I made a `covid_facts` table to store all facts about COVID cases and deaths per `fips` per day; this means that state-level and county-level facts both reside in the same table. 

It is intuitive that the `date` and `fips` fact columns could have their own dimension tables to describe the details of those attributes. The `date` timestamp is easily broken down into smaller date components, which can be used in different analyses that relate to COVID spread throughout a given week, month or year. The `fips` code is meaningless by itself without information about the name of the state, the state abbreviation, and the county name associated with a given code, and thus there is a location dimension table to avoid redundant descriptors in the fact table.

While the population and poverty data largely contain numeric types that could be used as facts in a different context, they are used as additional dimesions of location as the data they provide cover a year-long timescale whereas the NYT COVID data is provided on a daily timescale. Therefore, it is more useful to consider these data as additional descriptors for a given location that might provide insights about the spread of COVID as it relates to population size, population density, and poverty. Only the most recent estimates for population and poverty are included in the final model in order to provide the most accurate data to make analyses from.

### Data Cleaning

The following steps were necessary to clean the data such that it could be joined and transformed to fit the desired data model:
1. `datasets/us-counties.csv`
    - Drop rows where `fips` is null
        - This is necessary since `fips` is part of the unique identifier for the fact table
        
2. `datasets/us-states.csv`
    - Append '000' to `fips` values to complete state-county 5-digit format
        - This allows the states' `fips` format to conform that of the other datasets, allowing them to be joined on this column
        
3. `datasets/PopulationEstimates.csv`
    - Prepend '0' to `fips` codes that only have a single digit in the state code
    - Remove commas from numeric values using regular expressions in order to allow casting to integer types
    
4. `datasets/PovertyEstimates.csv`
    - Prepend '0' to `fips` codes that only have a single digit in the state code
    - Transform data to have dedicated columns for attributes and one row per `fips`
        - This involves copying attributes to their own columns and creating empty columns for other attributes for each row, grouping by `fips`, then reducing to one row per `fips`
        - This is necessary to ensure that there is one data type per column and that column type is not dependent on another column

## ETL Pipeline

The ETL pipeline begins by extracting data from the CSV files in the `datasets` directory into Spark dataframes, sometimes with an explicit schema. This is done using commands that include the following:

    schema_counties = StructType([
        StructField("date", TimestampType()),
        StructField("county", StringType()),
        StructField("state", StringType()),
        StructField("fips", StringType()),
        StructField("cases", IntegerType()),
        StructField("deaths", IntegerType())
    ])
    
    county_cases_filepath = input_data + "us-counties.csv"
    
    df_counties = spark.read.csv(county_cases_filepath, header=True, schema=schema_counties).dropna(subset=["fips"])
    
Then, data is cleaned and transformed in order to conform to the target data model using commands that include the following:

    df_states = df_states.withColumn("fips", concat(df_states["fips"], lit("000")))
    
    covid_facts = df_counties.select("date", "fips", "cases", "deaths").union(df_states.select("date", "fips", "cases", "deaths"))
    
    ...
    
    location_dim = df_states.select("fips", "state").dropDuplicates().withColumn("county", lit(None).cast(StringType())) \
                .union(df_counties.select("fips", "state", "county").dropDuplicates())
    
    temp_location_df = df_population.withColumn("state_abrv", df_population["State"]).withColumn("fips_state", substring(df_population["FIPStxt"], 1, 2)) \
                        .select("fips_state", "state_abrv").dropDuplicates()
    
    location_dim = location_dim.join(temp_location_df, [substring(location_dim.fips, 1, 2) == temp_location_df.fips_state], "left").drop("fips_state")
        
    ...
    
    df_poverty = df_poverty.withColumn("FIPStxt", when(length("FIPStxt") == 4, concat(lit("0"), col("FIPStxt"))) \
                                         .otherwise(col("FIPStxt")))
    
    df_poverty = df_poverty.withColumn("fips", df_poverty["FIPStxt"]) \
                    .withColumn("POV_ALL_2019", when(df_poverty["Attribute"] == "POVALL_2019", df_poverty["Value"]).cast(IntegerType())) \
                    .withColumn("PCT_POV_ALL_2019", when(df_poverty["Attribute"] == "PCTPOVALL_2019", df_poverty["Value"]).cast(FloatType())) \
                    .withColumn("POV_0-17_2019", when(df_poverty["Attribute"] == "POV017_2019", df_poverty["Value"]).cast(IntegerType())) \
                    .withColumn("PCT_POV_0-17_2019", when(df_poverty["Attribute"] == "PCTPOV017_2019", df_poverty["Value"]).cast(FloatType())) \
                    .withColumn("POV_5-17_2019", when(df_poverty["Attribute"] == "POV517_2019", df_poverty["Value"]).cast(IntegerType())) \
                    .withColumn("PCT_POV_5-17_2019", when(df_poverty["Attribute"] == "PCTPOV517_2019", df_poverty["Value"]).cast(FloatType())) \
                    .withColumn("MED_HH_INC_2019", when(df_poverty["Attribute"] == "MEDHHINC_2019", df_poverty["Value"]).cast(IntegerType())) \
                    .withColumn("POV_0-4_2019", when(df_poverty["Attribute"] == "POV04_2019", df_poverty["Value"]).cast(IntegerType())) \
                    .withColumn("PCT_POV_0-4_2019", when(df_poverty["Attribute"] == "PCTPOV04_2019", df_poverty["Value"]).cast(FloatType()))
    
    poverty_dim = df_poverty.drop("FIPStxt", "Stabr", "Area_name", "Attribute", "Value").groupBy("fips") \
        .agg(collect_list("POV_ALL_2019").getItem(0).alias("POV_ALL_2019"), \
             collect_list("PCT_POV_ALL_2019").getItem(0).alias("PCT_POV_ALL_2019"), \
             collect_list("POV_0-17_2019").getItem(0).alias("POV_0-17_2019"), \
             collect_list("PCT_POV_0-17_2019").getItem(0).alias("PCT_POV_0-17_2019"), \
             collect_list("POV_5-17_2019").getItem(0).alias("POV_5-17_2019"), \
             collect_list("PCT_POV_5-17_2019").getItem(0).alias("PCT_POV_5-17_2019"), \
             collect_list("MED_HH_INC_2019").getItem(0).alias("MED_HH_INC_2019"), \
             collect_list("POV_0-4_2019").getItem(0).alias("POV_0-4_2019"), \
             collect_list("PCT_POV_0-4_2019").getItem(0).alias("PCT_POV_0-4_2019")
            )

Finally, the resulting dataframes are written to disk in parquet file format using commands that include the following:

    date_dim.write.partitionBy("year", "month").parquet(output_data + "date_dim.parquet")
    
    ...
    
    population_dim.write.parquet(output_data + "population_dim.parquet")

### Data Quality Checks

Two data quality checks are performed on the fact table:
1. Check that the fact table has greater than zero rows
2. Check that there are no null values in either the `date` or `fips` columns of the fact table as these jointly serve as the unique identifier for each row and connect the fact table to all the dimension tables

### Technology Justification

I chose to use Spark to implement this project since it is an intuitive data processing tool that provides higher-level functions that make it easy to clean and transform data, an important requirement to meet for this dataset. Furthermore, it is well-suited to handle big data, an ability that will prove necessary as the size of this data increases with the continuation of the COVID-19 pandemic and additional data and schema are introduced to the data model.

## File and Code Structure

The datasets used in this project are stored in the `datasets` directory. All logic to instantiate a SparkSession and run the ETL pipeline are contained in the `etl.py` script, which can be run from the terminal with the `python3 etl.py` command. The resulting star schema dataframes will be written to a directory called `output_data` in parquet file format upon running the pipeline.

## Considerations for Scaling Up

The following describe the necessary steps to be taken in order handle the scale-up scenarios below:

1. The data was increased by 100x
    - The script already uses Apache Spark, a big-data processing tool, so it would not be necessary to change the technology being used. However, it would be necessary to deploy the solution in a multi-node context such as AWS EMR in order to handle a 100x increase in data.
    
2. The pipelines would be run on a daily basis by 7am every day
    - In order to run the pipeline everyday, it would be best to automate this process with Apache Airflow. Airflow is an intuitive technology that allows data engineers to automate data pipelines with auto-retry and notification in case of failure. This tool could also be used to automate a script that retrieves the newest version of the New York Times COVID-19 datasets, which are updated daily.
    
3. The database needed to be accessed by 100+ people
    - If the database needed to be accessed by 100+ people, it would be necessary to host the database on a cloud service that auto-scales resources according to need or host it on managed on-prem servers that are properly load-balanced by a service like NGINX.

## Additional Sample PySpark Commands

Extracts date attributes from a timestamp and creates a dimension table:

    date_dim = covid_facts.select("date").dropDuplicates()
    
    date_dim = date_dim.withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("dayOfWeek", date_format(col("date"), "u").cast(IntegerType())) \
        .withColumn("dayOfMonth", dayofmonth(col("date"))) \
        .withColumn("dayOfYear", dayofyear(col("date"))) \
        .withColumn("weekOfYear", weekofyear(col("date")))
        
Removes commas from numeric types and casts columns to integer and float types:

    population_dim = df_population.withColumn("fips", df_population["FIPStxt"]) \
                    .withColumn("Rural-urban_Continuum_Code_2013", df_population["Rural-urban_Continuum Code_2013"]) \
                    .select("fips","Rural-urban_Continuum_Code_2013","Urban_Influence_Code_2013", \
                           "Economic_typology_2015","POP_ESTIMATE_2019","N_POP_CHG_2019", \
                           "Births_2019","Deaths_2019","NATURAL_INC_2019","INTERNATIONAL_MIG_2019", \
                           "DOMESTIC_MIG_2019","NET_MIG_2019","RESIDUAL_2019","GQ_ESTIMATES_2019", \
                           "R_birth_2019","R_death_2019","R_NATURAL_INC_2019","R_INTERNATIONAL_MIG_2019", \
                           "R_DOMESTIC_MIG_2019","R_NET_MIG_2019") \
                    .withColumn("Rural-urban_Continuum_Code_2013", col('Rural-urban_Continuum_Code_2013').cast(IntegerType())) \
                    .withColumn("Urban_Influence_Code_2013", col('Urban_Influence_Code_2013').cast(IntegerType())) \
                    .withColumn("Economic_typology_2015", col('Economic_typology_2015').cast(IntegerType())) \
                    .withColumn("POP_ESTIMATE_2019", regexp_replace('POP_ESTIMATE_2019', ',', '').cast(IntegerType())) \
                    .withColumn("N_POP_CHG_2019", regexp_replace('N_POP_CHG_2019', ',', '').cast(IntegerType())) \
                    .withColumn("Births_2019", regexp_replace('Births_2019', ',', '').cast(IntegerType())) \
                    .withColumn("Deaths_2019", regexp_replace('Deaths_2019', ',', '').cast(IntegerType())) \
                    .withColumn("NATURAL_INC_2019", regexp_replace('NATURAL_INC_2019', ',', '').cast(IntegerType())) \
                    .withColumn("INTERNATIONAL_MIG_2019", regexp_replace('INTERNATIONAL_MIG_2019', ',', '').cast(IntegerType())) \
                    .withColumn("DOMESTIC_MIG_2019", regexp_replace('DOMESTIC_MIG_2019', ',', '').cast(IntegerType())) \
                    .withColumn("NET_MIG_2019", regexp_replace('NET_MIG_2019', ',', '').cast(IntegerType())) \
                    .withColumn("RESIDUAL_2019", regexp_replace('RESIDUAL_2019', ',', '').cast(IntegerType())) \
                    .withColumn("GQ_ESTIMATES_2019", regexp_replace('GQ_ESTIMATES_2019', ',', '').cast(IntegerType())) \
                    .withColumn("R_birth_2019", col('R_birth_2019').cast(FloatType())) \
                    .withColumn("R_death_2019", col('R_death_2019').cast(FloatType())) \
                    .withColumn("R_NATURAL_INC_2019", col('R_NATURAL_INC_2019').cast(FloatType())) \
                    .withColumn("R_INTERNATIONAL_MIG_2019", col('R_INTERNATIONAL_MIG_2019').cast(FloatType())) \
                    .withColumn("R_DOMESTIC_MIG_2019", col('R_DOMESTIC_MIG_2019').cast(FloatType())) \
                    .withColumn("R_NET_MIG_2019", col('R_NET_MIG_2019').cast(FloatType()))