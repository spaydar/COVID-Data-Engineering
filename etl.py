from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, when, length, substring, collect_list, regexp_replace, year, month, dayofmonth, dayofyear, date_format, weekofyear
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType

def create_spark_session():
    '''
    Instantiates and returns a SparkSession.
    '''
    spark = SparkSession \
        .builder \
        .appName("COVID Data Engineering") \
        .getOrCreate()
    return spark

def etl(spark, input_data, output_data):
    '''
    Reads data from the dataset files into Spark dataframes, cleans the data, organizes required columns into a star schema,
        and writes the schema dataframes to the output directory in parquet file format. In some cases,
        the output parquet files are partitioned by a particular column.
        
        Parameters:
            spark: The current SparkSession
            input_data: The directory housing the source dataset files
            output_data: The target directory for the output star schema parquet files
    '''
    
    schema_counties = StructType([
        StructField("date", TimestampType()),
        StructField("county", StringType()),
        StructField("state", StringType()),
        StructField("fips", StringType()),
        StructField("cases", IntegerType()),
        StructField("deaths", IntegerType())
    ])
    
    county_cases_filepath = input_data + "us-counties.csv"
    
    df_counties = spark.read.csv(county_cases_filepath, header=True, schema=schema_counties)
    
    
    
    schema_states = StructType([
        StructField("date", TimestampType()),
        StructField("state", StringType()),
        StructField("fips", StringType()),
        StructField("cases", IntegerType()),
        StructField("deaths", IntegerType())
    ])
    
    state_cases_filepath = input_data + "us-states.csv"
    
    df_states = spark.read.csv(state_cases_filepath, header=True, schema=schema_states)
    
    df_states = df_states.withColumn("fips", concat(df_states["fips"], lit("000")))
    
    
    
    covid_facts = df_counties.select("date", "fips", "cases", "deaths").union(df_states.select("date", "fips", "cases", "deaths"))
    
    covid_facts.write.partitionBy("date").parquet(output_data + "covid_facts.parquet")
    
    
    
    date_dim = covid_facts.select("date").dropDuplicates()
    
    date_dim = date_dim.withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("dayOfWeek", date_format(col("date"), "u").cast(IntegerType())) \
        .withColumn("dayOfMonth", dayofmonth(col("date"))) \
        .withColumn("dayOfYear", dayofyear(col("date"))) \
        .withColumn("weekOfYear", weekofyear(col("date")))
    
    date_dim.write.partitionBy("year", "month").parquet(output_data + "date_dim.parquet")
    
    
    
    population_filepath = input_data + "PopulationEstimates.csv"
    
    df_population = spark.read.csv(population_filepath, header=True)
    
    df_population = df_population.withColumn("FIPStxt", when(length("FIPStxt") == 4, concat(lit("0"), col("FIPStxt"))) \
                                         .otherwise(col("FIPStxt")))
    
    
    
    location_dim = df_states.select("fips", "state").dropDuplicates().withColumn("county", lit(None).cast(StringType())) \
                .union(df_counties.select("fips", "state", "county").dropDuplicates())
    
    temp_location_df = df_population.withColumn("state_abrv", df_population["State"]).withColumn("fips_state", substring(df_population["FIPStxt"], 1, 2)) \
                        .select("fips_state", "state_abrv").dropDuplicates()
    
    location_dim = location_dim.join(temp_location_df, [substring(location_dim.fips, 1, 2) == temp_location_df.fips_state], "left").drop("fips_state")
    
    location_dim.write.partitionBy("state").parquet(output_data + "location_dim.parquet")
    
    
    
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
    
    population_dim.write.parquet(output_data + "population_dim.parquet")
    
    
    
    poverty_filepath = input_data + "PovertyEstimates.csv"
    
    df_poverty = spark.read.csv(poverty_filepath, header=True)
    
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
       
    poverty_dim.write.parquet(output_data + "poverty_dim.parquet")

def main():
    '''
    Creates a SparkSession, extracts data from the dataset CSV files, transforms them, and writes them to disk as partitioned parquet files.
    '''
    spark = create_spark_session()
    
    input_data = "datasets/"
    output_data = "output_data/"
    
    etl(spark, input_data, output_data)
    
    spark.stop()
    

if __name__ == "__main__":
    main()