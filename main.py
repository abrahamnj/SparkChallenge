from pyspark.sql import SparkSession
from dependencies import logging
from dependencies.structure_types import schema_country_wise_latest, \
    schema_covid_19_clean_complete, \
    schema_day_wise, \
    schema_full_grouped, \
    schema_usa_county_wise, \
    schema_worldometer_data
import pandas as pd



def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[]):
    spark_builder = (SparkSession.builder.master(master)
                .appName(app_name))

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_sess.sparkContext.setLogLevel('WARN')
    spark_logger = logging.Log4j(spark_sess)

    return spark_sess, spark_logger


def main():
    spark, spark_logger = start_spark(app_name='my_local_job')

    # Carga de la tabla Organizacion
    df_country_wise_latest = extraxt_data(spark, 'data/country_wise_latest.csv', schema_country_wise_latest) 
    df_covid_19_clean_complete = extraxt_data(spark, 'data/covid_19_clean_complete.csv', schema_covid_19_clean_complete)
    df_day_wise = extraxt_data(spark, 'data/day_wise.csv', schema_day_wise)
    df_full_grouped = extraxt_data(spark, 'data/full_grouped.csv', schema_full_grouped)
    df_usa_county_wise = extraxt_data(spark, 'data/usa_county_wise.csv', schema_usa_county_wise)
    df_worldometer_data = extraxt_data(spark, 'data/worldometer_data.csv', schema_worldometer_data)


    df_country_wise_latest = df_country_wise_latest.withColumnRenamed("Country/Region","Country_Region") \
        .withColumnRenamed("New cases","New_cases") \
        .withColumnRenamed("New deaths","New_deaths") \
        .withColumnRenamed("New recovered","New_recovered") \
        .withColumnRenamed("Deaths / 100 Cases","Deaths_100_Cases") \
        .withColumnRenamed("Recovered / 100 Cases","Recovered_100_Cases") \
        .withColumnRenamed("Deaths / 100 Recovered","Deaths_100_Recovered") \
        .withColumnRenamed("Confirmed last week","Confirmed_last_week") \
        .withColumnRenamed("1 week change","one_week_change") \
        .withColumnRenamed("1 week % increase","one_week_percent_increase") \
        .withColumnRenamed("WHO Region","WHO_Region")

    df_covid_19_clean_complete = df_covid_19_clean_complete.withColumnRenamed("Province/State","Province_State") \
        .withColumnRenamed("Country/Region","Country_Region") \
        .withColumnRenamed("WHO Region","WHO_Region")

    df_day_wise = df_day_wise.withColumnRenamed("New cases","New_cases") \
        .withColumnRenamed("New deaths","New_deaths") \
        .withColumnRenamed("New recovered","New_recovered") \
        .withColumnRenamed("Deaths / 100 Cases","Deaths_100_Cases") \
        .withColumnRenamed("Recovered / 100 Cases","Recovered_100_Cases") \
        .withColumnRenamed("Deaths / 100 Recovered","Deaths_100_Recovered") \
        .withColumnRenamed("No. of countries","Nro_of_countries")

    df_full_grouped = df_full_grouped.withColumnRenamed("Country/Region","Country_Region") \
        .withColumnRenamed("New cases","New_cases") \
        .withColumnRenamed("New deaths","New_deaths") \
        .withColumnRenamed("New recovered","New_recovered") \
        .withColumnRenamed("WHO Region","WHO_Region")
    
    df_worldometer_data = df_worldometer_data.withColumnRenamed("Country/Region","Country_Region") \
        .withColumnRenamed("Tot Cases/1M pop","Tot_Cases_1M_pop") \
        .withColumnRenamed("Serious,Critical","Serious_Critical") \
        .withColumnRenamed("Deaths/1M pop","Deaths_1M_pop") \
        .withColumnRenamed("Tests/1M pop","Tests_1M_pop") \
        .withColumnRenamed("WHO Region","WHO_Region")
    
    spark_logger.warn("df_country_wise_latest")
    df_country_wise_latest.show(20, False)
    spark_logger.warn("df_covid_19_clean_complete")
    df_covid_19_clean_complete.show(20, False)
    spark_logger.warn("df_day_wise")
    df_day_wise.show(20, False)
    spark_logger.warn("df_full_grouped")
    df_full_grouped.show(20, False)
    spark_logger.warn("df_usa_county_wise")
    df_usa_county_wise.show(20, False)
    spark_logger.warn("df_worldometer_data")
    df_worldometer_data.show(20, False)
    
    save_as_parquet(df_country_wise_latest, "country_wise_latest")
    save_as_parquet(df_covid_19_clean_complete, "covid_19_clean_complete")
    save_as_parquet(df_day_wise, "day_wise")
    save_as_parquet(df_full_grouped, "full_grouped")
    save_as_parquet(df_usa_county_wise, "usa_county_wise")
    save_as_parquet(df_worldometer_data, "worldometer_data")

    # Cargar los datasets utilizando Pandas y mantenerlos en formato parquet.
    pd_df_country_wise_latest = read_parquet_pandas('country_wise_latest')
    pd_df_covid_19_clean_complete = read_parquet_pandas('covid_19_clean_complete')
    pd_df_day_wise = read_parquet_pandas('day_wise')
    pd_df_full_grouped = read_parquet_pandas('full_grouped')
    pd_df_usa_county_wise = read_parquet_pandas('usa_county_wise')
    pd_df_worldometer_data = read_parquet_pandas('worldometer_data')

    spark.stop() # Liberar recursos
    return None
    


def extraxt_data(spark, path, schema=None, format='csv'):
    df = (spark.read.format(format)
        .option("header", True)
        .schema(schema)
        .load(path))
    return df

def read_parquet_pandas(path):
    df = pd.read_parquet(
        f'data_parquet/{path}', 
        engine='auto', 
        columns=None, 
        storage_options=None, 
        use_nullable_dtypes=False)
    return df


def save_as_parquet(df, file_name):
    df.repartition(2).write.mode('overwrite').parquet(f'data_parquet/{file_name}')
    return None


# Entry point for PySpark ETL application
if __name__ == '__main__':
    main()
    exit()
    