from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DecimalType

schema_country_wise_latest = StructType([ 
        StructField("Country/Region",StringType(),True), 
        StructField("Confirmed",IntegerType(),True), 
        StructField("Deaths",IntegerType(),True), 
        StructField("Recovered", IntegerType(), True), 
        StructField("Active", IntegerType(), True), 
        StructField("New cases", IntegerType(), True),
        StructField("New deaths", IntegerType(), True),
        StructField("New recovered", IntegerType(), True),
        StructField("Deaths / 100 Cases", DecimalType(), True),
        StructField("Recovered / 100 Cases", DecimalType(), True),
        StructField("Deaths / 100 Recovered", DecimalType(), True),
        StructField("Confirmed last week", IntegerType(), True),
        StructField("1 week change", IntegerType(), True),
        StructField("1 week % increase", DecimalType(), True),
        StructField("WHO Region", StringType(), True)
    ])

schema_covid_19_clean_complete = StructType([ 
        StructField("Province/State",StringType(),True), 
        StructField("Country/Region",StringType(),True), 
        StructField("Lat",DecimalType(),True), 
        StructField("Long", DecimalType(), True), 
        StructField("Date", StringType(), True), 
        StructField("Confirmed", IntegerType(), True),
        StructField("Deaths", IntegerType(), True),
        StructField("Recovered", IntegerType(), True),
        StructField("Active", IntegerType(), True),
        StructField("WHO Region", StringType(), True)
    ])

schema_day_wise = StructType([ 
        StructField("Date",StringType(),True),
        StructField("Confirmed",IntegerType(),True), 
        StructField("Deaths",IntegerType(),True), 
        StructField("Recovered", IntegerType(), True), 
        StructField("Active", IntegerType(), True), 
        StructField("New cases", IntegerType(), True),
        StructField("New deaths", IntegerType(), True),
        StructField("New recovered", IntegerType(), True),
        StructField("Deaths / 100 Cases", DecimalType(), True),
        StructField("Recovered / 100 Cases", DecimalType(), True),
        StructField("Deaths / 100 Recovered", DecimalType(), True),
        StructField("No. of countries", IntegerType(), True)
    ])

schema_full_grouped = StructType([ 
        StructField("Date",StringType(),True),
        StructField("Country/Region",StringType(),True), 
        StructField("Confirmed",IntegerType(),True), 
        StructField("Deaths", IntegerType(), True), 
        StructField("Recovered", IntegerType(), True), 
        StructField("Active", IntegerType(), True),
        StructField("New cases", IntegerType(), True),
        StructField("New deaths", IntegerType(), True),
        StructField("New recovered", IntegerType(), True),
        StructField("WHO Region", StringType(), True)
    ])


schema_usa_county_wise = StructType([ 
        StructField("UID",StringType(),True),
        StructField("iso2",StringType(),True), 
        StructField("iso3", StringType(), True), 
        StructField("code3", StringType(), True), 
        StructField("FIPS", DecimalType(), True),
        StructField("Admin2", StringType(), True),
        StructField("Province_State", StringType(), True),
        StructField("Country_Region", StringType(), True),
        StructField("Lat", DecimalType(), True),
        StructField("Long_", DecimalType(), True),
        StructField("Combined_Key", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Confirmed", IntegerType(), True),
        StructField("Deaths", IntegerType(), True)
    ])


schema_worldometer_data = StructType([ 
        StructField("Country/Region",StringType(),True),
        StructField("Continent",StringType(),True), 
        StructField("Population", IntegerType(), True), 
        StructField("TotalCases", IntegerType(), True), 
        StructField("NewCases", IntegerType(), True),
        StructField("TotalDeaths", IntegerType(), True),
        StructField("NewDeaths", IntegerType(), True),
        StructField("TotalRecovered", IntegerType(), True),
        StructField("NewRecovered", IntegerType(), True),
        StructField("ActiveCases", IntegerType(), True),
        StructField("Serious,Critical", IntegerType(), True),
        StructField("Tot Cases/1M pop", IntegerType(), True),
        StructField("Deaths/1M pop", IntegerType(), True),
        StructField("TotalTests", IntegerType(), True),
        StructField("Tests/1M pop", IntegerType(), True),
        StructField("WHO Region", StringType(), True)
    ])
