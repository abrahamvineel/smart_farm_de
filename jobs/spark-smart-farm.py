from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

def main():
    spark = SparkSession.builder.appName("SmartFarmStreaming")\
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1", 
            "org.apache.hadoop:hadoop-aws:3.4.0", 
            "com.amazonaws:aws-java-sdk:1.11.469")\
    .config("spark.hadoop.f2.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.impl.SimpleAWSCredentialsProvider')
    .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    soilMoistureSchema = StructType([
        StructField("id", StringType(), True),
        StructField("sensorId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("moisturePercentage", IntegerType(), True),
        StructField("company", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
    ])

    lightTempSchema = StructType([
        StructField("id", StringType(), True),
        StructField("sensorId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("airTemperature", IntegerType(), True),
        StructField("soilTemperature", IntegerType(), True),
        StructField("leafTemperature", IntegerType(), True),
        StructField("lightIntensity", IntegerType(), True),
        StructField("PAR", IntegerType(), True),
        StructField("DLI", IntegerType(), True),
        StructField("company", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
    ])

    irrigationWaterSchema = StructType([
        StructField("id", StringType(), True),
        StructField("sensorId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("soilWaterPotential", IntegerType(), True),
        StructField("rainfallAmount", IntegerType(), True),
        StructField("rainfallDetection", IntegerType(), True),
        StructField("waterFlowRate", IntegerType(), True),
        StructField("company", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
    ])


    avGpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("sensorId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("company", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
    ])

    livestockSchema = StructType([
        StructField("id", StringType(), True),
        StructField("sensorId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("bodyTemperature", IntegerType(), True),
        StructField("heartRate", IntegerType(), True),
        StructField("respiratoryRate", IntegerType(), True),
        StructField("activityLevel", IntegerType(), True),
        StructField("company", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
    ])

    droneSchema = StructType([
        StructField("id", StringType(), True),
        StructField("sensorId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("aerialImagesLocation", IntegerType(), True),
        StructField("thermalImaging", IntegerType(), True),
        StructField("sprayCoverage", IntegerType(), True),
        StructField("applicationRate", IntegerType(), True),
        StructField("company", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                    .format('kafka')
                    .option('kafka.bootstrap.servers', 'broker:29092')
                    .option('subscribe', topic)
                    .option('startingOffsets', 'earliest')
                    .load()
                    .selectExpr('CAST(values AS STRING)')
                    .selectExpr(from_json(col('value'), schema).alias('data'))
                    .selectExpr('data.*')
                    .withWatermark('timestamp', '2 minutes'))


    soilMoistureDF = read_kafka_topic('soil_moisture_data', soilMoistureSchema).alias('soil_moisture')
    lightTempDF = read_kafka_topic('light_temp_data', lightTempSchema).alias('soil_moisture')
    irrigationWaterDF = read_kafka_topic('irrigation_water_data', irrigationWater).alias('irrigation_water_')
    avGpsDF = read_kafka_topic('avGps_data', avGpsSchema).alias('avGps')
    livestockDF = read_kafka_topic('livestock_data', livestockSchema).alias('livestock')
    droneDF = read_kafka_topic('drone_data', droneSchema).alias('drone')

if __name__ == "__main__":
    main()