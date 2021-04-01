from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,MapType
from pyspark.sql.types import StringType,IntegerType
from pyspark.sql.functions import explode,from_json,to_json,col,struct,expr,lit

sbsbSchema = StructType() \
  .add("SBSB_CK", IntegerType()) \
  .add("GRGR_CK", IntegerType()) \
  .add("SBSB_ID", StringType()) \
  .add("SBSB_LAST_NAME", StringType()) \
  .add("SBSB_FIRST_NAME", StringType()) \
  .add("SBSB_MID_INIT", StringType())\
  .add("after", StructType() \
            .add("FNAME_AFTER",StringType()) \
            .add("LNAME_AFTER",StringType())) \
  .add("ts",StringType())

memeSchema = StructType() \
  .add("SBSB_CK", IntegerType()) \
  .add("MEME_CK", IntegerType()) \
  .add("MEME_LAST_NAME", StringType()) \
  .add("MEME_FIRST_NAME", StringType()) \
  .add("MEME_MID_INIT", StringType()) \
  .add("MEME_REL", StringType())\
  .add("ts",StringType())

spark = SparkSession \
    .builder \
    .appName("streamSBSBMEMEData") \
    .getOrCreate()

sbsb_parsed = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "sbsb_input_topic") \
  .load()

meme_parsed = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "meme_input_topic") \
  .load()

SbsbOut = sbsb_parsed.select(from_json(col("value").cast("string"), sbsbSchema).alias("data_sbsb"))\
    .selectExpr("data_sbsb.SBSB_CK as sbsbSBSBCK","data_sbsb.GRGR_CK as GRGR_CK","data_sbsb.SBSB_ID as SBSB_ID",\
        "CAST(data_sbsb.ts AS TIMESTAMP) AS sbsbTimestamp",\
        "data_sbsb.after.FNAME_AFTER as after_FNAME",\
            "data_sbsb.after.LNAME_AFTER as after_LNAME")

SbsbOut = SbsbOut.withColumn("ChangeType",lit("memberDemographics"))

MemeOut = meme_parsed.select(from_json(col("value").cast("string"), memeSchema).alias("data_meme"))\
    .selectExpr("data_meme.SBSB_CK as memeSBSBCK","data_meme.MEME_CK","data_meme.MEME_LAST_NAME","data_meme.MEME_FIRST_NAME",\
        "data_meme.MEME_REL as MEME_REL","CAST(data_meme.ts AS TIMESTAMP) \
        AS memeTimestamp")    

sbsb_df = SbsbOut.selectExpr("ChangeType","sbsbSBSBCK", "GRGR_CK", "SBSB_ID","after_FNAME","after_LNAME","sbsbTimestamp")\
    .withWatermark("sbsbTimestamp", "10 seconds")

meme_df = MemeOut.selectExpr("memeSBSBCK", "MEME_CK", "MEME_LAST_NAME","MEME_FIRST_NAME",\
    "MEME_REL","memeTimestamp").withWatermark("memeTimestamp", "20 seconds")

merged_df = sbsb_df.join(meme_df, expr("""sbsbSBSBCK =  memeSBSBCK AND memeTimestamp >= sbsbTimestamp AND \
    memeTimestamp <= sbsbTimestamp + interval 1 minutes """))

merged_df.selectExpr("ChangeType as key","to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic","sbsb_meme_output_topic") \
    .option("checkpointLocation", "/tmp/kafka/memberDemo1") \
    .start() \
    .awaitTermination()