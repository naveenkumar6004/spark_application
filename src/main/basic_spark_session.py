from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def spark_session():
    """

    :return:
    """
    spark = SparkSession \
        .builder \
        .appName("Medicare_app") \
        .getOrCreate()
    return spark


if __name__=="__main__":
    spark = spark_session()
    df = spark.read.csv("test_data/Medicare_csv/OP_DTL_GNRL_PGYR2019_P01222021.csv", sep=',', header=True, inferSchema=True)
    #df.select("Change_Type").show(10,False)
    #df.printSchema()
    df = df.toDF(*[cols.upper() for cols in df.columns])
    #print(df.select("TEACHING_HOSPITAL_NAME").distinct().count())
    #df.groupby("TEACHING_HOSPITAL_NAME").agg(f.count("*")).show(100, False)
    #df.createTempView("Hospital")
    #spark.sql("select TEACHING_HOSPITAL_NAME, count(*) from Hospital group by TEACHING_HOSPITAL_NAME").show(100,False)
    #df.printSchema()
    # df.groupby("PHYSICIAN_FIRST_NAME","PHYSICIAN_LAST_NAME","TEACHING_HOSPITAL_NAME").agg(f.sum("TEACHING_HOSPITAL_NAME")).\
    #     select("PHYSICIAN_FIRST_NAME","PHYSICIAN_LAST_NAME","TEACHING_HOSPITAL_NAME").show(100,False)

    df.filter("PHYSICIAN_FIRST_NAME = 'ROBIN' AND PHYSICIAN_LAST_NAME = 'TRAVERS'").select("*").show(100,False)
    spark.stop()



