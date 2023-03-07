# %%
# pyspark
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

#custom
from spark_utils import SparkUtilsService
from interpolation import InterpolationService
from mongo import MongoService

sparkUtils = SparkUtilsService()
interpolation = InterpolationService()
mongo = MongoService()


# UDFs
interpolation_udf = udf(interpolation.handle_interpolation, ArrayType(StringType(), True))
consumptions_udf = udf(interpolation.consumption, ArrayType(StringType(), True))

def query():
    return sparkUtils\
        .read_collection_with_ids(
            collection='clientsRaw2', 
            collection_ids='clientsPipeline', 
            arr='needsInterpolation') \
        .select('instalacionID', 'lecturas') \
        .withColumn("lecturasInter", interpolation_udf("lecturas")) \
        .withColumn("consumosInter", consumptions_udf("lecturasInter"))

def save(dfx):
    dfx.show(699, False)
    # dfx.write \
    #     .format("mongodb") \
    #     .option("collection", "clientsDef") \
    #     .option("idFieldList", "instalacionID") \
    #     .option("operationType", "update") \
    #     .mode("append") \
    #     .save()

def save_track(dfx):
    ids = dfx \
        .select('instalacionID') \
        .rdd \
        .flatMap(lambda x: x) \
        .collect()
    doc = mongo.get_doc('needsMacroCorrelations')
    mongo.update_array_needs(doc, 'needsMacroCorrelations', ids)
    # mongo.update_array_needs(doc, 'needsInterpolation', [])
    print(f'Save Track: {len(ids)}')
    

def main():
    try:
        spark = sparkUtils.OpenSession()
        dfx = query()
        save(dfx)
        save_track(dfx)
        spark.stop()
        input("Press Enter to continue...")
        exit(0)
    except Exception as e:
        print("Error: ", e)
        exit(1)
if __name__ == "__main__":
    main()