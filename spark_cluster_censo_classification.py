# %%
# IMPORTS
# pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# custom
from spark_utils import SparkUtilsService
from centroids import CentroidsService
from mongo import MongoService

# INIT
sparkUtils = SparkUtilsService()
centroid = CentroidsService()
mongo = MongoService()


# UDFs
udf_assign_censo_cluster = udf(centroid.assign_censo_cluster, StringType())

def query():
    return sparkUtils \
        .read_collection_with_ids(
            collection='clientsDef', 
            collection_ids='clientsPipeline', 
            arr='needsCensusClassification') \
        .select('instalacionID', 'censo') \
        .withColumn("clusterCenso", udf_assign_censo_cluster("censo"))

def save(dfx):
    dfx.select('instalacionID', 'clusterCenso').show(5, False)
    # dfx.write \
    #     .format("mongodb") \
    #     .option("collection", "clustering") \
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
    doc = mongo.get_doc('needsConsumptionsClassification')
    mongo.update_array_needs(doc, 'needsCensusClassification', [])
    print(f'Save Track: {len(ids)}')

def main():
    try:
        spark = sparkUtils.OpenSession()
        centroid.model_censo
        dfx=query()
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

# %%
