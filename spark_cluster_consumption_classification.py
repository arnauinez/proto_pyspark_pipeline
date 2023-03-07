# %%
import numpy as np
# pyspark
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType, StringType

# custom
from spark_utils import SparkUtilsService
from centroids import CentroidsService
from mongo import MongoService

## INIT
sparkUtils = SparkUtilsService()
mongo = MongoService()
centroid = CentroidsService()

# %%
def aggregations(consumptions):
    # TODO: Refactoring
    if consumptions is None or len(consumptions) < 12:
        res = [-1, -1, -1, -1, -1, -1]
    if all(isinstance(x, float) for x in consumptions):
        res = [np.mean(consumptions), np.std(consumptions), np.median(consumptions), np.max(consumptions), np.min(consumptions)]
    else:
        res = [-2, -2, -2, -2, -2, -2]
    res = [float(i) for i in res]
    return res

# UDFs
udf_aggregations = udf(aggregations, ArrayType(FloatType(), True))
udf_assign_consumptions_cluster = udf(centroid.assign_consumptions_cluster, StringType())


def query():
# Selection
    df=sparkUtils \
            .read_collection_with_ids(
                collection='clientsDef', 
                collection_ids='clientsPipeline', 
                arr='needsConsumptionsClassification') \
            .select('instalacionID', 'unemploymentRateCorr', 'henryHubCorr', 'exchangeCorr', 'CPIBase2015Corr', 'consumosInter')
            
    # Parsing
    df =df \
        .withColumn("unemploymentRateCorr", F.split(F.regexp_replace(F.col("unemploymentRateCorr"), "[()]", ""), ",")[1]) \
        .withColumn("exchangeCorr", F.split(F.regexp_replace(F.col("exchangeCorr"), "[()]", ""), ",")[1]) \
        .withColumn("CPIBase2015Corr", F.split(F.regexp_replace(F.col("CPIBase2015Corr"), "[()]", ""), ",")[1]) \
        .withColumn("unemploymentRateCorr", F.col("unemploymentRateCorr").cast(FloatType())) \
        .withColumn("exchangeCorr", F.col("exchangeCorr").cast(FloatType())) \
        .withColumn("CPIBase2015Corr", F.col("CPIBase2015Corr").cast(FloatType()))
        
    # Aggregate
    df=df.withColumn("consumosInteAgg", udf_aggregations("consumosInter")) \
        .withColumn('consumoMean', F.col('consumosInteAgg')[0]) \
        .withColumn('consumoStd', F.col('consumosInteAgg')[1]) \
        .withColumn('consumoMedian', F.col('consumosInteAgg')[2]) \
        .withColumn('consumoMax', F.col('consumosInteAgg')[3]) \
        .withColumn('consumoMin', F.col('consumosInteAgg')[4]) \
        .drop('consumosInteAgg') \
        .drop("henryHubCorr") \
        .na.drop()

    # Classify
    df=df.withColumn("clusterConsumoYEco", udf_assign_consumptions_cluster(F.array("consumoMean", 
                                                                                "consumoStd", 
                                                                                "consumoMedian", 
                                                                                "consumoMax", 
                                                                                "consumoMin", 
                                                                                "unemploymentRateCorr", 
                                                                                "exchangeCorr", 
                                                                                "CPIBase2015Corr")
                                                                        ))
    return df

def save(dfx):
    dfx.select('instalacionID','clusterConsumoYEco').show(5, False)
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
    mongo.update_array_needs(doc, 'needsConsumptionsClassification', [])
    mongo.update_array_needs(doc, 'needsCensusClassification', ids)
    print(f'Save Track: {len(ids)}')

def main():
    try:
        spark = sparkUtils.OpenSession()
        centroid.model_consumptions
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
