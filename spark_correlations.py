# %%
# pyspark
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

#custom
from spark_utils import SparkUtilsService
from mongo import MongoService
from correlation import CorrelationService
# %%
# INIT
sparkUtils = SparkUtilsService()
spark = sparkUtils.OpenSession()
mongo = MongoService()

df_macro = sparkUtils \
    .readCollection(collection_name='macro') \
    .toPandas()
correlations = CorrelationService(df_macro)

# UDFs
cor_udf = udf(correlations.calculate_correlations, ArrayType(StringType()))

def query():
    return sparkUtils.read_collection_with_ids(collection='clientsDef', 
                                                collection_ids='clientsPipeline', 
                                                arr='needsMacroCorrelations') \
        .select('instalacionID', 'consumosInter') \
        .withColumn("macro_corr", cor_udf("consumosInter")) \
        .withColumn("unemploymentRateCorr", F.col("macro_corr")[0]) \
        .withColumn("henryHubCorr", F.col("macro_corr")[1]) \
        .withColumn("exchangeCorr", F.col("macro_corr")[2]) \
        .withColumn("CPIBase2015Corr", F.col("macro_corr")[3]) \
        .drop("macro_corr")

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
    doc = mongo.get_doc('needsConsumptionsClassification')
    mongo.update_array_needs(doc, 'needsConsumptionsClassification', ids)
    mongo.update_array_needs(doc, 'needsMacroCorrelations', [])
    
    print(f'Save Track: {len(ids)}')

def main():
    try:
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