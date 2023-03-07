import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class SparkUtilsService():
    def __init__(self, path="SparkConfig.json") -> None:
        self._config = SparkUtilsService.readConfig(path)
        self._session = None
    
    @property
    def session(self):
        return self._session
    
    @session.setter
    def session(self, value):
        self._session = value
    
    @property
    def config(self):
        return self._config
    
    @property
    def AppName(self):
        return  self._config["AppName"]
    
    @property 
    def DefaultDatabase(self):
        return self._config["DefaultDatabase"]
    
    
    def OpenSession(self):
        session = SparkSession.builder.appName(self._config["AppName"])
        for config in self._config["Configurations"]:
            key, val = config["key"], config["value"]
            session = session.config(key, val)
        
        self.session = session.getOrCreate()
        return self.session
    
    @staticmethod
    def readConfig(filename: str):
        with open(filename,'r') as file:
            return json.loads(file.read())
    
    def saveNewCollection(self, dfx, collection_name):
        return dfx.write \
            .format("mongodb") \
            .mode("append") \
            .option("collection", collection_name)\
            .save()
    
    
    def readCollection(self, collection_name):    
        return self._session.read.format("mongodb") \
            .option("database", "metrogas")\
            .option("collection", collection_name)\
            .load()
    
    def read_collection_with_ids(self, collection, collection_ids, arr):
        # IDS para produccion
        # ids = self.ids(collection_ids, arr)
        # IDS presentes en clientsDef para testing
        ids=[23780, 143316, 136015, 91089]
        return self.readCollection(collection) \
            .where(F.col('instalacionID').isNotNull()) \
            .where(F.col('instalacionID').isin(ids)) 
        
    def ids(self, collection_name, array_name):
        dfx = self.readCollection(collection_name) \
            .select(array_name) \
            .where(F.size(array_name) > 0)
        return dfx.select(F.explode(array_name) \
            .alias(array_name)) \
            .distinct()\
            .toPandas()[array_name] \
            .tolist()
        # return dfx.select(F.explode(array_name) \
        #     .alias(array_name)).distinct()
        # df = sparkUtils.readCollection(spark, collection_name="clientsPipeline")
        # df = df.select('needsInterpolation') \
        #     .where(F.size('needsInterpolation') > 0)
        # ids = df.select(F.explode('needsInterpolation')\
        #     .alias('needsInterpolation'))\
        #     .toPandas()['needsInterpolation']\
        #     .tolist()