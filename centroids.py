from sklearn.neighbors import NearestCentroid
import pandas as pd

class CentroidsService():
    def __init__(self):
        self._model_censo = None
        self._model_consumptions = None
    
    @property
    def model_consumptions(self):
        if self._model_consumptions is None:
            self._load_consumption_model()
        return self._model_consumptions

    @property
    def model_censo(self):
        if self._model_censo is None:
            self._load_censo_model()
        return self._model_censo
    
    def assign_consumptions_cluster(self, x):
        if x is None:
            return -1
        if -1 in x:
            return -2
        if all(isinstance(x, float) for x in x):
            y = [float(i) if type(i)!=tuple else float(i[0]) for i in x]
            return str(self._model_consumptions.predict([y])[0])
        return -3

    def assign_censo_cluster(self, censo):
        x = [censo.CANT_HOG, censo.EDAD_15A64, censo.HOMBRES,censo.INDIGENAS,
            censo.INMIGRANTES,censo.MATACEP,censo.v1_Casa,censo.v1_Departamento,
            censo.v3a_Albanyileria,censo.v3a_Hormigon,censo.v3b_Hormigon,censo.v3b_Metal]
        x = self._float_convertible(x)
        if x is not None:
            return str(self._model_censo.predict([x])[0])

    def _float_convertible(self, array):
        try:
            return [float(i) for i in array]
        except:
            return None
    
    def _load_censo_model(self):
        cen_centroids = pd.read_csv('clusteringCensoMedoids.csv', sep=';')
        cen_centroids = cen_centroids.apply(lambda x: x.str.replace(',','.'))
        cen_centroids = cen_centroids.astype(float)
        cen_centroids['cluster'] = [1, 2, 3]
        cen_centroids['cluster'] = cen_centroids['cluster'].astype('category')
        X = cen_centroids.drop(['cluster'], axis=1)
        y = cen_centroids['cluster']
        self._model_censo = NearestCentroid(metric='manhattan')
        self._model_censo.fit( X, y )
    
    def _load_consumption_model(self):
        cns_centroids  = pd.read_csv('clusteringConsumoMedoids.csv', sep=';')
        cns_centroids = cns_centroids.apply(lambda x: x.str.replace(',','.'))
        cns_centroids = cns_centroids.astype(float)
        cns_centroids['cluster'] = [1, 2, 3, 4]
        cns_centroids['cluster'] = cns_centroids['cluster'].astype('category')
        X = cns_centroids.drop(['cluster'], axis=1)
        y = cns_centroids['cluster']
        self._model_consumptions = NearestCentroid(metric='manhattan')
        self._model_consumptions.fit( X, y )