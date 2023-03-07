from scipy.stats import pearsonr
import numpy as np

class CorrelationService():
    def __init__(self, macro, array_len=95) -> None:
        self._macro = macro
        self._array_len = array_len
    
    @property
    def macro(self):
        return self._macro
    
    @property
    def array_len(self):
        return self._array_len
    
    def cor(self, x, y):
        # TODO: Ignore pearsonr warnings
        try:
            if x is None or y is None:
                return "None"
            r = pearsonr(x, y)
            if np.isnan(r):
                return "NaN"

            return str(r[0])

        except:
            return str(r)
    
    def correlate(self, x):
        try:
            # TODO: array of correlations
            unemployment = self.cor(x, self._macro['unemployment_rate'])
            henryhub = self.cor(x, self._macro['Henry_Hub_Natural_Gas_Spot_Price_(Dollars_per_Million_Btu)'])
            exchange = self.cor(x, self._macro['Exchange_rate'])
            cpi = self.cor(x, self._macro['CPI_base2015'])
            return [unemployment, henryhub, exchange, cpi]

        except:
            return ["None", "None", "None", "None"]
    

    def calculate_correlations(self, x):
        if x is None:
            return ["None", "None", "None", "None"]
        if x == ["No consumption"] or x == ["Negative consumption"]:
            return ["None", "None", "None", "None"]
        if len(x) < self._array_len:
            return ["None", "None", "None", "None"]
        else:
            x = [float(i) for i in x][:self._array_len]
            return self.correlate(x)