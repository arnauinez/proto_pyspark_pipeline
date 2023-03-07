
class InterpolationService():
    def __init__(self, array_len=96) -> None:
        self._array_len = array_len
    
    @property
    def array_len(self):
        return self._array_len
    
    def first_value(self, arr, z):
        return next((x, i) for i, x in enumerate(arr) if str(x) != z)

    def next_value(self, arr, index, z):
        # get next value of arr after index
        for i in range(index, len(arr)):
            if str(arr[i]) != z:
                return (arr[i], i)
        return (arr[index-1], i)

    def prev_value(self, arr, index, z):
        # get previous value of arr before index
        for i in range(index, -1, -1):
            if str(arr[i]) != z:
                return (arr[i], i)
        return self.first_value(arr, z)

    def interpolate(self, index, prev_index, next_index, prev_val, next_val):
        m = (next_val - prev_val) / (next_index - prev_index)
        interpolation = prev_val + m * (index - prev_index)
        return round(interpolation)

    def interpolation(self, arr, z):
        indices = [i for i, x in enumerate(arr) if str(x) == z]

        for index in indices:
            next_val, next_index = self.next_value(arr, index, z)
            prev_val, prev_index = self.prev_value(arr, index, z)

            if next_index - prev_index == 0:
                next_index += 1

            arr[index] = self.interpolate(index, prev_index, next_index, prev_val, next_val)

        return arr

    def handle_interpolation(self, x, z='-1'):
        if x is None:
            return ["None"]
        if len(x) < self.array_len:
            return ["Err Len"]
        else:
            return self.interpolation(x, z)
    
    def consumption(self, arr):
        if arr is None:
            return ["No consumption"]
        else:
            consumptions = [float(t) - float(s) for s, t in zip(arr, arr[1:])]

            if any(x < 0 for x in consumptions):
                return ["Negative consumption"]
            else:
                return consumptions