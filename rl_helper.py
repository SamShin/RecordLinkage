import pandas as pd
import pathlib
from typing import Union

class Data:
    def __init__(
        self,
        file_name:str,
        columns:list,
        unique_col:bool = False
    ):

        file_extension = pathlib.Path(file_name).suffix

        if file_extension == ".txt":
            data = pd.read_table(file_name, encoding="ISO-8859-1", low_memory=False)
        elif file_extension == ".csv":
            data = pd.read_csv(file_name, encoding="ISO-8859-1", low_memory = False)

        self.df = pd.DataFrame(data)
        self.df = self.df[columns]

        if unique_col == True:
            self.unique_col = unique_col
            unique_id = self.df.copy(deep=True)
            unique_id = unique_id.groupby(columns).ngroup()
            self.df['unique_id'] = unique_id



    def data_set(
        self,
        size:int,
        missing_frac:Union[int, float, list] = 0.1
    ):

        missing_list = []
        sample_size = round(size * 1.5)
        sample_set = self.df.sample(sample_size)
        half = round(sample_size / 3)

        if (type(missing_frac) == float) or (type(missing_frac) == int):
            missing_list = [missing_frac] * (len(self.df.columns))

            if self.unique_col == True:
                missing_list[-1] = 0

        elif self.unique_col == True:
            missing_frac.append(0)
            missing_list = missing_frac

        else:
            missing_list = missing_frac

        for i, col in enumerate(self.df.columns):
            sample_set.loc[sample_set.sample(frac=missing_list[i]).index, col] = None

        dfA_first = sample_set[0:half]
        dfB_first = sample_set[0:half]

        dfA_last = sample_set[(half):(2 * half)]
        dfB_last = sample_set.tail(half)

        frame_a = [dfA_first, dfA_last]
        frame_b = [dfB_first, dfB_last]

        dfA = pd.concat(frame_a)
        dfB = pd.concat(frame_b)

        return [dfA, dfB]