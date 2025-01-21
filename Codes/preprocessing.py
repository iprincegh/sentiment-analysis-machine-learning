import pandas as pd


class PreProcessData:
    """"
     PreProcess InComing data (CSV format)
    """

    def __init__(self, filename, index_column=None, null_fill=None, ):
        self.filename = filename
        self.index_column = index_column
        self.null_fill = null_fill
        self.dataframe = None

        self._open_file()
        self._remove_duplication()
        self._handle_null()

    # read data from file then set index if necessary
    def _open_file(self) -> pd.DataFrame():
        self.dataframe = self.load_raw_df()

    # remove duplicated rows
    def _remove_duplication(self):
        if len(self.dataframe[self.dataframe.duplicated()]) != 0:
            self.dataframe.drop_duplicates(inplace=True)

    #   handle null values (if no data replacement "null_fill" is specified the method drops rows with null values)
    def _handle_null(self):
        col_with_nan = self.dataframe.columns[self.dataframe.isna().any()].tolist()

        if self.null_fill:
            for c in col_with_nan:
                self.dataframe[c] = self.dataframe[c].fillna(self.null_fill)
        else:
            self.dataframe.dropna(inplace=True)

    # load/expose clean data for analysis
    def load_clean_df(self, save_cd_as):
        self.dataframe.to_csv(save_cd_as)
        return self.dataframe

    # load/expose Raw data for analysis
    def load_raw_df(self):
        df = pd.read_csv(self.filename)  # load data from csv file
        if self.index_column:
            df.set_index(self.index_column, inplace=True)
        return df
