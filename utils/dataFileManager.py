import os
import os.path
import re

import pandas as pd
import json

# set screen dimensions
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class DataFileManager:
    """
    Manage data directory and data file checking, writing and reading
    """

    cwd = re.sub(r'./notebooks', '', os.getcwd())
    print(f'{cwd = }')
    data_dir = "".join([cwd, rf'\\data\\'])

    print(f'{data_dir = }')

    def __init__(self):
        pass

    def datafile_writer(self, data_file=None, file_name=None):
        """
        write a datafile (dataframe) to a file in the format selected by the caller
        :param data_file: dataframe to write
        :param file_name: file name and type
        :return: False if write is not successful
        """

        if r".json" not in file_name and not isinstance(data_file, pd.DataFrame):
            print("datafile_writer can only write pd.DataFrame or json")
            print(f'output file_name {file_name}')
            print(f'input data type {type(data_file)}')
            return False

        out_file = "".join([self.data_dir, file_name])
        print(f'in DFM: file_name={out_file}')
        if r".csv" in file_name:
            data_file.to_csv(out_file, index=False)
        elif r".xlsx" in file_name:
            with pd.ExcelWriter(out_file, engine='xlsxwriter',
                                engine_kwargs={'options': {'strings_to_urls': False}}) as writer:
                data_file.to_excel(writer, index=False)
        elif r".pkl" in file_name:
            data_file.to_pickle(out_file)
        elif r".json" in file_name:
            with open(out_file, "w") as ofile:
                json.dump(data_file, ofile, indent=4)
        else:
            print("datafile_writer can only write xlsx, csv, pkl and json")
            return False

        return True

    # datafile reader

    def datafile_reader(self, file_name=None, sheet_name='Sheet1'):
        """
        read a datafile (dataframe) from a file in the format selected by the caller
        :param sheet_name: name of sheet (None means all sheets, nothing means first sheet)
        :param file_name: file name and type
        :return: False if write is not successful. Returns dict of df if there is more than one sheet
        """

        in_file = "".join([self.data_dir, file_name])

        if not os.path.isfile(in_file):
            print(f'the file {in_file} made from {file_name} does not exist')
            return False
        # print(f'reading in_file={in_file}')

        if r".csv" in file_name or r".txt" in file_name:
            # if '2020' in in_file:
            #     ecode = 'utf-8'
            # else:
            #     ecode = 'utf-16LE'
            ecode = 'utf-8'
            print(f'encoding = {ecode}')
            return pd.read_csv(in_file, skip_blank_lines=True, dtype=object, sep="\t", header=0,
                               encoding=ecode, encoding_errors='ignore', index_col=False)
            # encoding="utf-16LE", encoding_errors='ignore', index_col=False)
        elif r".xlsx" in file_name or r".xls" in file_name:
            if sheet_name == 'Sheet1':
                item = pd.read_excel(in_file)
            elif not sheet_name:
                item = pd.read_excel(in_file, sheet_name=None)
                # print('DataFileManager read multi-sheet XLSX datafile as a dict')
            else:
                item = pd.read_excel(in_file, sheet_name=sheet_name)
                # print(f'DataFileManager read specific sheet "{sheet_name}" from multi-sheet XLSX file')
            return item

        elif r".pkl" in file_name:
            return pd.read_pickle(in_file)
        elif r".json" in file_name:
            ifile = open(in_file, "r")
            return json.load(ifile)
        else:
            print("datafile_reader can only read xlsx [or xls], csv [& tab-separated txt] and pkl")
            return False


