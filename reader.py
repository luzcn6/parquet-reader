import pyarrow as pa
import pyarrow.parquet as pq

import numpy as np
import pandas as pd
import os
import glob


class Reader:
    def __init__(self, path):
        self.file_names = glob.glob(os.path.join(path, "**/*.parquet"), recursive=True)

    def list_files(self):
        for file in self.file_names:
            with open(file) as f:
                print(f.name)

    def read_parquet_file(self):
        for file_name in self.file_names:
            df = pq.read_pandas(file_name, columns=["ipsource"]).to_pandas()

            if not df.empty:
                df.to_csv("./results.csv", mode='a', header=False)


def main():
    reader = Reader("/Users/zheng.lu/csirt_query_7713034/stems")
    # reader.list_files()
    reader.read_parquet_file()


if __name__ == "__main__":
    main()
