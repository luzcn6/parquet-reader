import pyarrow.parquet as pq

import pandas as pd
import os
import glob


class Reader:
    def __init__(self, path):
        self.files = glob.glob(os.path.join(path, "**/*.parquet"), recursive=True)

    def list_files(self):
        for file in self.files:
            with open(file) as f:
                print(f.name)


def main():
    reader = Reader("/Users/zheng.lu/csirt_query_7713034/stems")
    reader.list_files()


if __name__ == "__main__":
    main()
