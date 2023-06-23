from openalexextract.openalex_core import OpenalexCore
from utils.time_run import time_run

@time_run
def main():

    oc = OpenalexCore()
    oc.core_extractor(entity='funders')
    print(oc.extract.head())

if __name__ == '__main__':
    main()
