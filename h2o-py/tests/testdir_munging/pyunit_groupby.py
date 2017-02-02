from __future__ import print_function
import sys
sys.path.insert(1,"../../")
import h2o
from tests import pyunit_utils



import pandas as pd
import numpy as np

def group_by():
    # Connect to a pre-existing cluster
    

    h2o_iris = h2o.import_file(path=pyunit_utils.locate("smalldata/iris/iris_wheader.csv"))
    pd_iris = pd.read_csv(pyunit_utils.locate("smalldata/iris/iris_wheader.csv"))
    h2o_iris2 = h2o.import_file(path=pyunit_utils.locate("smalldata/iris/iris_wheader.csv"))

    na_handling = ["all","rm"]
    col_names = h2o_iris.col_names[0:4]

    print("Running smoke test")

    # smoke test
    for na in na_handling:
      grouped = h2o_iris.group_by("class")
      grouped2 = h2o_iris2.group_by("class")
      grouped.count(na=na)
      print("*****************************")
      grouped = h2o_iris.group_by("class")
      print(grouped.get_frame())
      grouped.min(na=na)
      print("*****************************")
      grouped = h2o_iris.group_by("class")
      print(grouped.get_frame())
      grouped.max(na=na)
      print("*****************************")
      grouped = h2o_iris.group_by("class")
      print(grouped.get_frame())
      grouped.mean(na=na)
      print("*****************************")
      grouped = h2o_iris.group_by("class")
      print(grouped.get_frame())
      grouped.var(na=na)
      print("*****************************")
      grouped = h2o_iris.group_by("class")
      print(grouped.get_frame())
      grouped.sd(na=na)
      print("*****************************")
      grouped = h2o_iris.group_by("class")
      print(grouped.get_frame())
      grouped.ss(na=na)
      print("*****************************")
      print(grouped.get_frame())
      grouped.sum(na=na)
      print(grouped.get_frame())
      grouped2.count(na=na).min(  na=na).max(  na=na).mean( na=na).var(  na=na).sd(   na=na).ss(   na=na).sum(  na=na)
      print("*****************************")
      print(grouped2.get_frame())


if __name__ == "__main__":
    pyunit_utils.standalone_test(group_by)
else:
    group_by()
