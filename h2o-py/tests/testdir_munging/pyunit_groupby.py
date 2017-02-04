from __future__ import print_function
import sys
sys.path.insert(1,"../../")
import h2o
from tests import pyunit_utils

def group_by():
    # Connect to a pre-existing cluster
    

    h2o_iris = h2o.import_file(path=pyunit_utils.locate("smalldata/iris/iris_wheader.csv"), na_strings=['NA'])
    tcount = h2o_iris.nacnt()
    na_handling = ["rm","all"]

    print("Running smoke test")

    # smoke test
    for na in na_handling:
      print("Test group_by count....")
      grouped = h2o_iris.group_by("class")  # setup the Groupby object
      grouped.count(na=na)
      temp = grouped.get_frame()    # should get a frame 3 by 2
      print(grouped.get_frame())
      AssertGetFrameSize(temp, 3, 2)

      print("Test group_by count, min....")
      grouped = h2o_iris.group_by("class")  # setup the Groupby object
      grouped.count(na=na).min(na=na)
      temp = grouped.get_frame()
      print(grouped.get_frame())
      AssertGetFrameSize(temp, 3, 6)

      print("Test group_by count, min, max....")
      grouped = h2o_iris.group_by("class")  # setup the Groupby object
      grouped.count(na=na).min(na=na).max(na=na)
      temp = grouped.get_frame()
      print(grouped.get_frame())
      AssertGetFrameSize(temp, 3, 10)

      print("Test group_by count, min, max, mean....")
      grouped = h2o_iris.group_by("class")  # setup the Groupby object
      grouped.count(na=na).min(na=na).max(na=na).mean(na=na)
      temp = grouped.get_frame()
      print(grouped.get_frame())
      AssertGetFrameSize(temp, 3, 14)

      print("Test group_by count, min, max, mea, var....")
      grouped = h2o_iris.group_by("class")  # setup the Groupby object
      grouped.count(na=na).min(na=na).max(na=na).mean(na=na).var(na=na)
      temp = grouped.get_frame()
      print(grouped.get_frame())
      AssertGetFrameSize(temp, 3, 18)

      print("Test group_by count, min, max, mea, var, sd....")
      grouped = h2o_iris.group_by("class")  # setup the Groupby object
      grouped.count(na=na).min(na=na).max(na=na).mean(na=na).var(na=na).sd(na=na)
      temp = grouped.get_frame()
      print(grouped.get_frame())
      AssertGetFrameSize(temp, 3, 22)

      print("Test group_by count, min, max, mea, var, sd, ss....")
      grouped = h2o_iris.group_by("class")  # setup the Groupby object
      grouped.count(na=na).min(na=na).max(na=na).mean(na=na).var(na=na).sd(na=na).ss(na=na)
      temp = grouped.get_frame()
      print(grouped.get_frame())
      AssertGetFrameSize(temp, 3, 26)

      print("Test group_by count, min, max, mea, var, sd, ss, sum....")
      grouped = h2o_iris.group_by("class")  # setup the Groupby object
      grouped.count(na=na).min(na=na).max(na=na).mean(na=na).var(na=na).sd(na=na).ss(na=na).sum(na=na)
      temp = grouped.get_frame()
      print(grouped.get_frame())
      AssertGetFrameSize(temp, 3, 30)

      sys.stdout.flush()

def AssertGetFrameSize(h2oframe, expectedRows, expectedCols):
  assert (h2oframe.ncols==expectedCols) and (h2oframe.nrows==expectedRows), "The group_by operation failed."


if __name__ == "__main__":
    pyunit_utils.standalone_test(group_by)
else:
    group_by()
