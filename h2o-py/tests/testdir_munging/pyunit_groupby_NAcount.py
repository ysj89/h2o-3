from __future__ import print_function
import sys
sys.path.insert(1,"../../")
import h2o
from tests import pyunit_utils

def group_by_NA():
    # Connect to a pre-existing cluster
    

    h2o_iris = h2o.import_file(path=pyunit_utils.locate("smalldata/iris/iris_wheader_NA.csv"), na_strings=['NA'])
    na_handling = ["rm"]

    print("Running smoke test")

    # smoke test
    for na in na_handling:
      print("Test group_by count....")
      grouped = h2o_iris.group_by("class")  # setup the Groupby object
      grouped.count(na=na)
      temp = grouped.get_frame()    # should get a frame 3 by 2
      print(grouped.get_frame())
      AssertGetFrameSize(temp, 3, 2)
      assert temp[0,1]==49 and temp[1,1]==50 and temp[2,1]==50, "groupby count is not working." # this is wrong
      assert temp[0,1]==49 and temp[1,1]==49 and temp[2,1]==49, "groupby count is not working." # this is correct
      sys.stdout.flush()

def AssertGetFrameSize(h2oframe, expectedRows, expectedCols):
  assert (h2oframe.ncols==expectedCols) and (h2oframe.nrows==expectedRows), "The group_by operation failed."


if __name__ == "__main__":
    pyunit_utils.standalone_test(group_by_NA)
else:
    group_by_NA()
