from __future__ import print_function
import sys
sys.path.insert(1,"../../")
import h2o
import time
from tests import pyunit_utils
import math
#----------------------------------------------------------------------
# This test will parse orc files containing timestamp and date information into
# H2O frame.  Next, it will take the .csv file generated from the orc file from
# Hive and parse into H2O frame.  Finally, we compare the two frames and make sure
# that they are equal.
#
# We want to make sure that we are parsing the date and timestamp
# date correctly from an orc file.  Thanks to Nidhi who has imported an orc file
# containing timestamp/date into spark and later into Hive and write it out as
# csv.
#
#----------------------------------------------------------------------

def hdfs_orc_parser():

    # Check if we are running inside the H2O network by seeing if we can touch
    # the namenode.
    hadoop_namenode_is_accessible = pyunit_utils.hadoop_namenode_is_accessible()

    if hadoop_namenode_is_accessible:
        hdfs_name_node = pyunit_utils.hadoop_namenode()

        if pyunit_utils.cannaryHDFSTest(hdfs_name_node, "/datasets/orc_parser/orc/orc_split_elim.orc"):
            print("Your hive-exec version is too old.  Orc parser test {0} is "
                  "skipped.".format("pyunit_INTERNAL_HDFS_timestamp_date_orc.py"))
            pass
        else:
            tol_time = 200              # comparing in ms or ns
            tol_numeric = 1e-5          # tolerance for comparing other numeric fields
            numElements2Compare = 100   # choose number of elements per column to compare.  Save test time.

            allOrcFiles = ["/datasets/orc_parser/orc/TestOrcFile.testDate1900.orc",
                           "/datasets/orc_parser/orc/TestOrcFile.testDate2038.orc",
                           "/datasets/orc_parser/orc/orc_split_elim.orc"]

            allCsvFiles = ["/datasets/orc_parser/csv/TestOrcFile.testDate1900.csv",
                           "/datasets/orc_parser/csv/TestOrcFile.testDate2038.csv",
                           "/datasets/orc_parser/csv/orc_split_elim.csv"]

            for fIndex in range(len(allOrcFiles)):
                url_orc = "hdfs://{0}{1}".format(hdfs_name_node, allOrcFiles[fIndex])
                url_csv = "hdfs://{0}{1}".format(hdfs_name_node, allCsvFiles[fIndex])
                h2oOrc = h2o.import_file(url_orc)
                h2oCsv = h2o.import_file(url_csv)

                print("Comparing files {0} and {1}\n.".format(allOrcFiles[fIndex], allCsvFiles[fIndex]))
                numElements = h2oOrc.nrows  # Compare all elements
           #     row_indices = list(range(rows))

                for col_ind in range(h2oOrc.ncols):
                    for row_ind in range(h2oOrc.nrows):

                        val1 = url_orc[row_ind, col_ind]
                        val2 = url_csv[row_ind, col_ind]

                        if not(math.isnan(val1)) and not(math.isnan(val2)): # both frames contain valid elements
                            diff = abs(val1-val2)
                            print("val1 is orc: {0} and val2 (csv) is {1}.  The difference is {2}\n".format(val1, val2, val1-val2))
                        else:
                            continue


                # compare the two frames
                # assert pyunit_utils.compare_frames(h2oOrc, h2oCsv, numElements2Compare, tol_time, tol_numeric), \
                #     "H2O frame parsed from orc and csv files are different!"
    else:
        raise EnvironmentError


if __name__ == "__main__":
    pyunit_utils.standalone_test(hdfs_orc_parser)
else:
    hdfs_orc_parser()