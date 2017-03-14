package hex.util;

import hex.CreateFrame;
import hex.DataInfo;
import hex.Model;
import hex.aggregator.Aggregator;
import hex.aggregator.AggregatorModel;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import water.*;
import water.fvec.Frame;
import water.fvec.RebalanceDataSet;
import water.fvec.Vec;
import water.util.Log;

public class AggregatorTest extends TestUtil {
  @BeforeClass() public static void setup() { stall_till_cloudsize(1); }

  @Test public void testAggregator() {
    CreateFrame cf = new CreateFrame();
    cf.rows = 100000;
    cf.cols = 2;
    cf.categorical_fraction = 0.1;
    cf.integer_fraction = 0.3;
    cf.real_range = 100;
    cf.integer_range = 100;
    cf.seed = 1234;
    Frame frame = cf.execImpl().get();

    AggregatorModel.AggregatorParameters parms = new AggregatorModel.AggregatorParameters();
    parms._train = frame._key;
    parms._max_exemplars = 100;
    long start = System.currentTimeMillis();
    AggregatorModel agg = new Aggregator(parms).trainModel().get();
    System.out.println("AggregatorModel finished in: " + (System.currentTimeMillis() - start)/1000. + " seconds");
    agg.checkConsistency();
    Frame output = agg._output._output_frame.get();
    System.out.println(output.toTwoDimTable(0,10));
    frame.delete();
    Log.info("Number of exemplars: " + agg._exemplars.length);
    Assert.assertTrue(agg._exemplars.length<=100*1.05);
    output.remove();
    agg.remove();
  }

  @Test public void testAggregatorEigen() {
    CreateFrame cf = new CreateFrame();
    cf.rows = 1000;
    cf.cols = 10;
    cf.categorical_fraction = 0.6;
    cf.integer_fraction = 0.0;
    cf.binary_fraction = 0.0;
    cf.real_range = 100;
    cf.integer_range = 100;
    cf.missing_fraction = 0;
    cf.factors = 5;
    cf.seed = 1234;
    Frame frame = cf.execImpl().get();

    AggregatorModel.AggregatorParameters parms = new AggregatorModel.AggregatorParameters();
    parms._train = frame._key;
    parms._categorical_encoding = Model.Parameters.CategoricalEncodingScheme.Eigen;
    long start = System.currentTimeMillis();
    AggregatorModel agg = new Aggregator(parms).trainModel().get();  // 0.905
    System.out.println("AggregatorModel finished in: " + (System.currentTimeMillis() - start)/1000. + " seconds");
    agg.checkConsistency();
    Frame output = agg._output._output_frame.get();
    System.out.println(output.toTwoDimTable(0,10));
    Log.info("Number of exemplars: " + agg._exemplars.length);
    output.remove();
    frame.remove();
    agg.remove();
  }

  @Test public void testAggregatorBinary() {
    CreateFrame cf = new CreateFrame();
    cf.rows = 1000;
    cf.cols = 10;
    cf.categorical_fraction = 0.6;
    cf.integer_fraction = 0.0;
    cf.binary_fraction = 0.0;
    cf.real_range = 100;
    cf.integer_range = 100;
    cf.missing_fraction = 0.1;
    cf.factors = 5;
    cf.seed = 1234;
    Frame frame = cf.execImpl().get();

    AggregatorModel.AggregatorParameters parms = new AggregatorModel.AggregatorParameters();
    parms._train = frame._key;
    parms._transform = DataInfo.TransformType.NORMALIZE;
    parms._categorical_encoding = Model.Parameters.CategoricalEncodingScheme.Binary;
    long start = System.currentTimeMillis();
    AggregatorModel agg = new Aggregator(parms).trainModel().get();  // 0.905
    System.out.println("AggregatorModel finished in: " + (System.currentTimeMillis() - start)/1000. + " seconds");
    agg.checkConsistency();
    Frame output = agg._output._output_frame.get();
    System.out.println(output.toTwoDimTable(0,10));
    Log.info("Number of exemplars: " + agg._exemplars.length);
    Assert.assertTrue(agg._exemplars.length==1000);
    output.remove();
    frame.remove();
    agg.remove();
  }

  @Test public void testAggregatorOneHot() {
    Scope.enter();
    CreateFrame cf = new CreateFrame();
    cf.rows = 1000;
    cf.cols = 10;
    cf.categorical_fraction = 0.6;
    cf.integer_fraction = 0.0;
    cf.binary_fraction = 0.0;
    cf.real_range = 100;
    cf.integer_range = 100;
    cf.missing_fraction = 0.1;
    cf.factors = 5;
    cf.seed = 1234;
    Frame frame = cf.execImpl().get();

    AggregatorModel.AggregatorParameters parms = new AggregatorModel.AggregatorParameters();
    parms._train = frame._key;
    parms._max_exemplars = 278;
    parms._transform = DataInfo.TransformType.NORMALIZE;
    parms._categorical_encoding = Model.Parameters.CategoricalEncodingScheme.OneHotExplicit;
    long start = System.currentTimeMillis();
    AggregatorModel agg = new Aggregator(parms).trainModel().get();  // 0.905
    System.out.println("AggregatorModel finished in: " + (System.currentTimeMillis() - start)/1000. + " seconds");
    agg.checkConsistency();
    Frame output = agg._output._output_frame.get();
    System.out.println(output.toTwoDimTable(0,10));
    Log.info("Number of exemplars: " + agg._exemplars.length);
    Assert.assertTrue(agg._exemplars.length<=278);
    output.remove();
    frame.remove();
    agg.remove();
    Scope.exit();
  }

  @Test public void testCovtype() {
    Frame frame = parse_test_file("smalldata/covtype/covtype.20k.data");

    AggregatorModel.AggregatorParameters parms = new AggregatorModel.AggregatorParameters();
    parms._train = frame._key;
    parms._max_exemplars = 500;
    long start = System.currentTimeMillis();
    AggregatorModel agg = new Aggregator(parms).trainModel().get();  // 0.179
    System.out.println("AggregatorModel finished in: " + (System.currentTimeMillis() - start)/1000. + " seconds");    agg.checkConsistency();
    frame.delete();
    Frame output = agg._output._output_frame.get();
    Log.info("Exemplars: " + output.toString());
    output.remove();
    Log.info("Number of exemplars: " + agg._exemplars.length);
    Assert.assertTrue(agg._exemplars.length<=500*1.05);
    agg.remove();
  }

  @Test public void testChunks() {
    Frame frame = parse_test_file("smalldata/covtype/covtype.20k.data");

    AggregatorModel.AggregatorParameters parms = new AggregatorModel.AggregatorParameters();
    parms._train = frame._key;
    parms._max_exemplars = 137;
    long start = System.currentTimeMillis();
    AggregatorModel agg = new Aggregator(parms).trainModel().get();  // 0.418
    System.out.println("AggregatorModel finished in: " + (System.currentTimeMillis() - start)/1000. + " seconds");    agg.checkConsistency();
    Frame output = agg._output._output_frame.get();
    Log.info("Number of exemplars: " + agg._exemplars.length);
//    Assert.assertTrue(agg._exemplars.length==1993);
    output.remove();
    agg.remove();

    for (int i : new int[]{1,2,5,10,50,100}) {
      Key key = Key.make();
      RebalanceDataSet rb = new RebalanceDataSet(frame, key, i);
      H2O.submitTask(rb);
      rb.join();
      Frame rebalanced = DKV.get(key).get();

      parms = new AggregatorModel.AggregatorParameters();
      parms._train = frame._key;
      parms._max_exemplars = 137;
      start = System.currentTimeMillis();
      AggregatorModel agg2 = new Aggregator(parms).trainModel().get();  // 0.373 0.504 0.357 0.454 0.368 0.355
      System.out.println("AggregatorModel finished in: " + (System.currentTimeMillis() - start)/1000. + " seconds");      agg2.checkConsistency();
      Log.info("Number of exemplars for " + i + " chunks: " + agg2._exemplars.length);
      rebalanced.delete();
      Assert.assertTrue(Math.abs(agg._exemplars.length - agg2._exemplars.length) == 0);
      output = agg2._output._output_frame.get();
      output.remove();
      agg2.remove();
    }
    frame.delete();
  }

  @Ignore
  @Test public void testCovtypeMemberIndices() {
    Frame frame = parse_test_file("smalldata/covtype/covtype.20k.data");

    AggregatorModel.AggregatorParameters parms = new AggregatorModel.AggregatorParameters();
    parms._train = frame._key;
    parms._max_exemplars = 117;
    long start = System.currentTimeMillis();
    AggregatorModel agg = new Aggregator(parms).trainModel().get();   // 1.489
    System.out.println("AggregatorModel finished in: " + (System.currentTimeMillis() - start)/1000. + " seconds");    agg.checkConsistency();

//    Frame assignment = new Frame(new Vec[]{(Vec)agg._exemplar_assignment_vec_key.get()});
//    Frame.export(assignment, "/tmp/assignment", "yada", true);
//    Log.info("Exemplars: " + new Frame(new Vec[]{(Vec)agg._exemplar_assignment_vec_key.get()}).toString(0,20000));
    Log.info("Number of exemplars: " + agg._exemplars.length);

    Key<Frame> memberKey = Key.make();
    for (int i=0; i<agg._exemplars.length; ++i) {
      Frame members = agg.scoreExemplarMembers(memberKey, i);
      assert (members.numRows() == agg._counts[i]);
//    Log.info(members);
      members.delete();
    }

    Frame output = agg._output._output_frame.get();
    output.remove();
    Log.info("Number of exemplars: " + agg._exemplars.length);
    Assert.assertTrue(agg._exemplars.length<=117);
    frame.delete();
    agg.remove();
  }

  @Test public void testDomains() {
    Frame frame = parse_test_file("smalldata/junit/weather.csv");
    for (String s : new String[]{"MaxWindSpeed", "RelHumid9am", "Cloud9am"}) {
      Vec v = frame.vec(s);
      Vec newV = v.toCategoricalVec();
      frame.remove(s);
      frame.add(s,newV);
      v.remove();
    }
    DKV.put(frame);
    AggregatorModel.AggregatorParameters parms = new AggregatorModel.AggregatorParameters();
    parms._train = frame._key;
    parms._max_exemplars = 17;
    AggregatorModel agg = new Aggregator(parms).trainModel().get();
    Frame output = agg._output._output_frame.get();
    Assert.assertTrue(output.numRows() <= 17);
    boolean same = true;
    for (int i=0;i<frame.numCols();++i) {
      if (frame.vec(i).isCategorical()) {
        same = (frame.domains()[i].length == output.domains()[i].length);
        if (!same) break;
      }
    }
    frame.remove();
    output.remove();
    agg.remove();
    Assert.assertFalse(same);
  }

  @Ignore
  @Test public void testMNIST() {
    Frame frame = parse_test_file("bigdata/laptop/mnist/train.csv.gz");

    AggregatorModel.AggregatorParameters parms = new AggregatorModel.AggregatorParameters();
    parms._train = frame._key;
    long start = System.currentTimeMillis();
    AggregatorModel agg = new Aggregator(parms).trainModel().get();
    System.out.println("AggregatorModel finished in: " + (System.currentTimeMillis() - start)/1000. + " seconds");    agg.checkConsistency();
    frame.delete();
    Frame output = agg._output._output_frame.get();
//    Log.info("Exemplars: " + output);
    output.remove();
    Log.info("Number of exemplars: " + agg._exemplars.length);
    agg.remove();
  }
}
