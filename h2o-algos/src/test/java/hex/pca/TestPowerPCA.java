package hex.pca;

import hex.DataInfo;
import hex.gram.Gram;
import hex.svd.SVDModel;
import org.junit.BeforeClass;
import org.junit.Test;
import water.DKV;
import water.Key;
import water.Scope;
import water.TestUtil;
import water.fvec.Frame;
import water.rapids.Rapids;
import water.util.ArrayUtils;

import java.io.*;
import java.util.concurrent.ExecutionException;

import static water.util.ArrayUtils.*;

/**
	* PCA with power method is not working wide dataset.  Smaller dataset is doing okay.
	* Created by wendycwong on 3/11/17.
	*/
public class TestPowerPCA extends TestUtil {
		public static final double TOLERANCE = 1e-15;
		@BeforeClass
		public static void setup() { stall_till_cloudsize(1); }

		/**
			* Try to make sure the outer gram and the first eigenvector operation is correct for PCA method = Power
			* @throws InterruptedException
			* @throws ExecutionException
			*/
		@Test
		public void testOuterGramFirstEigenVector() throws InterruptedException, ExecutionException {

				// load in data frame and perform necessary operations
				Scope.enter();
				Frame train = null, scoreN = null, scoreW = null;

				DataInfo dinfo = null, tinfo = null;
				try {
						train = parse_test_file(Key.make("prostate_cat.hex"), "bigdata/laptop/jira/rotterdam.csv.zip");
						Scope.track(train);
						train.remove("relapse").remove();
						SVDModel.SVDParameters parms = new SVDModel.SVDParameters();
						parms._train = train._key;
						parms._nv  = 7;
						parms._transform = DataInfo.TransformType.NONE;
						parms._use_all_factor_levels = true;
						parms._svd_method = SVDModel.SVDParameters.Method.Power;
						parms._impute_missing=true;
						parms._seed = 12345;

						Frame tranRebalanced = new Frame(train);
						Scope.track(tranRebalanced);
						boolean frameHasNas = tranRebalanced.hasNAs();

						if (!parms._impute_missing && frameHasNas) { // remove NAs rows
								tinfo = new DataInfo(train, null, 0, parms._use_all_factor_levels, parms._transform,
																DataInfo.TransformType.NONE, /* skipMissing */ !parms._impute_missing, /* imputeMissing */
																parms._impute_missing, /* missingBucket */ false, /* weights */ false,
                    /* offset */ false, /* fold */ false, /* intercept */ false);
								DKV.put(tinfo._key, tinfo);

								DKV.put(tranRebalanced._key, tranRebalanced);
								train = Rapids.exec(String.format("(na.omit %s)", tranRebalanced._key)).getFrame(); // remove NA rows
								DKV.remove(tranRebalanced._key);
								Scope.track_generic(tinfo);
						}
						dinfo = new DataInfo(train, null, 0, parms._use_all_factor_levels, parms._transform,
														DataInfo.TransformType.NONE, /* skipMissing */ !parms._impute_missing, /* imputeMissing */
														parms._impute_missing, /* missingBucket */ false, /* weights */ false,
                /* offset */ false, /* fold */ false, /* intercept */ false);
						DKV.put(dinfo._key, dinfo);

						if (!parms._impute_missing && frameHasNas) {
								// fixed the std and mean of dinfo to that of the frame before removing NA rows
								dinfo._normMul = tinfo._normMul;
								dinfo._numMeans = tinfo._numMeans;
								dinfo._normSub = tinfo._normSub;
						}
						Scope.track_generic(dinfo);


						Gram.GramTask gtsk = null;
						Gram.OuterGramTask ogtsk = new Gram.OuterGramTask(null, dinfo).doAll(dinfo._adaptedFrame);
						Gram gram = ogtsk._gram;

						Frame gramFrame = new water.util.ArrayUtils().frame(gram.getXX());		// store outergram in a frame
						TestUtil.writeFrameToCSV2("/Users/wendycwong/h2o-3/h2o-py/tests/testdir_algos/pca/outerGram.csv",gramFrame);
						Scope.track(gramFrame);

						double[] randomInitialV = null; // store random initial eigenvectors, actually refering to V'
						int eigVecLen = (int) gram.fullN();       // size of one eigenvector

						// 1a) Initialize right singular vector v_1
						double[][] output_v = new double[parms._nv][eigVecLen];  // Store V' for ease of use and transpose back at end
						randomInitialV = new double[eigVecLen];   // allocate memroy for randomInitialV and finalV once, save time
						double[] v = new double[eigVecLen];
						double[][] allVecs = new double[4][286];	// store all eigenvector operations

						// actually layout the powerLoop and save the intermediate thingys into a frame for later Octave comparison
						randomInitialV = ArrayUtils.gaussianVector(parms._seed, randomInitialV);
						div(randomInitialV, l2norm(randomInitialV));

						int iters = 0;
						double err = 2*TOLERANCE;
      double[] vnew = new double[v.length];
						v = randomInitialV.clone();
						// Update v_i <- (A'Av_{i-1})/||A'Av_{i-1}|| where A'A = Gram matrix of training frame
						while(iters < 3 && err > TOLERANCE) {
								// Compute x_i <- A'Av_{i-1} and ||x_i||
								gram.mul(v, vnew, true);
								allVecs[iters] = v.clone();
								double norm = l2norm(vnew);

								double diff; err = 0;
								for (int i = 0; i < v.length; i++) {
										vnew[i] /= norm;        // Compute singular vector v_i = x_i/||x_i||
										diff = v[i] - vnew[i];  // Save error ||v_i - v_{i-1}||
										err += diff * diff;
										v[i] = vnew[i];         // Update v_i for next iteration

								}
								err = Math.sqrt(err);
								iters++;    // TODO: Should output vector of final iterations for each k

						}
						allVecs[iters] = v;		// add the final v
						Frame eigFrame = new water.util.ArrayUtils().frame(transpose(allVecs));
						Scope.track(eigFrame);
//						TestUtil.writeFrameToCSV("/Users/wendycwong/h2o-3/h2o-py/tests/testdir_algos/pca/checkPowerLoop.csv",eigFrame);
						TestUtil.writeFrameToCSV2("/Users/wendycwong/h2o-3/h2o-py/tests/testdir_algos/pca/sucessiveVs.csv",eigFrame);
						Scope.track(eigFrame);
				} catch (IOException e) {
						e.printStackTrace();
				} finally {
						Scope.exit();
				}

		}

}
