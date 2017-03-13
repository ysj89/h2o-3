package hex.pca;

import org.junit.BeforeClass;
import water.TestUtil;

import java.util.concurrent.ExecutionException;

/**
	* PCA with power method is not working wide dataset.  Smaller dataset is doing okay.
	* Created by wendycwong on 3/11/17.
	*/
public class TestPowerPCA extends TestUtil {
		public static final double TOLERANCE = 1e-6;
		@BeforeClass
		public static void setup() { stall_till_cloudsize(1); }

		/**
			* Try to make sure the outer gram and the first eigenvector operation is correct for PCA method = Power
			* @throws InterruptedException
			* @throws ExecutionException
			*/
		public void testOuterGramFirstEigenVector() throws InterruptedException, ExecutionException {

				// load in data frame and perform necessary operations


				// for outergram


				// go through power loop for first eigenvector and make sure all operations are verifiable.  Store it in a
				// csv frame or something so that I can check it out in Octave.
				
				ActualPCATests.testWideDataSetsWithNAs(PCAModel.PCAParameters.Method.GramSVD, TOLERANCE);	// pca_method=GramSVD
				ActualPCATests.testWideDataSetsWithNAs(PCAModel.PCAParameters.Method.Power, TOLERANCE);	// pca_method=Power
//				ActualPCATests.testWideDataSetsWithNAs(PCAModel.PCAParameters.Method.Randomized, TOLERANCE);	// pca_method=Randomized
//				ActualPCATests.testWideDataSetsWithNAs(PCAModel.PCAParameters.Method.GLRM, TOLERANCE);	// pca_method=GLRM
		}

}
