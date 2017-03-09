package hex.klime;

import hex.Model;
import hex.ModelCategory;
import hex.ModelMetrics;

import hex.ModelMetricsRegression;
import hex.glm.GLMModel;
import hex.klime.KLimeModel.*;
import water.*;
import water.fvec.Chunk;
import water.fvec.Frame;

import java.util.Arrays;

import static hex.kmeans.KMeansModel.KMeansParameters;

public class KLimeModel extends Model<KLimeModel, KLimeParameters, KLimeOutput> {

  public KLimeModel(Key<KLimeModel> selfKey, KLimeParameters params, KLimeOutput output) {
    super(selfKey, params, output);
    assert(Arrays.equals(_key._kb, selfKey._kb));
  }

  @Override
  public ModelMetrics.MetricBuilder makeMetricBuilder(String[] domain) {
    return new ModelMetricsRegression.MetricBuilderRegression();
  }

  @Override
  public double[] score0(Chunk[] chks, double weight, double offset, int row_in_chunk, double[] tmp, double[] preds) {
    double[] ps = _output._clustering.score0(chks, weight, offset, row_in_chunk, tmp, preds);
    int cluster = (int) ps[0];
    if ((cluster < 0) || (cluster >= _output._regressionModels.length)) {
      throw new IllegalStateException("Unknown cluster, cluster id = " + cluster);
    }
    return _output._regressionModels[cluster].score0(chks, weight, offset, row_in_chunk, tmp, preds);
  }

  @Override
  protected double[] score0(double[] data, double[] preds) {
    throw H2O.unimpl("Intentionally not implemented, should never be called!");
  }

  @Override
  public double deviance(double w, double y, double f) {
    return (y - f) * (y - f);
  }

  public static class KLimeParameters extends Model.Parameters {
    public String algoName() { return "KLime"; }
    public String fullName() { return "k-LIME"; }
    public String javaName() { return KLimeModel.class.getName(); }

    public int _k;

    @Override public long progressUnits() { return fillClusteringParms(new KMeansParameters(), null).progressUnits() + _k; }

    KMeansParameters fillClusteringParms(KMeansParameters p, Key<Frame> clusteringFrameKey) {
      p._estimate_k = false;
      p._k = _k;
      p._train = clusteringFrameKey;
      p._auto_rebalance = false;
      p._seed = _seed;
      return p;
    }

    GLMModel.GLMParameters fillRegressionParms(GLMModel.GLMParameters p, Key<Frame> frameKey) {
      p._family = GLMModel.GLMParameters.Family.gaussian;
      p._alpha = new double[] {0.5};
      p._lambda_search = true;
      p._intercept = true;
      p._train = frameKey;
      p._response_column = _response_column;
      p._weights_column = "__cluster_weights";
      p._auto_rebalance = false;
      p._seed = _seed;
      return p;
    }
  }

  public static class KLimeOutput extends Model.Output {
    public KLimeOutput(KLime b) { super(b); }

    public Model _clustering;
    public Model[] _regressionModels;

    @Override public ModelCategory getModelCategory() { return ModelCategory.Regression; }
  }

  @Override
  protected Futures remove_impl(Futures fs) {
    if (_output._clustering != null)
      _output._clustering.remove(fs);
    if (_output._regressionModels != null)
      for (Model m : _output._regressionModels)
        m.remove(fs);
    return super.remove_impl(fs);
  }

}
