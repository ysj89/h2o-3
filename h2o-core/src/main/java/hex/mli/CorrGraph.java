package hex.mli;

import water.Iced;
import water.Key;
import water.DKV;
import water.MRTask;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.util.ArrayUtils;
import java.util.ArrayList;

public class CorrGraph extends Iced {

    private double threshold;
    private int[] columns;
    private Key corKey;
    private Frame corFrame;

    public CorrGraph(){}

    public CorrGraph(double cutOff, int[] colIdx, Key corFrameKey) {
        threshold = cutOff;
        columns = colIdx;
        corKey = corFrameKey;
        corFrame = DKV.get(corKey).get();
    }

    private static class GraphTask extends MRTask<GraphTask> {

        double _cutOff;
        int[] _features;
        Edge[] _edges;

        GraphTask(double cutoff, int[] features) {
            _cutOff = cutoff;
            _features = features;
        }

        @Override
        public void map(Chunk cs[]) {
            ArrayList<Edge> edges = new ArrayList<Edge>();
            for (int idx=0; idx<cs[0]._len; idx++) {
                int gIdx = (int) cs[0].start() + idx;
                for(int j : _features) {
                    if (ArrayUtils.contains(_features, gIdx)) {
                        if (j < gIdx) {
                            if (cs[j].atd(idx) >= _cutOff) {
                                edges.add(new Edge(gIdx, j, cs[j].atd(idx)));
                            }
                        }
                    }
                }
            }
            _edges = edges.toArray(new Edge[edges.size()]);
        }
        @Override
        public void reduce(GraphTask gt) {
            ArrayUtils.append(_edges,gt._edges);
        }
    }

    public Edge[] getEdges(){
        GraphTask gt = new GraphTask(threshold, columns).doAll(corFrame);
        return gt._edges;
    }
}
