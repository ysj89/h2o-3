package water.api;

import hex.mli.CorrGraph;
import hex.mli.Edge;
import water.api.schemas3.CorrGraphV3;
import water.api.schemas3.EdgeV3;
import water.fvec.Frame;

public class CorrGraphHandler extends Handler {

    public CorrGraphV3 getCorrelationGraph(int version, CorrGraphV3 cg) {
        Frame fr = FramesHandler.getFromDKV("key", cg.cor_frame_id.key());
        int[] columnIdx = cg.col_idxs;
        double threshold = cg.threshold;
        CorrGraph corrGraph = new CorrGraph(threshold,columnIdx,fr._key);
        Edge[] edges = corrGraph.getEdges();
        String[] nodes = fr.names();
        EdgeV3[] edgeV3s = new EdgeV3[edges.length];
        for(int i = 0; i < edges.length; i++){
            edgeV3s[i] = new EdgeV3().fillFromImpl(edges[i]);
        }
        cg.edges = edgeV3s;
        cg.nodes = nodes;

        return cg;
    }

}
