package water.api.schemas3;

import hex.mli.CorrGraph;
import water.api.API;
import water.api.schemas3.KeyV3.FrameKeyV3;

public class CorrGraphV3 extends RequestSchemaV3<CorrGraph,CorrGraphV3> {
    @API(help="correlation frame id", direction = API.Direction.INPUT)
    public FrameKeyV3 cor_frame_id;

    @API(help="column indexes used to filter the correlation graph", direction = API.Direction.INPUT)
    public int[] col_idxs;

    @API(help="threshold that indicates minimum correlation allowed", direction = API.Direction.INPUT)
    public double threshold;

    @API(help="edges in correlation graph", direction = API.Direction.OUTPUT)
    public EdgeV3[] edges;

    @API(help="nodes in correlation graph", direction = API.Direction.OUTPUT)
    public String[] nodes;

}
