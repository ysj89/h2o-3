package water.api.schemas3;

import water.api.API;
import hex.mli.Edge;

public class EdgeV3 extends RequestSchemaV3<Edge,EdgeV3> {
    @API(help="node a (Generic representation of a column name in a correlation graph)", direction = API.Direction.OUTPUT)
    public int node_a;

    @API(help="node b (Generic representation of a column name in a correlation graph)", direction = API.Direction.OUTPUT)
    public int node_b;

    @API(help="weight (Represented by correlation coefficient)", direction = API.Direction.OUTPUT)
    public double weight;

}
