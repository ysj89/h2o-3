package hex.mli;

import water.Iced;

public class Edge extends Iced<Edge> {

    private final int node_a;
    private final int node_b;
    private final double weight;

    public Edge(int nodeA, int nodeB, double weight) {
        this.node_a = nodeA;
        this.node_b = nodeB;
        this.weight = weight;
    }

}
