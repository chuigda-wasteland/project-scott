package tech.icey.util;

import tech.icey.basic.Pair;

import java.util.ArrayList;
import java.util.List;

public class DirectedGraph {
    private List<Pair<String, String>> edges = new ArrayList<>();
    private List<Pair<String, String>> sameRankNodes = new ArrayList<>();

    public void addEdge(String from, String dest) {
        edges.add(new Pair<>(from, dest));
    }

    public void addSameRankNodes(String node1, String node2) {
        sameRankNodes.add(new Pair<>(node1, node2));
    }

    @Override
    public String toString() {
        var builder = new StringBuilder();
        builder.append("digraph {\n");
        for (var edge : edges) {
            builder.append("  \"");
            builder.append(edge.getFirst());
            builder.append("\" -> \"");
            builder.append(edge.getSecond());
            builder.append("\"\n");
        }
        for (var sameRankNodePair : sameRankNodes) {
            builder.append("  { rank=same; \"");
            builder.append(sameRankNodePair.getFirst());
            builder.append("\"; \"");
            builder.append(sameRankNodePair.getSecond());
            builder.append("\"; }\n");
        }
        builder.append("}");
        return builder.toString();
    }
}
