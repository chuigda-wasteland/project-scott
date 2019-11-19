package tech.icey.util;

import tech.icey.basic.Pair;

import java.util.ArrayList;
import java.util.List;

public class DirectedGraph {
    private List<Pair<String, String>> edges = new ArrayList<>();

    public void addEdge(String from, String dest) {
        edges.add(new Pair<>(from, dest));
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
        builder.append("}");
        return builder.toString();
    }
}
