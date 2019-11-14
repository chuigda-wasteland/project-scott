package tech.icey.basic;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class ListUtil {
    public static<T> ArrayList<T> copy(List<T> list) {
        return new ArrayList<>(list);
    }

    public static<T, U> List<Pair<T, U>> flatten(TreeMap<T, U> treeMap) {
        var ret = new ArrayList<Pair<T, U>>();
        for (var entry : treeMap.entrySet()) {
            ret.add(new Pair<>(entry.getKey(), entry.getValue()));
        }
        return ret;
    }
}
