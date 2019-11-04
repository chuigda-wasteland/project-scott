package tech.icey.basic;

import java.util.ArrayList;
import java.util.List;

public class ListUtil {
    public static<T> ArrayList<T> copy(List<T> list) {
        return new ArrayList<>(list);
    }
}
