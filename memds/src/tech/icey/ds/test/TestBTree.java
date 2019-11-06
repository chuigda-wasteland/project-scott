package tech.icey.ds.test;

import tech.icey.ds.BTree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeSet;

public class TestBTree {
    public static void main(String[] args) {
        var btree = new BTree(3);
        var set = new TreeSet<String>();
        var elemList = new ArrayList<String>();
        for (int i = 1; i <= 99; i++) {
            elemList.add(Integer.toString(i));
        }
        Collections.shuffle(elemList);

        for (var elem : elemList) {
            btree.insert(elem);
            set.add(elem);
        }

        Collections.shuffle(elemList);
        for (var elem : elemList) {
            btree.insert(elem);
        }

        assert Arrays.equals(btree.traverse().toArray(), set.toArray());

        Collections.shuffle(elemList);
        for (var elem : elemList) {
            var deleted = btree.delete(elem);
            assert deleted;
            set.remove(elem);

            assert Arrays.equals(btree.traverse().toArray(), set.toArray());
        }
    }
}
