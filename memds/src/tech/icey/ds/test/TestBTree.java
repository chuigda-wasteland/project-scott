package tech.icey.ds.test;

import tech.icey.ds.BTree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeSet;

public class TestBTree {
    public static void main(String[] args) {
        var btree = new BTree(4);
        var set = new TreeSet<String>();
        var elemList = new ArrayList<String>();

        elemList.add("17");
        elemList.add("1");
        elemList.add("15");
        elemList.add("2");
        elemList.add("12");
        elemList.add("16");
        elemList.add("19");
        elemList.add("18");
        elemList.add("5");
        elemList.add("6");
        elemList.add("13");
        elemList.add("14");
        elemList.add("8");
        elemList.add("7");
        elemList.add("11");
        elemList.add("3");
        elemList.add("9");
        elemList.add("4");
        elemList.add("20");
        elemList.add("10");

        for (var elem : elemList) {
            btree.insert(elem);
            set.add(elem);
        }

        var deleteList = new ArrayList<String>();
        deleteList.add("17");
        deleteList.add("15");
        deleteList.add("16");
        deleteList.add("19");
        deleteList.add("18");
        deleteList.add("5");
        deleteList.add("6");
        deleteList.add("13");
        deleteList.add("14");
        deleteList.add("8");
        deleteList.add("7");
        deleteList.add("11");
        deleteList.add("3");
        deleteList.add("9");
        deleteList.add("4");
        deleteList.add("20");
        deleteList.add("10");
        deleteList.add("1");
        deleteList.add("2");
        deleteList.add("12");

        for (var elem : deleteList) {
            var deleted = btree.delete(elem);
            assert deleted;
            set.remove(elem);
            for (var v : btree.traverse()) {
                System.err.print(v + " ");
            }
            System.err.println();
            assert Arrays.equals(btree.traverse().toArray(), set.toArray());
        }
    }
}
