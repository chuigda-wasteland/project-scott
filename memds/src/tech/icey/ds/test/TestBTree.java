package tech.icey.ds.test;

import tech.icey.ds.BTree;

import java.util.ArrayList;
import java.util.Collections;

public class TestBTree {
    public static void main(String[] args) {
        var btree = new BTree(10);
        var elemList = new ArrayList<String>();
        for (int i = 1; i <= 99; i++) {
            elemList.add(Integer.toString(i));
        }
        Collections.shuffle(elemList);

        for (var elem : elemList) {
            btree.insert(elem);
        }

        Collections.shuffle(elemList);
        for (var elem : elemList) {
            btree.insert(elem);
        }

        for (var elem : btree.traverse()) {
            System.out.println(elem);
        }
    }
}
