package tech.icey.ds;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BTreeTest {
    List<String> getFixedInsertSequence() {
        return List.of("17", "1", "15", "2", "12", "16", "19",
                "18", "5", "6", "13", "14", "8", "7", "11",
                "3", "9", "4", "20", "10");
    }

    List<String> getFixedDeletionSequence() {
        return List.of("17", "15", "16", "19", "18", "5", "6", "13",
                "14", "8", "7", "11", "3", "9", "4", "20", "10",
                "1", "2", "12");
    }

    List<String> getRandomSequence() {
        var ret = new ArrayList<String>();
        for (var i = 1; i < 1000; i++) {
            ret.add(Integer.toString(i));
        }
        Collections.shuffle(ret);
        return ret;
    }

    @Test
    void happyTestDegree3() {
        testSimpleInsertDelete(3, getFixedInsertSequence(), getFixedDeletionSequence());
    }

    @Test
    void happyTestDegree4() {
        testSimpleInsertDelete(4, getFixedInsertSequence(), getFixedDeletionSequence());
    }

    @Test
    void happyTestDegree5() {
        testSimpleInsertDelete(5, getFixedInsertSequence(), getFixedDeletionSequence());
    }

    @Test
    void thousandElementDegree3() {
        testSimpleInsertDelete(3, getRandomSequence(), getRandomSequence());
    }

    @Test
    void thousandElementDegree4() {
        testSimpleInsertDelete(4, getRandomSequence(), getRandomSequence());
    }

    @Test
    void thousandElementDegree20() {
        testSimpleInsertDelete(20, getRandomSequence(), getRandomSequence());
    }

    @Test
    void insertDeleteMixDegree3() {
        mixInsertAndDelete(3);
    }

    @Test
    void insertDeleteMixDegree4() {
        mixInsertAndDelete(4);
    }

    @Test
    void insertDeleteMixDegree20() {
        mixInsertAndDelete(20);
    }

    void mixInsertAndDelete(int degree) {
        var r = new Random();
        for (var i = 0; i < 10; i++) {
            var btree = new BTree(degree);
            var set = new TreeSet<String>();
            for (var elem : getRandomSequence()) {
                btree.insert(elem);
                set.add(elem);
                assertArrayEquals(set.toArray(), btree.traverse().toArray());
            }

            for (var j = 0; j < 200; j++) {
                var sequence = getRandomSequence().subList(0, r.nextInt(25));
                if (r.nextInt(2) == 1) {
                    for (var elem : sequence) {
                        btree.delete(elem);
                        set.remove(elem);
                    }
                } else {
                    for (var elem : sequence) {
                        btree.insert(elem);
                        set.add(elem);
                    }
                }
                assertArrayEquals(set.toArray(), btree.traverse().toArray());
            }
        }
    }

    void testSimpleInsertDelete(int degree, List<String> insertSequence, List<String> deleteSequence) {
        for (var i = 0; i < 10; i++) {
            var btree = new BTree(degree);
            var set = new TreeSet<String>();

            for (var elem : insertSequence) {
                btree.insert(elem);
                set.add(elem);
                assertArrayEquals(set.toArray(), btree.traverse().toArray());
            }

            for (var elem : deleteSequence) {
                var deleted = btree.delete(elem);
                Assertions.assertTrue(deleted);
                set.remove(elem);
                Assertions.assertArrayEquals(set.toArray(), btree.traverse().toArray());
            }

            for (var elem : deleteSequence) {
                btree.insert(elem);
                set.add(elem);
                assertArrayEquals(set.toArray(), btree.traverse().toArray());
            }

            for (var elem : insertSequence) {
                var deleted = btree.delete(elem);
                Assertions.assertTrue(deleted);
                set.remove(elem);
                Assertions.assertArrayEquals(set.toArray(), btree.traverse().toArray());
            }
        }
    }
}
