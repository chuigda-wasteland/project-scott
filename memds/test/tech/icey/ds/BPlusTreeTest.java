package tech.icey.ds;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.icey.basic.ListUtil;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class BPlusTreeTest {
    static List<String> getFixedKeySequence() {
        return List.of("17", "1", "15", "2", "12", "16", "19",
                       "18", "5", "6", "13", "14", "8", "7", "11",
                       "3", "9", "4", "20", "10");
    }

    static List<String> getFixedValueSequence() {
        return List.of("心中想的就是她", "任凭梦里三千落花", "走遍天涯心随你起落", "看惯了长风", "吹动你英勇的头发",
                       "我心中想得还是她", "哭也欢乐", "悲也潇洒", "只是我的心一直在问", "用什么",
                       "把你永久留下", "莫笑旧衣衫旧长剑走过天涯", "且看风为屏草为席天地为家",
                       "意气刀之尖血之涯划过春夏", "把酒一生笑一生醉一世为侠", "轰烈流沙枕上白发杯中酒比划",
                       "年少风雅鲜衣怒马也不过一刹那", "难免疏漏儿时檐下莫测变化", "隔却山海", "转身从容煎茶");
    }

    static List<String> getFixedDeletionSequence() {
        return List.of("17", "15", "16", "19", "18", "5", "6", "13",
                       "14", "8", "7", "11", "3", "9", "4", "20", "10",
                       "1", "2", "12");
    }

    static List<String> getRandomKeySequence(int size) {
        var ret = new ArrayList<String>();
        for (var i = 0; i < size; i++) {
            ret.add(Integer.toString(i));
        }
        Collections.shuffle(ret);
        return ret;
    }

    static List<String> getRandomValueSequence(int size) {
        var ret = new ArrayList<String>();
        var rand1 = getRandomKeySequence(size);
        var rand2 = getRandomKeySequence(size);
        for (int i = 0; i < rand1.size(); i++) {
            ret.add(rand1.get(i) + rand2.get(i));
        }
        return ret;
    }

    @Test
    void happyTestDegree3() {
        testSimpleInsertDelete(3, getFixedKeySequence(), getFixedDeletionSequence(), getFixedDeletionSequence());
    }

    @Test
    void happyTestDegree4() {
        testSimpleInsertDelete(4, getFixedKeySequence(), getFixedDeletionSequence(), getFixedDeletionSequence());
    }

    @Test
    void happyTestDegree5() {
        testSimpleInsertDelete(5, getFixedKeySequence(), getFixedDeletionSequence(), getFixedDeletionSequence());
    }

    @Test
    void thousandElementDegree3() {
        testSimpleInsertDelete(3, getRandomKeySequence(1024), getRandomValueSequence(1024),
                               getRandomKeySequence(1024));
    }

    @Test
    void thousandElementDegree4() {
        testSimpleInsertDelete(4, getRandomKeySequence(1024), getRandomValueSequence(1024),
                getRandomKeySequence(1024));
    }

    @Test
    void thousandElementDegree20() {
        testSimpleInsertDelete(20, getRandomKeySequence(1024), getRandomValueSequence(1024),
                getRandomKeySequence(1024));
    }

    @Test
    void insertDeleteMixTestSimple() {
       mixInsertAndDelete(3, 20, 3);
    }

    @Test
    void insertDeleteMixDegree3() {
        mixInsertAndDelete(3, 1000, 50);
    }

    @Test
    void insertDeleteMixDegree4() {
        mixInsertAndDelete(4, 1000, 50);
    }

    @Test
    void insertDeleteMixDegree20() {
        mixInsertAndDelete(20, 1000, 50);
    }

    void mixInsertAndDelete(int degree, int initSize, int batchSize) {
        var r = new Random();
        for (var i = 0; i < 10; i++) {
            var bplustree = new BPlusTree(degree);
            var map = new TreeMap<String, String>();
            var keySequence = getRandomKeySequence(initSize);
            var valueSequence = getRandomValueSequence(initSize);
            for (var j = 0; j < keySequence.size(); j++) {
                bplustree.insert(keySequence.get(j), valueSequence.get(j));
                map.put(keySequence.get(j), valueSequence.get(j));
                assertArrayEquals(ListUtil.flatten(map).toArray(), bplustree.traverse().toArray());
            }

            for (var j = 0; j < 200; j++) {
                var sectionSize = r.nextInt(batchSize);
                var keySequence1 = getRandomKeySequence(initSize).subList(0, sectionSize);
                var valueSequence1 = getRandomValueSequence(initSize).subList(0, sectionSize);
                if (r.nextInt(2) == 1) {
                    for (var k = 0; k < sectionSize; k++) {
                        bplustree.delete(keySequence1.get(k));
                        map.remove(keySequence1.get(k));
                        assertArrayEquals(ListUtil.flatten(map).toArray(), bplustree.traverse().toArray());
                    }
                } else {
                    for (var k = 0; k < sectionSize; k++) {
                        bplustree.insert(keySequence1.get(k), valueSequence1.get(k));
                        map.put(keySequence1.get(k), valueSequence1.get(k));
                        assertArrayEquals(ListUtil.flatten(map).toArray(), bplustree.traverse().toArray());
                    }
                }
            }
        }
    }

    void testSimpleInsertDelete(int degree, List<String> keySequence, List<String> valueSequence,
                                List<String> deleteSequence) {
        for (var i = 0; i < 10; i++) {
            var bplustree = new BPlusTree(degree);
            var map = new TreeMap<String, String>();

            for (var j = 0; j < keySequence.size(); j++) {
                bplustree.insert(keySequence.get(j), valueSequence.get(j));
                map.put(keySequence.get(j), valueSequence.get(j));
                assertArrayEquals(ListUtil.flatten(map).toArray(), bplustree.traverse().toArray());
            }

            for (var elem : deleteSequence) {
                var deleted = bplustree.delete(elem);
                Assertions.assertTrue(deleted);
                map.remove(elem);
                assertArrayEquals(ListUtil.flatten(map).toArray(), bplustree.traverse().toArray());
            }
        }
    }
}
