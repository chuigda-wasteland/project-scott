package tech.icey.ds;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.icey.basic.ListUtil;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

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

    static List<String> getRandomKeySequence() {
        var ret = new ArrayList<String>();
        for (var i = 1; i < 1000; i++) {
            ret.add(Integer.toString(i));
        }
        Collections.shuffle(ret);
        return ret;
    }

    static List<String> getRandomValueSequence() {
        var ret = new ArrayList<String>();
        var rand1 = getRandomKeySequence();
        var rand2 = getRandomKeySequence();
        for (int i = 0; i < rand1.size(); i++) {
            ret.add(rand1.get(i) + rand2.get(i));
        }
        return ret;
    }

    @Test
    void happyTestDegree3() {
        // 测试肯定过不了
        var bplustree = new BPlusTree(3);
        var map = new TreeMap<String, String>();

        var keySequence = getFixedKeySequence();
        var valueSequence = getFixedValueSequence();

        for (var j = 0; j < 20; j++) {
            bplustree.insert(keySequence.get(j), valueSequence.get(j));
            map.put(keySequence.get(j), valueSequence.get(j));
            Assertions.assertArrayEquals(ListUtil.flatten(map).toArray(), bplustree.traverse().toArray());
        }

        var deletionSequence = getFixedDeletionSequence();
        for (var j = 0; j < 20; j++) {
            var bplusDeleted = bplustree.delete(deletionSequence.get(j));
            var mapDeleted = map.remove(deletionSequence.get(j)) != null;
            Assertions.assertEquals(mapDeleted, bplusDeleted);
            Assertions.assertArrayEquals(ListUtil.flatten(map).toArray(), bplustree.traverse().toArray());
        }
    }

    public static void main(String[] args) {
        var bplustree = new BPlusTree(3);
        var map = new TreeMap<String, String>();

        var keySequence = getFixedKeySequence();
        var valueSequence = getFixedValueSequence();

        for (var j = 0; j < 20; j++) {
            bplustree.insert(keySequence.get(j), valueSequence.get(j));
            map.put(keySequence.get(j), valueSequence.get(j));
            Assertions.assertArrayEquals(ListUtil.flatten(map).toArray(), bplustree.traverse().toArray());
        }

        var deletionSequence = getFixedDeletionSequence();
        for (var j = 0; j < 20; j++) {
            var bplusDeleted = bplustree.delete(deletionSequence.get(j));
            var mapDeleted = map.remove(deletionSequence.get(j)) != null;
            assert mapDeleted == bplusDeleted;
            assert Arrays.equals(ListUtil.flatten(map).toArray(), bplustree.traverse().toArray());
        }
    }
}
