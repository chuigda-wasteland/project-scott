package tech.icey.ds;

import tech.icey.basic.ListUtil;

import java.util.ArrayList;
import java.util.List;

class BTreeNode {
    BTreeNode(int degree, BTreeNode parent, List<String> keys, List<BTreeNode> children) {
        this.parent = parent;
        this.degree = degree;
        this.keys = keys;
        this.children = children;
    }

    BTreeNode insert(String key) {
        if (this.isLeaf()) {
            return leafInsert(key);
        } else {
            return nonLeafInsert(key);
        }
    }

    void traverse(List<String> outputKeys) {
        if (isLeaf()) {
            outputKeys.addAll(ListUtil.copy(keys));
        } else {
            for (int i = 0; i < outputKeys.size(); i++) {
                children.get(i).traverse(outputKeys);
                outputKeys.add(keys.get(i));
            }
            children.get(children.size() - 1).traverse(outputKeys);
        }
    }

    private boolean isLeaf() {
        return this.children.size() == 0;
    }

    private BTreeNode leafInsert(String key) {
        var insertionPoint = findInsertPoint(key);
        if (insertionPoint == keys.size()) {
            keys.add(key);
        } else {
            keys.add(insertionPoint, key);
        }

        return maybeExplode();
    }

    private BTreeNode nonLeafInsert(String key) {
        var insertionPoint = findInsertPoint(key);
        return children.get(insertionPoint).insert(key);
    }

    private BTreeNode maybeExplode() {
        if (keys.size() >= degree) {
            var powder = keys.get(keys.size() / 2);
            var leftKeys = ListUtil.copy(keys.subList(0, keys.size() / 2));
            var rightKeys = ListUtil.copy(keys.subList(keys.size() / 2 + 1, keys.size()));
            var leftChildren = isLeaf() ? ListUtil.copy(children.subList(0, keys.size() / 2 + 1))
                                        : new ArrayList<BTreeNode>();
            var rightChildren = isLeaf() ? ListUtil.copy(children.subList(keys.size() / 2 + 1, keys.size() + 1))
                                         : new ArrayList<BTreeNode>();
            var leftNode = new BTreeNode(degree, null, leftKeys, leftChildren);
            var rightNode = new BTreeNode(degree, null, rightKeys, rightChildren);

            if (parent == null) {
                return new BTreeNode(degree, null, List.of(powder), List.of(leftNode, rightNode));
            } else {
                return parent.onChildExplode(this, powder, leftNode, rightNode);
            }
        } else {
            return null;
        }
    }

    private BTreeNode onChildExplode(BTreeNode exploded, String powder, BTreeNode leftChild, BTreeNode rightChild) {
        var explodedIndex = findChild(exploded);
        children.remove(explodedIndex);
        children.add(explodedIndex, rightChild);
        children.add(explodedIndex, leftChild);
        keys.add(explodedIndex - 1, powder);
        return maybeExplode();
    }

    private int findChild(BTreeNode child) {
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i) == child) {
                return i;
            }
        }
        return -1;
    }

    private int findInsertPoint(String key) {
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).compareTo(key) > 0) /*  keys[i] < key */ {
                return i;
            }
        }
        return keys.size();
    }

    private BTreeNode parent;

    private int degree;

    private List<String> keys;

    private List<BTreeNode> children;
}

public class BTree {
    public BTree(int degree) {
        this.rootNode = new BTreeNode(degree, null, new ArrayList<>(), new ArrayList<>());
    }

    public void insert(String key) {
        var newRoot = rootNode.insert(key);
        if (newRoot != null) {
            rootNode = newRoot;
        }
    }

    public List<String> traverse() {
        var ret = new ArrayList<String>();
        rootNode.traverse(ret);
        return ret;
    }

    private BTreeNode rootNode;
}
