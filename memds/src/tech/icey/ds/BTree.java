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

        this.leftSibling = null;
        this.rightSibling = null;
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
            for (int i = 0; i < keys.size(); i++) {
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
            if (keys.get(insertionPoint).equals(key)) {
                return null;
            }
            keys.add(insertionPoint, key);
        }
        return maybeExplode();
    }

    private BTreeNode nonLeafInsert(String key) {
        var insertionPoint = findInsertPoint(key);
        if (insertionPoint < keys.size() && keys.get(insertionPoint).equals(key)) {
            return null;
        }
        return children.get(insertionPoint).insert(key);
    }

    private BTreeNode maybeExplode() {
        if (keys.size() >= degree) {
            var powder = keys.get(keys.size() / 2);
            var leftKeys = ListUtil.copy(keys.subList(0, keys.size() / 2));
            var rightKeys = ListUtil.copy(keys.subList(keys.size() / 2 + 1, keys.size()));
            var leftChildren = isLeaf() ? new ArrayList<BTreeNode>()
                                        : ListUtil.copy(children.subList(0, keys.size() / 2 + 1));
            var rightChildren = isLeaf() ? new ArrayList<BTreeNode>()
                                         : ListUtil.copy(children.subList(keys.size() / 2 + 1, keys.size() + 1));
            var leftNode = new BTreeNode(degree, null, leftKeys, leftChildren);
            var rightNode = new BTreeNode(degree, null, rightKeys, rightChildren);

            leftNode.setSibling(this.leftSibling, rightNode);
            rightNode.setSibling(leftNode, this.rightSibling);

            if (parent == null) {
                var newRoot = new BTreeNode(degree, null,
                                            ListUtil.copy(List.of(powder)),
                                            ListUtil.copy(List.of(leftNode, rightNode)));
                leftNode.setParent(newRoot);
                rightNode.setParent(newRoot);
                return newRoot;
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
        leftChild.setParent(this);
        rightChild.setParent(this);
        keys.add(explodedIndex, powder);
        return maybeExplode();
    }

    private void setParent(BTreeNode parent) {
        this.parent = parent;
    }

    private void setSibling(BTreeNode leftSibling, BTreeNode rightSibling) {
        this.leftSibling = leftSibling;
        this.rightSibling = rightSibling;
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
            if (keys.get(i).compareTo(key) >= 0) /*  keys[i] >= key */ {
                return i;
            }
        }
        return keys.size();
    }

    private BTreeNode parent;

    private BTreeNode leftSibling, rightSibling;

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
