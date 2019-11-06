package tech.icey.ds;

import tech.icey.basic.ListUtil;
import tech.icey.basic.Pair;

import java.util.ArrayList;
import java.util.List;

class BTreeNode {
    public BTreeNode(int degree, BTreeNode parent, List<String> keys, List<BTreeNode> children) {
        this.parent = parent;
        this.degree = degree;
        this.keys = keys;
        this.children = children;

        this.leftSibling = null;
        this.rightSibling = null;
    }

    public BTreeNode insert(String key) {
        if (this.isLeaf()) {
            return leafInsert(key);
        } else {
            return nonLeafInsert(key);
        }
    }

    public Pair<BTreeNode, Boolean> delete(String key) {
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).equals(key)) {
                return new Pair<>(localDelete(key, i), true);
            }
        }
        if (this.isLeaf()) {
            return new Pair<>(null, false);
        } else {
            for (int i = 0; i < keys.size(); i++) {
                if (keys.get(i).compareTo(key) > 0) {
                    return this.children.get(i).delete(key);
                }
            }
            return this.children.get(this.keys.size()).delete(key);
        }
    }

    private Pair<String, Integer> getSeparator(BTreeNode child1, BTreeNode child2) {
        for (int i = 0; i < children.size() - 1; i++) {
            if (children.get(i) == child1 && children.get(i + 1) == child2
                || children.get(i) == child2 && children.get(i + 1) == child1) {
                return new Pair<>(keys.get(i), i);
            }
        }
        assert false;
        return null;
    }

    private BTreeNode maybeShrink() {
        if (this.keys.size() * 2 < degree) {
            var siblingChoosed = chooseSibling();
            var sibling = siblingChoosed.getFirst();
            var whichSibling = siblingChoosed.getSecond();

            var separatorChoosed = this.parent.getSeparator(this, sibling);
            assert separatorChoosed != null;
            var separator = separatorChoosed.getFirst();
            var separatorIndex = separatorChoosed.getSecond();

            var allKeys = new ArrayList<String>();
            var allChildren = new ArrayList<BTreeNode>();

            if (whichSibling == WhichSiblingChoosed.LeftSibling) {
                allKeys.addAll(sibling.keys);
                allKeys.addAll(this.keys);
                allKeys.add(separator);
                allChildren.addAll(sibling.children);
                allChildren.addAll(this.children);
            } else {
                allKeys.addAll(this.keys);
                allKeys.addAll(sibling.keys);
                allKeys.add(separator);
                allChildren.addAll(this.children);
                allChildren.addAll(sibling.children);
            }

            if (sibling.keys.size() + keys.size() < degree) {
                var newNode = new BTreeNode(degree, parent, allKeys, new ArrayList<>());
                if (whichSibling == WhichSiblingChoosed.LeftSibling) {
                    newNode.setSibling(sibling.leftSibling, this.rightSibling);
                    return onChildShrink(sibling, this, newNode, separatorIndex);
                } else {
                    newNode.setSibling(this.leftSibling, sibling.rightSibling);
                    return onChildShrink(this, sibling, newNode, separatorIndex);
                }
            } else {
                var leftKeys = ListUtil.copy(allKeys.subList(0, allKeys.size() / 2));
                var rightKeys = ListUtil.copy(allKeys.subList(allKeys.size() / 2, allKeys.size()));
                var newSpearator = allKeys.get(allKeys.size() / 2);
                var leftChildren = ListUtil.copy(allChildren.subList(0, allKeys.size() / 2));
                var rightChildren = ListUtil.copy(allChildren.subList(allKeys.size() / 2, allKeys.size()));
                if (whichSibling == WhichSiblingChoosed.LeftSibling) {
                    sibling.keys = leftKeys;
                    this.keys = rightKeys;
                    sibling.children = leftChildren;
                    this.children = rightChildren;
                } else {
                    this.keys = leftKeys;
                    sibling.keys = rightKeys;
                    this.children = leftChildren;
                    sibling.children = rightChildren;
                }
                this.parent.keys.set(separatorIndex, newSpearator);
                return null;
            }
        } else {
            return null;
        }
    }

    private BTreeNode localDelete(String key, int keyIndex) {
        if (this.isLeaf()) {
            this.keys.remove(keyIndex);
            return maybeShrink();
        } else {
            var adjacent = this.findAdjacentKey(key, keyIndex);
            this.keys.set(keyIndex, adjacent.getFirst());
            return adjacent.getSecond().delete(adjacent.getFirst()).getFirst();
        }
    }

    private BTreeNode onChildShrink(BTreeNode left, BTreeNode right, BTreeNode newNode, int separatorIndex) {
        keys.remove(separatorIndex);
        children.remove(left);
        children.remove(right);
        children.add(separatorIndex, newNode);

        if (this.parent == null && this.keys.size() == 0) {
            newNode.parent = null;
            return newNode;
        }

        return maybeShrink();
    }

    private enum WhichSiblingChoosed {
        LeftSibling, RightSibling
    }

    private Pair<BTreeNode, WhichSiblingChoosed> chooseSibling() {
        if (this.leftSibling == null) {
            return new Pair<>(this.rightSibling, WhichSiblingChoosed.RightSibling);
        } else if (this.rightSibling == null) {
            return new Pair<>(this.leftSibling, WhichSiblingChoosed.LeftSibling);
        } else {
            return this.leftSibling.keys.size() > this.rightSibling.keys.size()
                       ? new Pair<>(this.leftSibling, WhichSiblingChoosed.LeftSibling)
                       : new Pair<>(this.rightSibling, WhichSiblingChoosed.RightSibling);
        }
    }

    private Pair<String, BTreeNode> findAdjacentKey(String key, int keyIndex) {
        var it = this.children.get(keyIndex + 1);
        while (!it.isLeaf()) {
            it = it.children.get(0);
        }
        return new Pair<>(it.keys.get(0), it);
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
