package tech.icey.ds;

import tech.icey.basic.ListUtil;
import tech.icey.basic.Pair;

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

    Pair<BTreeNode, Boolean> delete(String key) {
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
        if (this.parent == null) {
            return null;
        }

        if (this.keys.size() * 2 < degree) {
            var siblingP = chooseSibling();
            var sibling = siblingP.getFirst();
            var whichSibling = siblingP.getSecond();

            var separatorP = this.parent.getSeparator(this, sibling);
            assert separatorP != null;
            var separator = separatorP.getFirst();
            var separatorIndex = separatorP.getSecond();

            var allKeys = new ArrayList<String>();
            var allChildren = isLeaf() ? null : new ArrayList<BTreeNode>();

            if (whichSibling == WhichSibling.LeftSibling) {
                allKeys.addAll(sibling.keys);
                allKeys.add(separator);
                allKeys.addAll(this.keys);
                if (allChildren != null) {
                    allChildren.addAll(sibling.children);
                    allChildren.addAll(this.children);
                }
            } else {
                allKeys.addAll(this.keys);
                allKeys.add(separator);
                allKeys.addAll(sibling.keys);
                if (allChildren != null) {
                    allChildren.addAll(this.children);
                    allChildren.addAll(sibling.children);
                }
            }

            if (allKeys.size() < degree) {
                var newNode = new BTreeNode(degree, parent, allKeys, allChildren);
                if (allChildren != null) {
                    for (var child : allChildren) {
                        child.setParent(newNode);
                    }
                }
                if (whichSibling == WhichSibling.LeftSibling) {
                    newNode.setSiblings(sibling.leftSibling, this.rightSibling);
                    if (sibling.leftSibling != null) {
                        sibling.leftSibling.setRightSibling(newNode);
                    }
                    if (this.rightSibling != null) {
                        this.rightSibling.setLeftSibling(newNode);
                    }
                    return parent.onChildrenShrink(sibling, this, newNode, separatorIndex);
                } else {
                    newNode.setSiblings(this.leftSibling, sibling.rightSibling);
                    if (this.leftSibling != null) {
                        this.leftSibling.setRightSibling(newNode);
                    }
                    if (sibling.rightSibling != null) {
                        sibling.rightSibling.setLeftSibling(newNode);
                    }
                    return parent.onChildrenShrink(this, sibling, newNode, separatorIndex);
                }
            } else {
                var leftKeys = ListUtil.copy(allKeys.subList(0, allKeys.size() / 2));
                var rightKeys = ListUtil.copy(allKeys.subList(allKeys.size() / 2 + 1, allKeys.size()));
                var newSpearator = allKeys.get(allKeys.size() / 2);
                var leftChildren =
                      allChildren == null ? null : ListUtil.copy(allChildren.subList(0, (allChildren.size() + 1) / 2));
                var rightChildren =
                      allChildren == null ? null : ListUtil.copy(allChildren.subList((allChildren.size() + 1) / 2,
                                                                                      allChildren.size()));
                if (whichSibling == WhichSibling.LeftSibling) {
                    sibling.keys = leftKeys;
                    this.keys = rightKeys;
                    sibling.children = leftChildren;
                    this.children = rightChildren;
                    if (allChildren != null) {
                        for (var child : leftChildren) {
                            child.setParent(sibling);
                        }
                        for (var child : rightChildren) {
                            child.setParent(this);
                        }
                    }
                } else {
                    this.keys = leftKeys;
                    sibling.keys = rightKeys;
                    this.children = leftChildren;
                    sibling.children = rightChildren;
                    if (allChildren != null) {
                        for (var child : leftChildren) {
                            child.setParent(this);
                        }
                        for (var child : rightChildren) {
                            child.setParent(sibling);
                        }
                    }
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

    private BTreeNode onChildrenShrink(BTreeNode left, BTreeNode right, BTreeNode newNode, int separatorIndex) {
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

    private enum WhichSibling {
        LeftSibling, RightSibling
    }

    private Pair<BTreeNode, WhichSibling> chooseSibling() {
        if (this.leftSibling == null || this.leftSibling.parent != this.parent) {
            return new Pair<>(this.rightSibling, WhichSibling.RightSibling);
        } else if (this.rightSibling == null || this.rightSibling.parent != this.parent) {
            return new Pair<>(this.leftSibling, WhichSibling.LeftSibling);
        } else {
            return this.leftSibling.keys.size() > this.rightSibling.keys.size()
                       ? new Pair<>(this.leftSibling, WhichSibling.LeftSibling)
                       : new Pair<>(this.rightSibling, WhichSibling.RightSibling);
        }
    }

    private Pair<String, BTreeNode> findAdjacentKey(String key, int keyIndex) {
        var it = this.children.get(keyIndex + 1);
        while (!it.isLeaf()) {
            it = it.children.get(0);
        }
        return new Pair<>(it.keys.get(0), it);
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
            var leftNode = new BTreeNode(degree, null, leftKeys, null);
            var rightNode = new BTreeNode(degree, null, rightKeys, null);
            if (!isLeaf()) {
                var leftChildren = ListUtil.copy(children.subList(0, keys.size() / 2 + 1));
                var rightChildren = ListUtil.copy(children.subList(keys.size() / 2 + 1, keys.size() + 1));
                for (var child : leftChildren) {
                    child.setParent(leftNode);
                }
                for (var child : rightChildren) {
                    child.setParent(rightNode);
                }
                leftNode.setChildren(leftChildren);
                rightNode.setChildren(rightChildren);
            }

            if (this.leftSibling != null) {
                this.leftSibling.setRightSibling(leftNode);
            }
            if (this.rightSibling != null) {
                this.rightSibling.setLeftSibling(rightNode);
            }
            leftNode.setSiblings(this.leftSibling, rightNode);
            rightNode.setSiblings(leftNode, this.rightSibling);

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
        var explodedIndex = children.indexOf(exploded);
        children.remove(explodedIndex);
        children.add(explodedIndex, rightChild);
        children.add(explodedIndex, leftChild);
        leftChild.setParent(this);
        rightChild.setParent(this);
        keys.add(explodedIndex, powder);
        return maybeExplode();
    }

    private void setChildren(ArrayList<BTreeNode> children) {
        this.children = children;
    }

    private void setParent(BTreeNode parent) {
        this.parent = parent;
    }

    private void setSiblings(BTreeNode leftSibling, BTreeNode rightSibling) {
        this.leftSibling = leftSibling;
        this.rightSibling = rightSibling;
    }

    private void setLeftSibling(BTreeNode leftSibling) {
        this.leftSibling = leftSibling;
    }

    private void setRightSibling(BTreeNode rightSibling) {
        this.rightSibling = rightSibling;
    }

    private int findInsertPoint(String key) {
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).compareTo(key) >= 0) /*  keys[i] >= key */ {
                return i;
            }
        }
        return keys.size();
    }

    private boolean isLeaf() {
        return this.children == null;
    }

    private BTreeNode parent;

    private BTreeNode leftSibling, rightSibling;

    private int degree;

    private List<String> keys;

    private List<BTreeNode> children;
}

public class BTree {
    public BTree(int degree) {
        this.rootNode = new BTreeNode(degree, null, new ArrayList<>(), null);
    }

    public void insert(String key) {
        var newRoot = rootNode.insert(key);
        if (newRoot != null) {
            rootNode = newRoot;
        }
    }

    public boolean delete(String key) {
        var result = rootNode.delete(key);
        if (result.getFirst() != null) {
            rootNode = result.getFirst();
        }
        return result.getSecond();
    }

    public List<String> traverse() {
        var ret = new ArrayList<String>();
        rootNode.traverse(ret);
        return ret;
    }

    private BTreeNode rootNode;
}
