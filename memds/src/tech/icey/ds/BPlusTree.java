package tech.icey.ds;

import tech.icey.basic.ListUtil;
import tech.icey.basic.Pair;

import java.util.Collections;
import java.util.List;

abstract class BPlusTreeNode {
    BPlusTreeNode(int degree, BPlusTreeNode parent, BPlusTreeNode leftSibling, BPlusTreeNode rightSibling) {
        this.degree = degree;
        this.parent = parent;
        this.leftSibling = leftSibling;
        this.rightSibling = rightSibling;
    }

    void setParent(BPlusTreeNode parent) {
        this.parent = parent;
    }

    void setSiblings(BPlusTreeNode leftSibling, BPlusTreeNode rightSibling) {
        this.leftSibling = leftSibling;
        this.rightSibling = rightSibling;
    }

    void setLeftSibling(BPlusTreeNode leftSibling) {
        this.leftSibling = leftSibling;
    }

    void setRightSibling(BPlusTreeNode rightSibling) {
        this.rightSibling = rightSibling;
    }

    abstract BPlusTreeNode insert(String key, String value);
    abstract protected BPlusTreeNode onChildExplode(BPlusTreeNode exploded, String powder,
                                                    BPlusTreeNode leftChild, BPlusTreeNode rightChild);

    protected int degree;
    protected BPlusTreeNode parent, leftSibling, rightSibling;
}

class BPlusTreeIntNode extends BPlusTreeNode {
    BPlusTreeIntNode(int degree, BPlusTreeNode parent, BPlusTreeNode leftSibling, BPlusTreeNode rightSibling,
                     List<String> keys, List<BPlusTreeNode> children) {
        super(degree, parent, leftSibling, rightSibling);
    }

    @Override
    BPlusTreeNode insert(String key, String value) {
        for (var i = 0; i < keys.size(); i++) {
            if (keys.get(i).compareTo(key) > 0) {
                return children.get(i).insert(key, value);
            }
        }
        return children.get(children.size() - 1).insert(key, value);
    }

    @Override
    protected BPlusTreeNode onChildExplode(BPlusTreeNode exploded, String powder,
                                           BPlusTreeNode leftChild, BPlusTreeNode rightChild) {
        var explodedIndex = children.indexOf(exploded);
        children.remove(explodedIndex);
        children.add(explodedIndex, rightChild);
        children.add(explodedIndex, leftChild);
        keys.add(explodedIndex, powder);
        return maybeExplode();
    }

    private BPlusTreeNode maybeExplode() {
        if (this.children.size() > degree) {
            var powder = keys.get(keys.size() / 2);
            var leftKeys = ListUtil.copy(keys.subList(0, keys.size() / 2));
            var rightKeys = ListUtil.copy(keys.subList(keys.size() / 2 + 1, keys.size()));
            var leftChildren = ListUtil.copy(children.subList(0, keys.size() / 2 + 1));
            var rightChildren = ListUtil.copy(children.subList(keys.size() / 2 + 1, keys.size() + 1));

            var leftNode = new BPlusTreeIntNode(degree, this.parent, this.leftSibling,
                                     null, leftKeys, leftChildren);
            var rightNode = new BPlusTreeIntNode(degree, this.parent, null,
                                                 this.rightSibling, rightKeys, rightChildren);
            for (var child : leftChildren) {
                child.setParent(leftNode);
            }
            for (var child : rightChildren) {
                child.setParent(rightNode);
            }

            if (this.parent == null) {
                var newRoot = new BPlusTreeIntNode(degree, null, null, null,
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

    private List<String> keys;
    private List<BPlusTreeNode> children;
}

class BPlusTreeLeafNode extends BPlusTreeNode {
    BPlusTreeLeafNode(int degree, BPlusTreeNode parent,
                      BPlusTreeNode leftSibling, BPlusTreeNode rightSibling,
                      List<Pair<String, String>> kvPairs) {
        super(degree, parent, leftSibling, rightSibling);
        this.kvPairs = kvPairs;
    }

    @Override
    BPlusTreeNode insert(String key, String value) {
        for (var i = 0; i < kvPairs.size(); i++) {
            var compareResult = kvPairs.get(i).getFirst().compareTo(key);
            if (compareResult > 0) {
                kvPairs.add(i, new Pair<>(key, value));
                return maybeExplode();
            } else if (compareResult == 0) {
                return null;
            }
        }
        kvPairs.add(new Pair<>(key, value));
        return maybeExplode();
    }

    private BPlusTreeNode maybeExplode() {
        if (this.kvPairs.size() >= degree) {
            var powder = this.kvPairs.get(this.kvPairs.size() / 2);
            var leftKVPairs = ListUtil.copy(this.kvPairs.subList(0, this.kvPairs.size() / 2));
            var rightKVPairs = ListUtil.copy(this.kvPairs.subList(this.kvPairs.size() / 2, this.kvPairs.size()));

            var leftNode = new BPlusTreeLeafNode(degree, this.parent, this.leftSibling, null, leftKVPairs);
            var rightNode = new BPlusTreeLeafNode(degree, this.parent, null, this.rightSibling, rightKVPairs);
            leftNode.setRightSibling(rightNode);
            rightNode.setLeftSibling(leftNode);

            return parent.onChildExplode(this, powder.getFirst(), leftNode, rightNode);
        } else {
            return null;
        }
    }

    @Override
    protected BPlusTreeNode onChildExplode(BPlusTreeNode exploded, String powder,
                                           BPlusTreeNode leftChild, BPlusTreeNode rightChild) {
        assert false;
        return null;
    }

    private List<Pair<String, String>> kvPairs;
}

public class BPlusTree {
}
