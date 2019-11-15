package tech.icey.ds;

import tech.icey.basic.ListUtil;
import tech.icey.basic.Pair;

import java.util.ArrayList;
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
    abstract Pair<Boolean, BPlusTreeNode> delete(String key);
    abstract protected BPlusTreeLeafNode onChildShrink(BPlusTreeNode child1, BPlusTreeNode child2,
                                                       int separatorIndex, BPlusTreeNode newChild);
    abstract protected int getSeparatorIndex(BPlusTreeNode child1, BPlusTreeNode child2);

    abstract void traverse(List<Pair<String, String>> outputKV);

    protected int degree;
    protected BPlusTreeNode parent, leftSibling, rightSibling;

    protected enum WhichSibling { LeftSibling, RightSibling }
}

class BPlusTreeIntNode extends BPlusTreeNode {
    BPlusTreeIntNode(int degree, BPlusTreeNode parent, BPlusTreeNode leftSibling, BPlusTreeNode rightSibling,
                     List<String> keys, List<BPlusTreeNode> children) {
        super(degree, parent, leftSibling, rightSibling);
        this.keys = keys;
        this.children = children;
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

    @Override
    Pair<Boolean, BPlusTreeNode> delete(String key) {
        return null;
    }

    @Override
    protected BPlusTreeLeafNode onChildShrink(BPlusTreeNode child1, BPlusTreeNode child2,
                                              int separatorIndex, BPlusTreeNode newChild) {
        return null;
    }

    @Override
    protected int getSeparatorIndex(BPlusTreeNode child1, BPlusTreeNode child2) {
        var ret = this.children.indexOf(child1);
        if (child1.rightSibling == child2) {
            return ret;
        } else {
            return ret - 1;
        }
    }

    @Override
    void traverse(List<Pair<String, String>> outputKV) {
        this.children.get(0).traverse(outputKV);
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
            leftNode.setRightSibling(rightNode);
            rightNode.setLeftSibling(leftNode);

            if (leftSibling != null) {
                leftSibling.rightSibling = leftNode;
            }
            if (rightSibling != null) {
                rightSibling.leftSibling = rightNode;
            }

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

            if (leftSibling != null) {
                leftSibling.rightSibling = leftNode;
            }
            if (rightSibling != null) {
                rightSibling.leftSibling = rightNode;
            }

            if (parent == null) {
                var newRoot = new BPlusTreeIntNode(degree, null, null, null,
                                                ListUtil.copy(List.of(powder.getFirst())),
                                                ListUtil.copy(List.of(leftNode, rightNode)));
                leftNode.parent = newRoot;
                rightNode.parent = newRoot;
                return newRoot;
            } else {
                return parent.onChildExplode(this, powder.getFirst(), leftNode, rightNode);
            }
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

    @Override
    Pair<Boolean, BPlusTreeNode> delete(String key) {
        var index = getDeletionIndex(key);
        if (index == -1) {
            return new Pair<>(false, null);
        }
        kvPairs.remove(index);
        return new Pair<>(true, maybeShrink());
    }

    private BPlusTreeNode maybeShrink() {
        if (this.kvPairs.size() * 2 < degree) {
            var siblingP = chooseSibling();
            var sibling = (BPlusTreeLeafNode) siblingP.getFirst();
            var whichSibling = siblingP.getSecond();
            var separatorIndex = getSeparatorIndex(this, sibling);
            var allKVPairs = new ArrayList<Pair<String, String>>();
            if (whichSibling == WhichSibling.LeftSibling) {
                allKVPairs.addAll(sibling.kvPairs);
                allKVPairs.addAll(this.kvPairs);
            } else {
                allKVPairs.addAll(this.kvPairs);
                allKVPairs.addAll(sibling.kvPairs);
            }

            if (allKVPairs.size() < degree) {
                BPlusTreeNode newNodeLeftSibling = null, newNodeRightSibling = null;
                if (whichSibling == WhichSibling.LeftSibling) {
                    newNodeLeftSibling = sibling.leftSibling;
                    newNodeRightSibling = this.rightSibling;
                } else {
                    newNodeLeftSibling = this.leftSibling;
                    newNodeRightSibling = sibling.rightSibling;
                }
                var newNode = new BPlusTreeLeafNode(degree, this.parent, newNodeLeftSibling, newNodeRightSibling, allKVPairs);
                newNodeLeftSibling.setRightSibling(newNode);
                newNodeRightSibling.setLeftSibling(newNode);
                return onChildShrink(newNodeLeftSibling, newNodeRightSibling, separatorIndex, newNode);
            } else {
                return null;
            }
        }
        return null;
    }

    private Pair<BPlusTreeLeafNode, WhichSibling> chooseSibling() {
        var leftSibling = (BPlusTreeLeafNode)this.leftSibling;
        var rightSibling = (BPlusTreeLeafNode)this.rightSibling;
        if (leftSibling == null || leftSibling.parent != this.parent) {
            return new Pair<>(leftSibling, WhichSibling.LeftSibling);
        } else if (rightSibling == null || rightSibling.parent != this.parent){
            return new Pair<>(rightSibling, WhichSibling.RightSibling);
        } else {
            return leftSibling.kvPairs.size() > rightSibling.kvPairs.size()
                    ? new Pair<>(leftSibling, WhichSibling.LeftSibling)
                    : new Pair<>(rightSibling, WhichSibling.RightSibling);
        }
    }

    private int getDeletionIndex(String key) {
        for (var i = 0; i < kvPairs.size(); i++) {
            if (kvPairs.get(i).getFirst().equals(key)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    protected BPlusTreeLeafNode onChildShrink(BPlusTreeNode child1, BPlusTreeNode child2,
                                              int separatorIndex, BPlusTreeNode newChild) {
        assert false;
        return null;
    }

    @Override
    protected int getSeparatorIndex(BPlusTreeNode child1, BPlusTreeNode child2) {
        assert false;
        return -1;
    }

    @Override
    void traverse(List<Pair<String, String>> outputKV) {
        var it = this;
        while (it != null) {
            outputKV.addAll(it.kvPairs);
            it = (BPlusTreeLeafNode)it.rightSibling;
        }
    }

    private List<Pair<String, String>> kvPairs;
}

public class BPlusTree {
    private BPlusTreeNode rootNode;

    public BPlusTree(int degree) {
        this.rootNode = new BPlusTreeLeafNode(degree, null, null, null, new ArrayList<>());
    }

    public void insert(String key, String value) {
        var newRoot = rootNode.insert(key, value);
        if (newRoot != null) {
            rootNode = newRoot;
        }
    }

    public List<Pair<String, String>> traverse() {
        var ret = new ArrayList<Pair<String, String>>();
        rootNode.traverse(ret);
        return ret;
    }
}
