package tech.icey.ds;

import tech.icey.basic.ListUtil;
import tech.icey.basic.Pair;
import tech.icey.util.DirectedGraph;
import tech.icey.util.GlobalIdAllocator;
import tech.icey.util.Graphvizible;

import java.util.ArrayList;
import java.util.List;

abstract class BPlusTreeNode {
    BPlusTreeNode(int degree, BPlusTreeNode parent, BPlusTreeNode leftSibling, BPlusTreeNode rightSibling) {
        this.degree = degree;
        this.parent = parent;
        this.leftSibling = leftSibling;
        this.rightSibling = rightSibling;
        this.globalId = GlobalIdAllocator.getInstance().nextId();
    }

    void setParent(BPlusTreeNode parent) {
        this.parent = parent;
    }

    void setLeftSibling(BPlusTreeNode leftSibling) {
        this.leftSibling = leftSibling;
    }

    void setRightSibling(BPlusTreeNode rightSibling) {
        this.rightSibling = rightSibling;
    }

    abstract void buildUpDirectedGraph(DirectedGraph d);

    abstract BPlusTreeNode insert(String key, String value);
    abstract protected BPlusTreeNode onChildExplode(BPlusTreeNode exploded, String powder,
                                                    BPlusTreeNode leftChild, BPlusTreeNode rightChild);
    abstract Pair<Boolean, BPlusTreeNode> delete(String key);
    abstract protected BPlusTreeNode onChildrenShrink(BPlusTreeNode child1, BPlusTreeNode child2,
                                                      int separatorIndex, BPlusTreeNode newChild);
    abstract protected Pair<String, Integer> getSeparator(BPlusTreeNode child1, BPlusTreeNode child2);

    abstract void traverse(List<Pair<String, String>> outputKV);

    abstract String buildDescriptor();

    protected int degree;
    protected BPlusTreeNode parent, leftSibling, rightSibling;

    protected int globalId;

    protected abstract void onChildrenReBalance(int separatorIndex, String newSeparator);

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
    void buildUpDirectedGraph(DirectedGraph d) {
        var selfDescriptor = buildDescriptor();
        if (parent != null) {
            d.addEdge(selfDescriptor, parent.buildDescriptor());
        }
        for (var child : children) {
            d.addEdge(selfDescriptor, child.buildDescriptor());
        }
        for (var child : children) {
            child.buildUpDirectedGraph(d);
        }
        if (leftSibling != null) {
            d.addEdge(selfDescriptor, leftSibling.buildDescriptor());
        }
        if (rightSibling != null) {
            d.addEdge(selfDescriptor, rightSibling.buildDescriptor());
            d.addSameRankNodes(selfDescriptor, rightSibling.buildDescriptor());
        }
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
        for (var i = 0; i < this.keys.size(); i++) {
            if (keys.get(i).compareTo(key) > 0) {
                return children.get(i).delete(key);
            }
        }
        return children.get(children.size() - 1).delete(key);
    }

    @Override
    protected BPlusTreeNode onChildrenShrink(BPlusTreeNode child1, BPlusTreeNode child2,
                                             int separatorIndex, BPlusTreeNode newChild) {
        this.children.remove(child1);
        this.children.remove(child2);
        this.children.add(separatorIndex, newChild);
        this.keys.remove(separatorIndex);

        if (this.parent == null && this.keys.size() == 0) {
            this.children.get(0).setParent(null);
            return this.children.get(0);
        } else {
            return maybeShrink();
        }
    }

    private BPlusTreeNode maybeShrink() {
        if (this.keys.size() == 0) {
            var siblingP = chooseSibling();
            var sibling = siblingP.getFirst();
            var whichSibling = siblingP.getSecond();
            var separatorP = parent.getSeparator(this, sibling);
            var separator = separatorP.getFirst();
            var separatorIndex = separatorP.getSecond();
            var allKeys = new ArrayList<String>();
            var allChildren = new ArrayList<BPlusTreeNode>();

            if (whichSibling == WhichSibling.LeftSibling) {
                allKeys.addAll(sibling.keys);
                allKeys.add(separator);
                allKeys.addAll(keys);
                allChildren.addAll(sibling.children);
                allChildren.addAll(this.children);
            } else {
                allKeys.addAll(keys);
                allKeys.add(separator);
                allKeys.addAll(sibling.keys);
                allChildren.addAll(this.children);
                allChildren.addAll(sibling.children);
            }

            if (allKeys.size() < degree) {
                BPlusTreeNode newNodeLeftSibling, newNodeRightSibling;
                if (whichSibling == WhichSibling.LeftSibling) {
                    newNodeLeftSibling = sibling.leftSibling;
                    newNodeRightSibling = this.rightSibling;
                } else {
                    newNodeLeftSibling = this.leftSibling;
                    newNodeRightSibling = sibling.rightSibling;
                }
                var newNode = new BPlusTreeIntNode(degree, this.parent, newNodeLeftSibling,
                                                   newNodeRightSibling, allKeys, allChildren);
                for (var child : allChildren) {
                    child.setParent(newNode);
                }
                if (newNodeLeftSibling != null) {
                    newNodeLeftSibling.setRightSibling(newNode);
                }
                if (newNodeRightSibling != null) {
                    newNodeRightSibling.setLeftSibling(newNode);
                }
                return parent.onChildrenShrink(this, sibling, separatorIndex, newNode);
            } else {
                var leftKeys = ListUtil.copy(allKeys.subList(0, allKeys.size() / 2));
                var rightKeys = ListUtil.copy(allKeys.subList(allKeys.size() / 2 + 1, allKeys.size()));
                var newSeparator = allKeys.get(allKeys.size() / 2);
                var leftChildren = ListUtil.copy(allChildren.subList(0, (allChildren.size() + 1) / 2));
                var rightChildren = ListUtil.copy(allChildren.subList((allChildren.size() + 1) / 2, allChildren.size()));

                if (whichSibling == WhichSibling.LeftSibling) {
                    sibling.keys = leftKeys;
                    sibling.children = leftChildren;
                    this.keys = rightKeys;
                    this.children = rightChildren;
                } else {
                    this.keys = leftKeys;
                    this.children = leftChildren;
                    sibling.keys = rightKeys;
                    sibling.children = rightChildren;
                }

                for (var child : this.children) {
                    child.setParent(this);
                }
                for (var child : sibling.children) {
                    child.setParent(sibling);
                }

                var parent = (BPlusTreeIntNode)this.parent;
                parent.keys.set(separatorIndex, newSeparator);
                return null;
            }
        } else {
            return null;
        }
    }

    private Pair<BPlusTreeIntNode, WhichSibling> chooseSibling() {
        var leftSibling = (BPlusTreeIntNode) this.leftSibling;
        var rightSibling = (BPlusTreeIntNode) this.rightSibling;
        if (leftSibling == null || leftSibling.parent != this.parent) {
            return new Pair<>(rightSibling, WhichSibling.RightSibling);
        } else if (rightSibling == null || rightSibling.parent != this.parent) {
            return new Pair<>(leftSibling, WhichSibling.LeftSibling);
        } else {
            return leftSibling.keys.size() > rightSibling.keys.size()
                    ? new Pair<>(leftSibling, WhichSibling.LeftSibling)
                    : new Pair<>(rightSibling, WhichSibling.RightSibling);
        }
    }

    @Override
    protected Pair<String, Integer> getSeparator(BPlusTreeNode child1, BPlusTreeNode child2) {
        var ret = this.children.indexOf(child1);
        if (child1.leftSibling == child2) {
            ret -= 1;
        }
        return new Pair<>(keys.get(ret), ret);
    }

    @Override
    void traverse(List<Pair<String, String>> outputKV) {
        this.children.get(0).traverse(outputKV);
    }

    @Override
    String buildDescriptor() {
        var builder = new StringBuilder("(");
        builder.append(globalId);
        builder.append(") ");
        if (this.keys.isEmpty()) {
            return builder.toString();
        }
        for (var i = 0; i < this.keys.size() - 1; i++) {
            builder.append(this.keys.get(i));
            builder.append(", ");
        }
        builder.append(this.keys.get(this.keys.size() - 1));
        return builder.toString();
    }

    @Override
    protected void onChildrenReBalance(int separatorIndex, String newSeparator) {
        this.keys.set(separatorIndex, newSeparator);
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
    void buildUpDirectedGraph(DirectedGraph d) {
        var selfDescriptor = buildDescriptor();
        if (parent != null) {
            d.addEdge(selfDescriptor, parent.buildDescriptor());
        }
        if (this.leftSibling != null) {
            d.addEdge(selfDescriptor, leftSibling.buildDescriptor());
        }
        if (this.rightSibling != null) {
            d.addEdge(selfDescriptor, rightSibling.buildDescriptor());
            d.addSameRankNodes(selfDescriptor, rightSibling.buildDescriptor());
        }
    }

    @Override
    BPlusTreeNode insert(String key, String value) {
        for (var i = 0; i < kvPairs.size(); i++) {
            var compareResult = kvPairs.get(i).getFirst().compareTo(key);
            if (compareResult > 0) {
                kvPairs.add(i, new Pair<>(key, value));
                return maybeExplode();
            } else if (compareResult == 0) {
                kvPairs.set(i, new Pair<>(key, value));
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
        if (parent == null) {
            return null;
        }

        if (this.kvPairs.size() == 0) {
            var siblingP = chooseSibling();
            var sibling = siblingP.getFirst();
            var whichSibling = siblingP.getSecond();
            var separatorP = parent.getSeparator(this, sibling);
            var separatorIndex = separatorP.getSecond();
            var allKVPairs = new ArrayList<Pair<String, String>>();
            if (whichSibling == WhichSibling.LeftSibling) {
                allKVPairs.addAll(sibling.kvPairs);
                allKVPairs.addAll(this.kvPairs);
            } else {
                allKVPairs.addAll(this.kvPairs);
                allKVPairs.addAll(sibling.kvPairs);
            }

            if (allKVPairs.size() < degree) {
                BPlusTreeNode newNodeLeftSibling, newNodeRightSibling;
                if (whichSibling == WhichSibling.LeftSibling) {
                    newNodeLeftSibling = sibling.leftSibling;
                    newNodeRightSibling = this.rightSibling;
                } else {
                    newNodeLeftSibling = this.leftSibling;
                    newNodeRightSibling = sibling.rightSibling;
                }
                var newNode = new BPlusTreeLeafNode(degree, this.parent, newNodeLeftSibling,
                                                    newNodeRightSibling, allKVPairs);
                if (newNodeLeftSibling != null) {
                    newNodeLeftSibling.setRightSibling(newNode);
                }
                if (newNodeRightSibling != null) {
                    newNodeRightSibling.setLeftSibling(newNode);
                }
                return parent.onChildrenShrink(this, sibling, separatorIndex, newNode);
            } else {
                var leftKVPairs = ListUtil.copy(allKVPairs.subList(0, allKVPairs.size() / 2));
                var rightKVPairs = ListUtil.copy(allKVPairs.subList(allKVPairs.size() / 2, allKVPairs.size()));
                var newSeparator = allKVPairs.get(allKVPairs.size() / 2).getFirst();
                if (whichSibling == WhichSibling.LeftSibling) {
                    sibling.kvPairs = leftKVPairs;
                    this.kvPairs = rightKVPairs;
                } else {
                    this.kvPairs = leftKVPairs;
                    sibling.kvPairs = rightKVPairs;
                }
                parent.onChildrenReBalance(separatorIndex, newSeparator);
                return null;
            }
        }
        return null;
    }

    private Pair<BPlusTreeLeafNode, WhichSibling> chooseSibling() {
        var leftSibling = (BPlusTreeLeafNode)this.leftSibling;
        var rightSibling = (BPlusTreeLeafNode)this.rightSibling;
        if (leftSibling == null || leftSibling.parent != this.parent) {
            return new Pair<>(rightSibling, WhichSibling.RightSibling);
        } else if (rightSibling == null || rightSibling.parent != this.parent){
            return new Pair<>(leftSibling, WhichSibling.LeftSibling);
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
    protected BPlusTreeNode onChildrenShrink(BPlusTreeNode child1, BPlusTreeNode child2,
                                             int separatorIndex, BPlusTreeNode newChild) {
        assert false;
        return null;
    }

    @Override
    protected Pair<String, Integer> getSeparator(BPlusTreeNode child1, BPlusTreeNode child2) {
        assert false;
        return new Pair<>(null, -1);
    }

    @Override
    void traverse(List<Pair<String, String>> outputKV) {
        var it = this;
        while (it != null) {
            outputKV.addAll(it.kvPairs);
            it = (BPlusTreeLeafNode)it.rightSibling;
        }
    }

    @Override
    String buildDescriptor() {
        var builder = new StringBuilder("(");
        builder.append(globalId);
        builder.append(") ");
        if (this.kvPairs.isEmpty()) {
            return builder.toString();
        }
        for (var i = 0; i < this.kvPairs.size() - 1; i++) {
            builder.append(this.kvPairs.get(i).getFirst());
            builder.append(", ");
        }
        builder.append(this.kvPairs.get(this.kvPairs.size() - 1).getFirst());
        return builder.toString();
    }

    @Override
    protected void onChildrenReBalance(int separatorIndex, String newSeparator) {
        assert false;
    }

    private List<Pair<String, String>> kvPairs;
}

public class BPlusTree implements Graphvizible  {
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

    public boolean delete(String key) {
        var deleteResult = rootNode.delete(key);
        if (deleteResult.getSecond() != null) {
            rootNode = deleteResult.getSecond();
        }
        return deleteResult.getFirst();
    }

    public List<Pair<String, String>> traverse() {
        var ret = new ArrayList<Pair<String, String>>();
        rootNode.traverse(ret);
        return ret;
    }

    @Override
    public DirectedGraph toDirectedGraph() {
        var ret = new DirectedGraph();
        rootNode.buildUpDirectedGraph(ret);
        return ret;
    }
}
