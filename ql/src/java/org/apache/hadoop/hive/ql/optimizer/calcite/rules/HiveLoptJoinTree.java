/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.hadoop.hive.ql.optimizer.calcite.Bug;
import org.checkerframework.checker.initialization.qual.NotOnlyInitialized;
import org.checkerframework.checker.initialization.qual.UnderInitialization;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class HiveLoptJoinTree {
  //~ Instance fields --------------------------------------------------------

  @NotOnlyInitialized
  private final BinaryTree factorTree;
  private final RelNode joinTree;
  private final boolean removableSelfJoin;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a join-tree consisting of a single node.
   *
   * @param joinTree RelNode corresponding to the single node
   * @param factorId factor id of the node
   */
  @SuppressWarnings("argument.type.incompatible")
  public HiveLoptJoinTree(RelNode joinTree, int factorId) {
    if (Bug.CALCITE_6737_FIXED) {
      throw new IllegalStateException("This class is redundant when the fix for CALCITE-6737 is merged into Calcite. " +
              "Remove it and use LoptOptimizeJoinRule instead of HiveLoptOptimizeJoinRule");
    }

    this.joinTree = joinTree;
    this.factorTree = new Leaf(factorId, this);
    this.removableSelfJoin = false;
  }

  /**
   * Associates the factor ids with a join-tree.
   *
   * @param joinTree RelNodes corresponding to the join tree
   * @param factorTree tree of the factor ids
   * @param removableSelfJoin whether the join corresponds to a removable
   * self-join
   */
  public HiveLoptJoinTree(
          RelNode joinTree,
          BinaryTree factorTree,
          boolean removableSelfJoin) {
    if (Bug.CALCITE_6737_FIXED) {
      throw new IllegalStateException("This class is redundant when the fix for CALCITE-6737 is merged into Calcite. " +
              "Remove it and use LoptOptimizeJoinRule instead of HiveLoptOptimizeJoinRule");
    }

    this.joinTree = joinTree;
    this.factorTree = factorTree;
    this.removableSelfJoin = removableSelfJoin;
  }

  /**
   * Associates the factor ids with a join-tree given the factors corresponding
   * to the left and right subtrees of the join.
   *
   * @param joinTree RelNodes corresponding to the join tree
   * @param leftFactorTree tree of the factor ids for left subtree
   * @param rightFactorTree tree of the factor ids for the right subtree
   */
  public HiveLoptJoinTree(
          RelNode joinTree,
          BinaryTree leftFactorTree,
          BinaryTree rightFactorTree) {
    this(joinTree, leftFactorTree, rightFactorTree, false);
  }

  /**
   * Associates the factor ids with a join-tree given the factors corresponding
   * to the left and right subtrees of the join. Also indicates whether the
   * join is a removable self-join.
   *
   * @param joinTree RelNodes corresponding to the join tree
   * @param leftFactorTree tree of the factor ids for left subtree
   * @param rightFactorTree tree of the factor ids for the right subtree
   * @param removableSelfJoin true if the join is a removable self-join
   */
  public HiveLoptJoinTree(
          RelNode joinTree,
          BinaryTree leftFactorTree,
          BinaryTree rightFactorTree,
          boolean removableSelfJoin) {
    if (Bug.CALCITE_6737_FIXED) {
      throw new IllegalStateException("This class is redundant when the fix for CALCITE-6737 is merged into Calcite. "
              + "Remove it and use LoptOptimizeJoinRule instead of HiveLoptOptimizeJoinRule");
    }

    factorTree = new HiveLoptJoinTree.Node(leftFactorTree, rightFactorTree, this);
    this.joinTree = joinTree;
    this.removableSelfJoin = removableSelfJoin;
  }

  //~ Methods ----------------------------------------------------------------

  public RelNode getJoinTree() {
    return joinTree;
  }

  public HiveLoptJoinTree getLeft() {
    final HiveLoptJoinTree.Node node = (HiveLoptJoinTree.Node) factorTree;
    return new HiveLoptJoinTree(
            ((Join) joinTree).getLeft(),
            node.getLeft(),
            node.getLeft().getParent().isRemovableSelfJoin());
  }

  public HiveLoptJoinTree getRight() {
    final HiveLoptJoinTree.Node node = (HiveLoptJoinTree.Node) factorTree;
    return new HiveLoptJoinTree(
            ((Join) joinTree).getRight(),
            node.getRight(),
            node.getRight().getParent().isRemovableSelfJoin());
  }

  public BinaryTree getFactorTree() {
    return factorTree;
  }

  public List<Integer> getTreeOrder() {
    List<Integer> treeOrder = new ArrayList<>();
    getTreeOrder(treeOrder);
    return treeOrder;
  }

  public void getTreeOrder(List<Integer> treeOrder) {
    factorTree.getTreeOrder(treeOrder);
  }

  public boolean isRemovableSelfJoin() {
    return removableSelfJoin;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Simple binary tree class that stores an id in the leaf nodes and keeps
   * track of the parent HiveLoptJoinTree object associated with the binary tree.
   */
  protected abstract static class BinaryTree {
    @NotOnlyInitialized
    private final HiveLoptJoinTree parent;

    protected BinaryTree(@UnderInitialization HiveLoptJoinTree parent) {
      this.parent = parent;
    }

    public HiveLoptJoinTree getParent() {
      return parent;
    }

    public abstract void getTreeOrder(List<Integer> treeOrder);
  }

  /** Binary tree node that has no children. */
  protected static class Leaf extends BinaryTree {
    private final int id;

    public Leaf(int rootId, @UnderInitialization HiveLoptJoinTree parent) {
      super(parent);
      this.id = rootId;
    }

    /** Returns the id associated with a leaf node in a binary tree. */
    public int getId() {
      return id;
    }

    @Override public void getTreeOrder(List<Integer> treeOrder) {
      treeOrder.add(id);
    }
  }

  /** Binary tree node that has two children. */
  protected static class Node extends BinaryTree {
    private final BinaryTree left;
    private final BinaryTree right;

    public Node(BinaryTree left, BinaryTree right, @UnderInitialization HiveLoptJoinTree parent) {
      super(parent);
      this.left = Objects.requireNonNull(left, "left");
      this.right = Objects.requireNonNull(right, "right");
    }

    public BinaryTree getLeft() {
      return left;
    }

    public BinaryTree getRight() {
      return right;
    }

    @Override public void getTreeOrder(List<Integer> treeOrder) {
      left.getTreeOrder(treeOrder);
      right.getTreeOrder(treeOrder);
    }
  }
}
