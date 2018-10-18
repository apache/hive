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

package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.ql.lib.Node;

/**
 *
 */
public class ASTNode extends CommonTree implements Node,Serializable {
  private static final long serialVersionUID = 1L;
  private transient StringBuilder astStr;
  private transient ASTNodeOrigin origin;
  private transient int startIndx = -1;
  private transient int endIndx = -1;
  private transient ASTNode rootNode;
  private transient boolean isValidASTStr;
  private transient boolean visited = false;

  private static final Interner<ImmutableCommonToken> TOKEN_CACHE = Interners.newWeakInterner();

  public ASTNode() {
  }

  /**
   * @param t Token for the CommonTree Node
   */
  public ASTNode(Token t) {
    super(internToken(t));
  }

  public ASTNode(ASTNode node) {
    super(node);
    this.origin = node.origin;
  }

  @Override
  public Tree dupNode() {
    return new ASTNode(this);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hive.ql.lib.Node#getChildren()
   */
  @Override
  public ArrayList<Node> getChildren() {
    if (super.getChildCount() == 0) {
      return null;
    }

    ArrayList<Node> ret_vec = new ArrayList<Node>();
    for (int i = 0; i < super.getChildCount(); ++i) {
      ret_vec.add((Node) super.getChild(i));
    }

    return ret_vec;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hive.ql.lib.Node#getName()
   */
  @Override
  public String getName() {
    return String.valueOf(super.getToken().getType());
  }

  /**
   * For every node in this subtree, make sure it's start/stop token's
   * are set.  Walk depth first, visit bottom up.  Only updates nodes
   * with at least one token index < 0.
   *
   * In contrast to the method in the parent class, this method is
   * iterative.
   */
  @Override
  public void setUnknownTokenBoundaries() {
    Deque<ASTNode> stack1 = new ArrayDeque<ASTNode>();
    Deque<ASTNode> stack2 = new ArrayDeque<ASTNode>();
    stack1.push(this);

    while (!stack1.isEmpty()) {
      ASTNode next = stack1.pop();
      stack2.push(next);

      if (next.children != null) {
        for (int i = next.children.size() - 1; i >= 0 ; i--) {
          stack1.push((ASTNode)next.children.get(i));
        }
      }
    }

    while (!stack2.isEmpty()) {
      ASTNode next = stack2.pop();

      if (next.children == null) {
        if (next.startIndex < 0 || next.stopIndex < 0) {
          next.startIndex = next.stopIndex = next.token.getTokenIndex();
        }
      } else if (next.startIndex >= 0 && next.stopIndex >= 0) {
        continue;
      } else if (next.children.size() > 0) {
        ASTNode firstChild = (ASTNode)next.children.get(0);
        ASTNode lastChild = (ASTNode)next.children.get(next.children.size()-1);
        next.startIndex = firstChild.getTokenStartIndex();
        next.stopIndex = lastChild.getTokenStopIndex();
      }
    }
  }

  /**
   * @return information about the object from which this ASTNode originated, or
   *         null if this ASTNode was not expanded from an object reference
   */
  public ASTNodeOrigin getOrigin() {
    return origin;
  }

  /**
   * Tag this ASTNode with information about the object from which this node
   * originated.
   */
  public void setOrigin(ASTNodeOrigin origin) {
    this.origin = origin;
  }

  public String dump() {
    StringBuilder sb = new StringBuilder("\n");
    dump(sb);
    return sb.toString();
  }

  private StringBuilder dump(StringBuilder sb) {
    Deque<ASTNode> stack = new ArrayDeque<ASTNode>();
    stack.push(this);
    int tabLength = 0;

    while (!stack.isEmpty()) {
      ASTNode next = stack.peek();

      if (!next.visited) {
        sb.append(StringUtils.repeat(" ", tabLength * 3));
        sb.append(next.toString());
        sb.append("\n");

        if (next.children != null) {
          for (int i = next.children.size() - 1 ; i >= 0 ; i--) {
            stack.push((ASTNode)next.children.get(i));
          }
        }

        tabLength++;
        next.visited = true;
      } else {
        tabLength--;
        next.visited = false;
        stack.pop();
      }
    }

    return sb;
  }

  private void getRootNodeWithValidASTStr () {

    if (rootNode != null && rootNode.parent == null &&
        rootNode.hasValidMemoizedString()) {
      return;
    }
    ASTNode retNode = this;
    while (retNode.parent != null) {
      retNode = (ASTNode) retNode.parent;
    }
    rootNode=retNode;
    if (!rootNode.isValidASTStr) {
      rootNode.astStr = new StringBuilder();
      rootNode.toStringTree(rootNode);
      rootNode.isValidASTStr = true;
    }
    return;
  }

  private boolean hasValidMemoizedString() {
    return isValidASTStr && astStr != null;
  }

  private void resetRootInformation() {
    // Reset the previously stored rootNode string
    if (rootNode != null) {
      rootNode.astStr = null;
      rootNode.isValidASTStr = false;
    }
  }

  private int getMemoizedStringLen() {
    return astStr == null ? 0 : astStr.length();
  }

  private String getMemoizedSubString(int start, int end) {
    return  (astStr == null || start < 0 || end > astStr.length() || start >= end) ? null :
      astStr.subSequence(start, end).toString();
  }

  private void addtoMemoizedString(String string) {
    if (astStr == null) {
      astStr = new StringBuilder();
    }
    astStr.append(string);
  }

  @Override
  public void setParent(Tree t) {
    super.setParent(t);
    resetRootInformation();
  }

  @Override
  public void addChild(Tree t) {
    super.addChild(t);
    resetRootInformation();
  }

  @Override
  public void addChildren(List kids) {
    super.addChildren(kids);
    resetRootInformation();
  }

  @Override
  public void setChild(int i, Tree t) {
    super.setChild(i, t);
    resetRootInformation();
  }

  @Override
  public void insertChild(int i, Object t) {
    super.insertChild(i, t);
    resetRootInformation();
  }

  @Override
  public Object deleteChild(int i) {
   Object ret = super.deleteChild(i);
   resetRootInformation();
   return ret;
  }

  @Override
  public void replaceChildren(int startChildIndex, int stopChildIndex, Object t) {
    super.replaceChildren(startChildIndex, stopChildIndex, t);
    resetRootInformation();
  }

  @Override
  protected List createChildrenList() {
    // Measurements show that in most situations the number of children is small.
    // Avoid wasting memory by creating ArrayList with the default capacity of 10.
    return new ArrayList(2);
  }

  @Override
  public String toStringTree() {

    // The root might have changed because of tree modifications.
    // Compute the new root for this tree and set the astStr.
    getRootNodeWithValidASTStr();

    // If rootNotModified is false, then startIndx and endIndx will be stale.
    if (startIndx >= 0 && endIndx <= rootNode.getMemoizedStringLen()) {
      return rootNode.getMemoizedSubString(startIndx, endIndx);
    }
    return toStringTree(rootNode);
  }

  private String toStringTree(ASTNode rootNode) {
    Deque<ASTNode> stack = new ArrayDeque<ASTNode>();
    stack.push(this);

    while (!stack.isEmpty()) {
      ASTNode next = stack.peek();
      if (!next.visited) {
        if (next.parent != null && next.parent.getChildCount() > 1 &&
                next != next.parent.getChild(0)) {
          rootNode.addtoMemoizedString(" ");
        }

        next.rootNode = rootNode;
        next.startIndx = rootNode.getMemoizedStringLen();

        // Leaf
        if (next.children == null || next.children.size() == 0) {
          String str = next.toString();
          rootNode.addtoMemoizedString(next.getType() != HiveParser.StringLiteral ? str.toLowerCase() : str);
          next.endIndx =  rootNode.getMemoizedStringLen();
          stack.pop();
          continue;
        }

        if ( !next.isNil() ) {
          rootNode.addtoMemoizedString("(");
          String str = next.toString();
          rootNode.addtoMemoizedString((next.getType() == HiveParser.StringLiteral || null == str) ? str :  str.toLowerCase());
          rootNode.addtoMemoizedString(" ");
        }

        if (next.children != null) {
          for (int i = next.children.size() - 1 ; i >= 0 ; i--) {
            stack.push((ASTNode)next.children.get(i));
          }
        }

        next.visited = true;
      } else {
        if ( !next.isNil() ) {
          rootNode.addtoMemoizedString(")");
        }
        next.endIndx = rootNode.getMemoizedStringLen();
        next.visited = false;
        stack.pop();
      }

    }

    return rootNode.getMemoizedSubString(startIndx, endIndx);
  }

  private static Token internToken(Token t) {
    if (t == null) {
      return null;
    }
    if (t instanceof ImmutableCommonToken) {
      return TOKEN_CACHE.intern((ImmutableCommonToken) t);
    } else {
      t.setText(StringInternUtils.internIfNotNull(t.getText()));
      return t;
    }
  }
}
