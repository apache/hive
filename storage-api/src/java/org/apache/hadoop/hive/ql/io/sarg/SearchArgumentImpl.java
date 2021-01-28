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

package org.apache.hadoop.hive.ql.io.sarg;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.NoDynamicValuesException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of SearchArguments. Visible for testing only.
 */
public final class SearchArgumentImpl implements SearchArgument {

  private static final Logger LOG = LoggerFactory.getLogger(SearchArgumentImpl.class);

  public static final class PredicateLeafImpl implements PredicateLeaf {
    private final Operator operator;
    private final Type type;
    private String columnName;
    private final Object literal;
    private final List<Object> literalList;
    private int id;

    // Used by kryo
    @SuppressWarnings("unused")
    PredicateLeafImpl() {
      operator = null;
      type = null;
      columnName = null;
      literal = null;
      literalList = null;
      id = -1;
    }

    public PredicateLeafImpl(Operator operator,
                             Type type,
                             String columnName,
                             Object literal,
                             List<Object> literalList) {
      this(operator, type, columnName, literal, literalList, null);
    }

    public PredicateLeafImpl(Operator operator,
                             Type type,
                             String columnName,
                             Object literal,
                             List<Object> literalList, Configuration conf) {
      this.operator = operator;
      this.type = type;
      this.columnName = columnName;
      this.literal = literal;
      this.id = -1;
      checkLiteralType(literal, type, conf);
      this.literalList = literalList;
      if (literalList != null) {
        for(Object lit: literalList) {
          checkLiteralType(lit, type, conf);
        }
      }
    }

    @Override
    public Operator getOperator() {
      return operator;
    }

    @Override
    public Type getType(){
      return type;
    }

    @Override
    public String getColumnName() {
      return columnName;
    }

    @Override
    public Object getLiteral() {
      if (literal instanceof LiteralDelegate) {
        return ((LiteralDelegate) literal).getLiteral();
      }

      return literal;
    }

    @Override
    public List<Object> getLiteralList() {
      if (literalList != null && literalList.size() > 0 && literalList.get(0) instanceof LiteralDelegate) {
        List<Object> newLiteraList = new ArrayList<>();
        try {
          for (Object litertalObj : literalList) {
            Object literal = ((LiteralDelegate) litertalObj).getLiteral();
            if (literal != null) {
              newLiteraList.add(literal);
            }
          }
        } catch (NoDynamicValuesException err) {
          LOG.debug("Error while retrieving literalList, returning null", err);
          return Collections.emptyList();
        }
        return newLiteraList;
      }
      return literalList;
    }

    @Override
    public int getId() {
      return id;
    }

    public void setId(int newId) {
      id = newId;
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append('(');
      buffer.append(operator);
      buffer.append(' ');
      buffer.append(columnName);
      if (literal != null) {
        buffer.append(' ');
        buffer.append(literal);
      } else if (literalList != null) {
        for(Object lit: literalList) {
          buffer.append(' ');
          buffer.append(lit == null ? "null" : lit.toString());
        }
      }
      buffer.append(')');
      return buffer.toString();
    }

    private static boolean isEqual(Object left, Object right) {

      return left == right ||
          (left != null && right != null && left.equals(right));
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || other.getClass() != getClass()) {
        return false;
      } else if (other == this) {
        return true;
      } else {
        PredicateLeafImpl o = (PredicateLeafImpl) other;
        return operator == o.operator &&
            type == o.type &&
            columnName.equals(o.columnName) &&
            isEqual(literal, o.literal) &&
            isEqual(literalList, o.literalList);
      }
    }

    @Override
    public int hashCode() {
      return operator.hashCode() +
             type.hashCode() * 17 +
             columnName.hashCode() * 3 * 17+
             (literal == null ? 0 : literal.hashCode()) * 101 * 3 * 17 +
             (literalList == null ? 0 : literalList.hashCode()) *
                 103 * 101 * 3 * 17;
    }

    public static void setColumnName(PredicateLeaf leaf, String newName) {
      assert leaf instanceof PredicateLeafImpl;
      ((PredicateLeafImpl)leaf).columnName = newName;
    }

    protected void checkLiteralType(Object literal, Type type, Configuration conf) {
      if (literal == null) {
        return;
      }

      if (literal instanceof LiteralDelegate) {
        // Give it a pass. Optionally, have LiteralDelegate provide a getLiteralClass() to check.
        ((LiteralDelegate) literal).setConf(conf);
      } else {
        if (literal.getClass() != type.getValueClass()) {
          throw new IllegalArgumentException("Wrong value class " +
              literal.getClass().getName() + " for " + type + "." + operator +
              " leaf");
        }
      }
    }
  }

  private final List<PredicateLeaf> leaves;
  private final ExpressionTree normalizedExpression;
  private final ExpressionTree compactExpression;

  SearchArgumentImpl(ExpressionTree normalizedExpression,
                     ExpressionTree compactExpression,
                     List<PredicateLeaf> leaves) {
    this.normalizedExpression = normalizedExpression;
    this.compactExpression = compactExpression;
    this.leaves = leaves;
  }

  // Used by kyro
  @SuppressWarnings("unused")
  SearchArgumentImpl() {
        leaves = null;
        normalizedExpression = null;
        compactExpression = null;
  }

  @Override
  public List<PredicateLeaf> getLeaves() {
    return leaves;
  }

  @Override
  public TruthValue evaluate(TruthValue[] leaves) {
    return normalizedExpression == null ? TruthValue.YES
        : normalizedExpression.evaluate(leaves);
  }

  @Override
  public ExpressionTree getExpression() {
    return normalizedExpression;
  }

  @Override
  public ExpressionTree getCompactExpression() {
    return compactExpression;
  }

  @Override
  public String toString() {
    return normalizedExpression.toString();
  }

  /**
   * Generate the backwards compatible string for test cases
   * @return the sarg using the old string
   */
  public String toOldString() {
    StringBuilder buffer = new StringBuilder();
    for(int i=0; leaves != null && i < leaves.size(); ++i) {
      buffer.append("leaf-");
      buffer.append(i);
      buffer.append(" = ");
      buffer.append(leaves.get(i).toString());
      buffer.append(", ");
    }
    buffer.append("expr = ");
    buffer.append(normalizedExpression.toOldString());
    return buffer.toString();
  }

  static class BuilderImpl implements Builder {

    Configuration conf;
    public BuilderImpl(Configuration conf) {
      this.conf = conf;
    }

    // max threshold for CNF conversion. having >8 elements in andList will be
    // converted to maybe
    private static final int CNF_COMBINATIONS_THRESHOLD = 256;

    private final Deque<ExpressionTree> currentTree =
        new ArrayDeque<>();
    private final Map<PredicateLeaf, PredicateLeaf> leaves =
        new HashMap<>();
    private final ExpressionTree root =
        new ExpressionTree(ExpressionTree.Operator.AND);
    {
      currentTree.add(root);
    }

    @Override
    public Builder startOr() {
      ExpressionTree node = new ExpressionTree(ExpressionTree.Operator.OR);
      currentTree.getFirst().getChildren().add(node);
      currentTree.addFirst(node);
      return this;
    }

    @Override
    public Builder startAnd() {
      ExpressionTree node = new ExpressionTree(ExpressionTree.Operator.AND);
      currentTree.getFirst().getChildren().add(node);
      currentTree.addFirst(node);
      return this;
    }

    @Override
    public Builder startNot() {
      ExpressionTree node = new ExpressionTree(ExpressionTree.Operator.NOT);
      currentTree.getFirst().getChildren().add(node);
      currentTree.addFirst(node);
      return this;
    }

    @Override
    public Builder end() {
      ExpressionTree current = currentTree.removeFirst();
      if (current.getChildren().size() == 0) {
        throw new IllegalArgumentException("Can't create expression " + root +
            " with no children.");
      }
      if (current.getOperator() == ExpressionTree.Operator.NOT &&
          current.getChildren().size() != 1) {
        throw new IllegalArgumentException("Can't create not expression " +
            current + " with more than 1 child.");
      }
      return this;
    }

    private PredicateLeaf addLeaf(PredicateLeaf leaf) {
      PredicateLeaf result = leaves.get(leaf);
      if (result == null) {
        leaves.put(leaf, leaf);
        return leaf;
      } else {
        return result;
      }
    }

    @Override
    public Builder lessThan(String column, PredicateLeaf.Type type,
                            Object literal) {
      ExpressionTree parent = currentTree.getFirst();
      if (column == null || literal == null) {
        parent.getChildren().add(new ExpressionTree(TruthValue.YES_NO_NULL));
      } else {
        PredicateLeafImpl leaf =
            new PredicateLeafImpl(PredicateLeaf.Operator.LESS_THAN,
                type, column, literal, null, conf);
        parent.getChildren().add(new ExpressionTree(addLeaf(leaf)));
      }
      return this;
    }

    @Override
    public Builder lessThanEquals(String column, PredicateLeaf.Type type,
                                  Object literal) {
      ExpressionTree parent = currentTree.getFirst();
      if (column == null || literal == null) {
        parent.getChildren().add(new ExpressionTree(TruthValue.YES_NO_NULL));
      } else {
        PredicateLeafImpl leaf =
            new PredicateLeafImpl(PredicateLeaf.Operator.LESS_THAN_EQUALS,
                type, column, literal, null, conf);
        parent.getChildren().add(new ExpressionTree(addLeaf(leaf)));
      }
      return this;
    }

    @Override
    public Builder equals(String column, PredicateLeaf.Type type,
                          Object literal) {
      ExpressionTree parent = currentTree.getFirst();
      if (column == null || literal == null) {
        parent.getChildren().add(new ExpressionTree(TruthValue.YES_NO_NULL));
      } else {
        PredicateLeafImpl leaf =
            new PredicateLeafImpl(PredicateLeaf.Operator.EQUALS,
                type, column, literal, null, conf);
        parent.getChildren().add(new ExpressionTree(addLeaf(leaf)));
      }
      return this;
    }

    @Override
    public Builder nullSafeEquals(String column, PredicateLeaf.Type type,
                                  Object literal) {
      ExpressionTree parent = currentTree.getFirst();
      if (column == null || literal == null) {
        parent.getChildren().add(new ExpressionTree(TruthValue.YES_NO_NULL));
      } else {
        PredicateLeafImpl leaf =
            new PredicateLeafImpl(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                type, column, literal, null, conf);
        parent.getChildren().add(new ExpressionTree(addLeaf(leaf)));
      }
      return this;
    }

    @Override
    public Builder in(String column, PredicateLeaf.Type type,
                      Object... literal) {
      ExpressionTree parent = currentTree.getFirst();
      if (column  == null || literal == null) {
        parent.getChildren().add(new ExpressionTree(TruthValue.YES_NO_NULL));
      } else {
        if (literal.length == 0) {
          throw new IllegalArgumentException("Can't create in expression with "
              + "no arguments");
        }
        List<Object> argList = new ArrayList<>(Arrays.asList(literal));

        PredicateLeafImpl leaf =
            new PredicateLeafImpl(PredicateLeaf.Operator.IN,
                type, column, null, argList, conf);
        parent.getChildren().add(new ExpressionTree(addLeaf(leaf)));
      }
      return this;
    }

    @Override
    public Builder isNull(String column, PredicateLeaf.Type type) {
      ExpressionTree parent = currentTree.getFirst();
      if (column == null) {
        parent.getChildren().add(new ExpressionTree(TruthValue.YES_NO_NULL));
      } else {
        PredicateLeafImpl leaf =
            new PredicateLeafImpl(PredicateLeaf.Operator.IS_NULL,
                type, column, null, null, conf);
        parent.getChildren().add(new ExpressionTree(addLeaf(leaf)));
      }
      return this;
    }

    @Override
    public Builder between(String column, PredicateLeaf.Type type, Object lower,
                           Object upper) {
      ExpressionTree parent = currentTree.getFirst();
      if (column == null || lower == null || upper == null) {
        parent.getChildren().add(new ExpressionTree(TruthValue.YES_NO_NULL));
      } else {
        List<Object> argList = new ArrayList<>();
        argList.add(lower);
        argList.add(upper);
        PredicateLeafImpl leaf =
            new PredicateLeafImpl(PredicateLeaf.Operator.BETWEEN,
                type, column, null, argList, conf);
        parent.getChildren().add(new ExpressionTree(addLeaf(leaf)));
      }
      return this;
    }

    @Override
    public Builder literal(TruthValue truth) {
      ExpressionTree parent = currentTree.getFirst();
      parent.getChildren().add(new ExpressionTree(truth));
      return this;
    }

    /**
     * Recursively explore the tree to find the leaves that are still reachable
     * after optimizations.
     * @param tree the node to check next
     * @param leaves the list of leaves that were found
     */
    static void compactLeaves(ExpressionTree tree,
                              List<PredicateLeaf> leaves) {
      if (tree.getOperator() == ExpressionTree.Operator.LEAF) {
        PredicateLeafImpl newLeaf = (PredicateLeafImpl) tree.getPredicateLeaf();
        if (newLeaf.getId() == -1) {
          newLeaf.setId(leaves.size());
          leaves.add(newLeaf);
        }
      } else if (tree.getChildren() != null){
        for(ExpressionTree child: tree.getChildren()) {
          compactLeaves(child, leaves);
        }
      }
    }

    @Override
    public SearchArgument build() {
      if (currentTree.size() != 1) {
        throw new IllegalArgumentException("Failed to end " +
            currentTree.size() + " operations.");
      }
      ExpressionTree optimized = optimize(root);
      ExpressionTree expanded = convertToCNF(new ExpressionTree(optimized));
      expanded = flatten(expanded);
      List<PredicateLeaf> finalLeaves = new ArrayList<>(leaves.size());
      compactLeaves(expanded, finalLeaves);
      return new SearchArgumentImpl(expanded, optimized, finalLeaves);
    }

    static ExpressionTree optimize(ExpressionTree root) {
      ExpressionTree optimized = pushDownNot(root);
      optimized = foldMaybe(optimized);
      return flatten(optimized);
    }

    /**
     * Push the negations all the way to just before the leaves. Also remove
     * double negatives.
     * @param root the expression to normalize
     * @return the normalized expression, which may share some or all of the
     * nodes of the original expression.
     */
    static ExpressionTree pushDownNot(ExpressionTree root) {
      if (root.getOperator() == ExpressionTree.Operator.NOT) {
        ExpressionTree child = root.getChildren().get(0);
        switch (child.getOperator()) {
          case NOT:
            return pushDownNot(child.getChildren().get(0));
          case CONSTANT:
            return  new ExpressionTree(child.getConstant().not());
          case AND:
            root = new ExpressionTree(ExpressionTree.Operator.OR);
            for(ExpressionTree kid: child.getChildren()) {
              root.getChildren().add(pushDownNot(new
                  ExpressionTree(ExpressionTree.Operator.NOT, kid)));
            }
            break;
          case OR:
            root = new ExpressionTree(ExpressionTree.Operator.AND);
            for(ExpressionTree kid: child.getChildren()) {
              root.getChildren().add(pushDownNot(new ExpressionTree
                  (ExpressionTree.Operator.NOT, kid)));
            }
            break;
          // for leaf, we don't do anything
          default:
            break;
        }
      } else if (root.getChildren() != null) {
        // iterate through children and push down not for each one
        for(int i=0; i < root.getChildren().size(); ++i) {
          root.getChildren().set(i, pushDownNot(root.getChildren().get(i)));
        }
      }
      return root;
    }

    /**
     * Remove MAYBE values from the expression. If they are in an AND operator,
     * they are dropped. If they are in an OR operator, they kill their parent.
     * This assumes that pushDownNot has already been called.
     * @param expr The expression to clean up
     * @return The cleaned up expression
     */
    static ExpressionTree foldMaybe(ExpressionTree expr) {
      if (expr.getChildren() != null) {
        for(int i=0; i < expr.getChildren().size(); ++i) {
          ExpressionTree child = foldMaybe(expr.getChildren().get(i));
          if (child.getConstant() == TruthValue.YES_NO_NULL) {
            switch (expr.getOperator()) {
              case AND:
                expr.getChildren().remove(i);
                i -= 1;
                break;
              case OR:
                // a maybe will kill the or condition
                return child;
              default:
                throw new IllegalStateException("Got a maybe as child of " +
                    expr);
            }
          } else {
            expr.getChildren().set(i, child);
          }
        }
        if (expr.getChildren().isEmpty()) {
          return new ExpressionTree(TruthValue.YES_NO_NULL);
        }
      }
      return expr;
    }

    /**
     * Converts multi-level ands and ors into single level ones.
     * @param root the expression to flatten
     * @return the flattened expression, which will always be root with
     *   potentially modified children.
     */
    static ExpressionTree flatten(ExpressionTree root) {
      if (root.getChildren() != null) {
        // iterate through the index, so that if we add more children,
        // they don't get re-visited
        for(int i=0; i < root.getChildren().size(); ++i) {
          ExpressionTree child = flatten(root.getChildren().get(i));
          // do we need to flatten?
          if (child.getOperator() == root.getOperator() &&
              child.getOperator() != ExpressionTree.Operator.NOT) {
            boolean first = true;
            for(ExpressionTree grandkid: child.getChildren()) {
              // for the first grandkid replace the original parent
              if (first) {
                first = false;
                root.getChildren().set(i, grandkid);
              } else {
                root.getChildren().add(++i, grandkid);
              }
            }
          } else {
            root.getChildren().set(i, child);
          }
        }
        // if we have a singleton AND or OR, just return the child
        if ((root.getOperator() == ExpressionTree.Operator.OR ||
            root.getOperator() == ExpressionTree.Operator.AND) &&
            root.getChildren().size() == 1) {
          return root.getChildren().get(0);
        }
      }
      return root;
    }

    /**
     * Generate all combinations of items on the andList. For each item on the
     * andList, it generates all combinations of one child from each and
     * expression. Thus, (and a b) (and c d) will be expanded to: (or a c)
     * (or a d) (or b c) (or b d). If there are items on the nonAndList, they
     * are added to each or expression.
     * @param result a list to put the results onto
     * @param andList a list of and expressions
     * @param nonAndList a list of non-and expressions
     */
    private static void generateAllCombinations(List<ExpressionTree> result,
                                                List<ExpressionTree> andList,
                                                List<ExpressionTree> nonAndList
    ) {
      List<ExpressionTree> kids = andList.get(0).getChildren();
      if (result.isEmpty()) {
        for(ExpressionTree kid: kids) {
          ExpressionTree or = new ExpressionTree(ExpressionTree.Operator.OR);
          result.add(or);
          for(ExpressionTree node: nonAndList) {
            or.getChildren().add(new ExpressionTree(node));
          }
          or.getChildren().add(kid);
        }
      } else {
        List<ExpressionTree> work = new ArrayList<>(result);
        result.clear();
        for(ExpressionTree kid: kids) {
          for(ExpressionTree or: work) {
            ExpressionTree copy = new ExpressionTree(or);
            copy.getChildren().add(kid);
            result.add(copy);
          }
        }
      }
      if (andList.size() > 1) {
        generateAllCombinations(result, andList.subList(1, andList.size()),
            nonAndList);
      }
    }

    /**
     * Convert an expression so that the top level operator is AND with OR
     * operators under it. This routine assumes that all of the NOT operators
     * have been pushed to the leaves via pushdDownNot.
     * @param root the expression
     * @return the normalized expression
     */
    static ExpressionTree convertToCNF(ExpressionTree root) {
      if (root.getChildren() != null) {
        // convert all of the children to CNF
        int size = root.getChildren().size();
        for(int i=0; i < size; ++i) {
          root.getChildren().set(i, convertToCNF(root.getChildren().get(i)));
        }
        if (root.getOperator() == ExpressionTree.Operator.OR) {
          // a list of leaves that weren't under AND expressions
          List<ExpressionTree> nonAndList = new ArrayList<>();
          // a list of AND expressions that we need to distribute
          List<ExpressionTree> andList = new ArrayList<>();
          for(ExpressionTree child: root.getChildren()) {
            if (child.getOperator() == ExpressionTree.Operator.AND) {
              andList.add(child);
            } else if (child.getOperator() == ExpressionTree.Operator.OR) {
              // pull apart the kids of the OR expression
              nonAndList.addAll(child.getChildren());
            } else {
              nonAndList.add(child);
            }
          }
          if (!andList.isEmpty()) {
            if (checkCombinationsThreshold(andList)) {
              root = new ExpressionTree(ExpressionTree.Operator.AND);
              generateAllCombinations(root.getChildren(), andList, nonAndList);
            } else {
              root = new ExpressionTree(TruthValue.YES_NO_NULL);
            }
          }
        }
      }
      return root;
    }

    private static boolean checkCombinationsThreshold(List<ExpressionTree> andList) {
      int numComb = 1;
      for (ExpressionTree tree : andList) {
        numComb *= tree.getChildren().size();
        if (numComb > CNF_COMBINATIONS_THRESHOLD) {
          return false;
        }
      }
      return true;
    }

  }
}
