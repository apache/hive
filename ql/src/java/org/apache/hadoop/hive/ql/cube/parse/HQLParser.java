package org.apache.hadoop.hive.ql.cube.parse;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;

public class HQLParser {

  public static interface ASTNodeVisitor {
    public void visit(TreeNode node);
  }

  public static class TreeNode {
    final TreeNode parent;
    final ASTNode node;
    public TreeNode(TreeNode parent, ASTNode node) {
      this.parent = parent;
      this.node = node;
    }

    public TreeNode getParent() {
      return parent;
    }

    public ASTNode getNode() {
      return node;
    }
  }

  public static final Set<Integer> BINARY_OPERATORS;
  public static final Set<Integer> ARITHMETIC_OPERATORS;


  static {
    HashSet<Integer> ops = new HashSet<Integer>();
    ops.add(DOT);
    ops.add(KW_AND);
    ops.add(KW_OR);
    ops.add(EQUAL);
    ops.add(NOTEQUAL);
    ops.add(GREATERTHAN);
    ops.add(GREATERTHANOREQUALTO);
    ops.add(LESSTHAN);
    ops.add(LESSTHANOREQUALTO);
    ops.add(PLUS);
    ops.add(MINUS);
    ops.add(STAR);
    ops.add(DIVIDE);
    ops.add(MOD);
    ops.add(KW_LIKE);
    BINARY_OPERATORS = Collections.unmodifiableSet(ops);


    ARITHMETIC_OPERATORS = new HashSet<Integer>();
    ARITHMETIC_OPERATORS.add(PLUS);
    ARITHMETIC_OPERATORS.add(MINUS);
    ARITHMETIC_OPERATORS.add(STAR);
    ARITHMETIC_OPERATORS.add(DIVIDE);
    ARITHMETIC_OPERATORS.add(MOD);

  }

  public static boolean isArithmeticOp(int tokenType) {
    return ARITHMETIC_OPERATORS.contains(tokenType);
  }

  public static ASTNode parseHQL(String query) throws ParseException {
    ParseDriver driver = new ParseDriver();
    ASTNode tree = driver.parse(query);
    tree = ParseUtils.findRootNonNullToken(tree);
    printAST(tree);
    return tree;
  }

  public static void printAST(ASTNode node) {
    try {
      printAST(getHiveTokenMapping(), node, 0, 0);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Debug function for printing query AST to stdout
   * @param node
   * @param level
   */
  public static void printAST(Map<Integer, String> tokenMapping, ASTNode node,
      int level, int child) {
    if (node == null || node.isNil()) {
      return;
    }

    for (int i = 0; i < level; i++) {
      System.out.print("  ");
    }

    System.out.print(node.getText() + " [" + tokenMapping.get(
        node.getToken().getType()) + "]");
    System.out.print(" (l"+level + "c" + child + ")");

    if (node.getChildCount() > 0) {
      System.out.println(" {");

      for (int i = 0; i < node.getChildCount(); i++) {
        Tree tree = node.getChild(i);
        if (tree instanceof ASTNode) {
          printAST(tokenMapping, (ASTNode) tree, level + 1, i+1);
        } else {
          System.out.println("NON ASTNode");
        }
        System.out.println();
      }

      for (int i = 0; i < level; i++) {
        System.out.print("  ");
      }

      System.out.print("}");

    } else {
      System.out.print('$');
    }
  }

  public static Map<Integer, String> getHiveTokenMapping() throws Exception {
    Map<Integer, String> mapping = new HashMap<Integer, String>();

    for (Field f : HiveParser.class.getFields()) {
      if (f.getType() == int.class) {
        Integer tokenId = f.getInt(null);
        String token = f.getName();
        mapping.put(tokenId, token);
      }
    }

    return mapping;
  }


  /**
   * Find a node in the tree rooted at root, given the path of type of tokens
   *  from the root's children to the desired node
   *
   * @param root
   * @param path starts at the level of root's children
   * @return
   */
  public static ASTNode findNodeByPath (ASTNode root, int... path) {
    for (int i = 0; i < path.length; i++) {
      int type = path[i];
      boolean hasChildWithType = false;

      for (int j = 0; j < root.getChildCount(); j++) {
        ASTNode node = (ASTNode) root.getChild(j);
        if (node.getToken().getType() == type) {
          hasChildWithType = true;
          root = node;
          // If this is the last type in path, return this node
          if (i == path.length - 1) {
            return root;
          } else {
            // Go to next level
            break;
          }
        } else {
          // Go to next sibling.
          continue;
        }
      }

      if (!hasChildWithType) {
        // No path from this level
        break;
      }
    }

    return null;
  }

  /**
   * Breadth first traversal of AST
   * @param root
   * @param visitor
   */
  public static void bft(ASTNode root, ASTNodeVisitor visitor) {
    if (root == null) {
      throw new NullPointerException("Root cannot be null");
    }

    if (visitor == null) {
      throw new NullPointerException("Visitor cannot be null");
    }
    Queue<TreeNode> queue = new LinkedList<TreeNode>();
    queue.add(new TreeNode(null, root));

    while (!queue.isEmpty()) {
      TreeNode node = queue.poll();
      visitor.visit(node);
      ASTNode astNode = node.getNode();
      for (int i = 0; i < astNode.getChildCount(); i++) {
        queue.offer(new TreeNode (node, (ASTNode)astNode.getChild(i)) );
      }
    }
  }

  /**
   * Recursively reconstruct query string given a query AST
   * @param root
   * @param buf preallocated builder where the reconstructed string will
   *  be written
   */
  public static void toInfixString(ASTNode root, StringBuilder buf) {
    if (root == null) {
      return;
    }
    int rootType = root.getToken().getType();
    // Operand, print contents
    if (Identifier == rootType|| Number == rootType ||
        StringLiteral == rootType) {
      buf.append(' ').append(root.getText()).append(' ');
    } else if (BINARY_OPERATORS.contains(
          Integer.valueOf(root.getToken().getType()))) {
        buf.append("(");
        toInfixString((ASTNode)root.getChild(0), buf);
        buf.append(' ').append(root.getText()).append(' ');
        toInfixString((ASTNode) root.getChild(1), buf);
        buf.append(")");
    } else if (TOK_FUNCTION == root.getToken().getType()) {
        String fname = ((ASTNode) root.getChild(0)).getText();
        buf.append(fname).append("(");
        for (int i = 1; i < root.getChildCount(); i++) {
          toInfixString((ASTNode) root.getChild(i), buf);
          if (i != root.getChildCount() -1) {
            buf.append(", ");
          }
        }
        buf.append(")");
    } else if (TOK_SELECT == rootType) {
      for (int i = 0; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf);
        if (i != root.getChildCount() -1) {
          buf.append(", ");
        }
      }
    } else {
      for (int i = 0; i < root.getChildCount(); i++) {
        toInfixString((ASTNode) root.getChild(i), buf);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    ASTNode ast = parseHQL("select * from default_table "
    		);

    printAST(getHiveTokenMapping(), ast, 0, 0);
  }

  public static String getString(ASTNode tree) {
    StringBuilder buf = new StringBuilder();
    toInfixString(tree, buf);
    return buf.toString();
  }
}