package org.apache.hadoop.hive.ql.io.sarg;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SearchArgumentFactoryTest {

  private final Configuration conf = new Configuration();

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void testNormalize() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startOr()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 6L)
      .in("f2", PredicateLeaf.Type.STRING, "a", "c")
      .end()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 3L, 4L)
      .in("f2", PredicateLeaf.Type.STRING, "c", "e")
      .end()
      .end()
      .build();

    Assert.assertEquals("(and "
                        + "(or leaf-(IN f1 1 6) leaf-(IN f1 3 4)) "
                        + "(or leaf-(IN f2 a c) leaf-(IN f1 3 4)) "
                        + "(or leaf-(IN f1 1 6) leaf-(IN f2 c e)) "
                        + "(or leaf-(IN f2 a c) leaf-(IN f2 c e)))",
                        sArg.getExpression().toString());

    Assert.assertEquals("(or "
                        + "(and leaf-(IN f1 1 6) leaf-(IN f2 a c)) "
                        + "(and leaf-(IN f1 3 4) leaf-(IN f2 c e)))",
                        sArg.getCompactExpression().toString());
  }

  @Test
  public void testNormalizeNested() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startAnd()
      .startOr()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 6L)
      .in("f2", PredicateLeaf.Type.STRING, "a", "c")
      .end()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 3L, 4L)
      .in("f2", PredicateLeaf.Type.STRING, "c", "e")
      .end()
      .end()
      .startOr()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 5L)
      .in("f2", PredicateLeaf.Type.STRING, "a", "d")
      .end()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 3L, 5L)
      .in("f2", PredicateLeaf.Type.STRING, "b", "e")
      .end()
      .end()
      .end()
      .build();

    Assert.assertEquals("(and "
                        + "(or leaf-(IN f1 1 6) leaf-(IN f1 3 4)) "
                        + "(or leaf-(IN f2 a c) leaf-(IN f1 3 4)) "
                        + "(or leaf-(IN f1 1 6) leaf-(IN f2 c e)) "
                        + "(or leaf-(IN f2 a c) leaf-(IN f2 c e)) "
                        + "(or leaf-(IN f1 1 5) leaf-(IN f1 3 5)) "
                        + "(or leaf-(IN f2 a d) leaf-(IN f1 3 5)) "
                        + "(or leaf-(IN f1 1 5) leaf-(IN f2 b e)) "
                        + "(or leaf-(IN f2 a d) leaf-(IN f2 b e)))",
                        sArg.getExpression().toString());
    Assert.assertEquals("(and "
                        + "(or "
                        + "(and leaf-(IN f1 1 6) leaf-(IN f2 a c)) "
                        + "(and leaf-(IN f1 3 4) leaf-(IN f2 c e))) "
                        + "(or "
                        + "(and leaf-(IN f1 1 5) leaf-(IN f2 a d)) "
                        + "(and leaf-(IN f1 3 5) leaf-(IN f2 b e))))",
                        sArg.getCompactExpression().toString());
  }

  @Test
  public void testNoNormalize() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder(conf)
      .startOr()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 6L)
      .in("f2", PredicateLeaf.Type.STRING, "a", "c")
      .end()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 3L, 4L)
      .in("f2", PredicateLeaf.Type.STRING, "c", "e")
      .end()
      .end()
      .build();

    ExpressionTree unexpanded = sArg.getCompactExpression();
    Assert.assertEquals(ExpressionTree.Operator.OR, unexpanded.getOperator());
    Assert.assertEquals(2, unexpanded.getChildren().size());
    for (ExpressionTree child : unexpanded.getChildren()) {
      Assert.assertEquals(ExpressionTree.Operator.AND, child.getOperator());
      Assert.assertEquals(2, child.getChildren().size());
      for (ExpressionTree gChild : child.getChildren()) {
        Assert.assertEquals(ExpressionTree.Operator.LEAF, gChild.getOperator());
      }
    }
  }

  @Test
  public void testNoNormalizeNormalize() {
    SearchArgument nnSArg = SearchArgumentFactory.newBuilder(conf)
      .startOr()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 6L)
      .in("f2", PredicateLeaf.Type.STRING, "a", "c")
      .end()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 3L, 4L)
      .in("f2", PredicateLeaf.Type.STRING, "c", "e")
      .end()
      .end()
      .build();

    Assert.assertEquals("(and (or leaf-(IN f1 1 6) leaf-(IN f1 3 4))" +
        " (or leaf-(IN f2 a c) leaf-(IN f1 3 4))" +
        " (or leaf-(IN f1 1 6) leaf-(IN f2 c e))" +
        " (or leaf-(IN f2 a c) leaf-(IN f2 c e)))",
        nnSArg.getExpression().toString());
    Assert.assertEquals("(or (and leaf-(IN f1 1 6) leaf-(IN f2 a c))" +
            " (and leaf-(IN f1 3 4) leaf-(IN f2 c e)))",
        nnSArg.getCompactExpression().toString());
  }

  @Test
  public void testNoNormalizeValidations() {
    conf.setBoolean("sarg.normalize", false);
    thrown.expectMessage("Failed to end");
    thrown.expect(IllegalArgumentException.class);
    SearchArgumentFactory.newBuilder(conf)
      .startOr()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 6L)
      .in("f2", PredicateLeaf.Type.STRING, "a", "c")
      .end()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 3L, 4L)
      .in("f2", PredicateLeaf.Type.STRING, "c", "e")
      .end()
      .build();
  }
}
