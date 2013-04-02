package org.apache.hadoop.hive.ql.cube.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.junit.Before;
import org.junit.Test;

public class TestCubeSemanticAnalyzer {
  private final Configuration conf = new Configuration();

  private final CubeSemanticAnalyzer analyzer;

  ASTNode astRoot;

  public TestCubeSemanticAnalyzer() throws Exception {
    analyzer = new CubeSemanticAnalyzer(
        new HiveConf(conf, HiveConf.class));
  }

  String queries[] = { "SELECT t1.c1 rsalias0, f(t1.c2) rsalias1," +
  		" (t2.c3 + t2.c4) rsalias2, avg(fc5/fc6) * fc7 " +
      " FROM facttab t1" +
      " WHERE ( fc1='foo' and fc2 = 250 or sin(fc3)=1.0 ) " +
      " and time_range_in('NOW-7DAYS', 'NOW')" +
      " GROUP BY t1.ca, t1.cb" +
      " HAVING t2.c3 > 100" +
      " ORDER BY t3.ca, t4.cb" +
      " LIMIT 100",
      "SELECT count(*) FROM TAB2 WHERE time_range_in('NOW - 1MONTH', 'NOW')"
  };

  @Before
  public void setup() throws Exception {
    CubeTestSetup setup = new CubeTestSetup();
    setup.createSources();
  }

  //@Test
  public void testSemnaticAnalyzer() throws Exception {
    astRoot = HQLParser.parseHQL(queries[0]);
    analyzer.analyzeInternal(astRoot);
  }

  @Test
  public void testSimpleQuery() throws Exception {
    astRoot = HQLParser.parseHQL("select SUM(msr2) from testCube where time_range_in('NOW - 2DAYS', 'NOW')");
    analyzer.analyzeInternal(astRoot);
    CubeQueryContext cubeql = analyzer.getQueryContext();
    //System.out.println("cube hql:" + cubeql.toHQL());
    //Assert.assertEquals(queries[1], cubeql.toHQL());
  }
}
