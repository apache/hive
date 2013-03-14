package org.apache.hadoop.hive.ql.cube.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.junit.Test;

public class TestCubeSemanticAnalyzer {
  Configuration conf = new Configuration();

  CubeSemanticAnalyzer analyzer;

  ASTNode astRoot;

  String queries[] = { "SELECT t1.c1 rsalias0, f(t1.c2) rsalias1," +
  		" (t2.c3 + t2.c4) rsalias2, avg(fc5/fc6) * fc7 " +
      " FROM facttab t1" +
      " WHERE ( fc1='foo' and fc2 = 250 or sin(fc3)=1.0 ) " +
      " and time_range_in('NOW-7DAYS', 'NOW')" +
      " GROUP BY t1.ca, t1.cb" +
      " HAVING t2.c3 > 100" +
      " ORDER BY t3.ca, t4.cb" +
      " LIMIT 100"
  };

  @Test
  public void testSemnaticAnalyzer() throws Exception {
    analyzer = new CubeSemanticAnalyzer(new HiveConf(conf, HiveConf.class));
    astRoot = HQLParser.parseHQL(queries[0]);
    analyzer.analyzeInternal(astRoot);
  }
}
