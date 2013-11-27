package org.apache.hadoop.hive.ql.io.orc;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.google.common.collect.Lists;

public class TestOrcSplitElimination {

  public static class AllTypesRow {
    Long userid;
    Text string1;
    Double subtype;
    HiveDecimal decimal1;
    Timestamp ts;

    AllTypesRow(Long uid, String s1, Double d1, HiveDecimal decimal, Timestamp ts) {
      this.userid = uid;
      this.string1 = new Text(s1);
      this.subtype = d1;
      this.decimal1 = decimal;
      this.ts = ts;
    }
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  JobConf conf;
  FileSystem fs;
  Path testFilePath;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception {
    conf = new JobConf();
    // all columns
    conf.set("columns", "userid,string1,subtype,decimal1,ts");
    conf.set("columns.types", "bigint,string,double,decimal,timestamp");
    // needed columns
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "userid,subtype");
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testSplitEliminationSmallMaxSplit() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory
          .getReflectionObjectInspector(AllTypesRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        100000, CompressionKind.NONE, 10000, 10000);
    writeData(writer);
    writer.close();
    conf.set("mapred.min.split.size", "1000");
    conf.set("mapred.max.split.size", "5000");
    InputFormat<?, ?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());

    GenericUDF udf = new GenericUDFOPEqualOrLessThan();
    List<ExprNodeDesc> childExpr = Lists.newArrayList();
    ExprNodeColumnDesc col = new ExprNodeColumnDesc(Long.class, "userid", "T", false);
    ExprNodeConstantDesc con = new ExprNodeConstantDesc(100);
    childExpr.add(col);
    childExpr.add(con);
    ExprNodeGenericFuncDesc en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    String sargStr = Utilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(5, splits.length);

    con = new ExprNodeConstantDesc(1);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = Utilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    assertEquals(0, splits.length);

    con = new ExprNodeConstantDesc(2);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = Utilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    assertEquals(1, splits.length);

    con = new ExprNodeConstantDesc(5);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = Utilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    assertEquals(2, splits.length);

    con = new ExprNodeConstantDesc(13);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = Utilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    assertEquals(3, splits.length);

    con = new ExprNodeConstantDesc(29);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = Utilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    assertEquals(4, splits.length);

    con = new ExprNodeConstantDesc(70);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = Utilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    assertEquals(5, splits.length);
  }

  @Test
  public void testSplitEliminationLargeMaxSplit() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory
          .getReflectionObjectInspector(AllTypesRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        100000, CompressionKind.NONE, 10000, 10000);
    writeData(writer);
    writer.close();
    conf.set("mapred.min.split.size", "1000");
    conf.set("mapred.max.split.size", "150000");
    InputFormat<?, ?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());

    GenericUDF udf = new GenericUDFOPEqualOrLessThan();
    List<ExprNodeDesc> childExpr = Lists.newArrayList();
    ExprNodeColumnDesc col = new ExprNodeColumnDesc(Long.class, "userid", "T", false);
    ExprNodeConstantDesc con = new ExprNodeConstantDesc(100);
    childExpr.add(col);
    childExpr.add(con);
    ExprNodeGenericFuncDesc en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    String sargStr = Utilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(2, splits.length);

    con = new ExprNodeConstantDesc(0);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = Utilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // no stripes satisfies the condition
    assertEquals(0, splits.length);

    con = new ExprNodeConstantDesc(2);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = Utilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // only first stripe will satisfy condition and hence single split
    assertEquals(1, splits.length);

    con = new ExprNodeConstantDesc(5);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = Utilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // first stripe will satisfy the predicate and will be a single split, last stripe will be a
    // separate split
    assertEquals(2, splits.length);

    con = new ExprNodeConstantDesc(13);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = Utilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // first 2 stripes will satisfy the predicate and merged to single split, last stripe will be a
    // separate split
    assertEquals(2, splits.length);

    con = new ExprNodeConstantDesc(29);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = Utilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // first 3 stripes will satisfy the predicate and merged to single split, last stripe will be a
    // separate split
    assertEquals(2, splits.length);

    con = new ExprNodeConstantDesc(70);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = Utilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // first 2 stripes will satisfy the predicate and merged to single split, last two stripe will
    // be a separate split
    assertEquals(2, splits.length);
  }


  @Test
  public void testSplitEliminationComplexExpr() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory
          .getReflectionObjectInspector(AllTypesRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        100000, CompressionKind.NONE, 10000, 10000);
    writeData(writer);
    writer.close();
    conf.set("mapred.min.split.size", "1000");
    conf.set("mapred.max.split.size", "150000");
    InputFormat<?, ?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());

    // predicate expression: userid <= 100 and subtype <= 1000.0
    GenericUDF udf = new GenericUDFOPEqualOrLessThan();
    List<ExprNodeDesc> childExpr = Lists.newArrayList();
    ExprNodeColumnDesc col = new ExprNodeColumnDesc(Long.class, "userid", "T", false);
    ExprNodeConstantDesc con = new ExprNodeConstantDesc(100);
    childExpr.add(col);
    childExpr.add(con);
    ExprNodeGenericFuncDesc en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

    GenericUDF udf1 = new GenericUDFOPEqualOrLessThan();
    List<ExprNodeDesc> childExpr1 = Lists.newArrayList();
    ExprNodeColumnDesc col1 = new ExprNodeColumnDesc(Double.class, "subtype", "T", false);
    ExprNodeConstantDesc con1 = new ExprNodeConstantDesc(1000.0);
    childExpr1.add(col1);
    childExpr1.add(con1);
    ExprNodeGenericFuncDesc en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

    GenericUDF udf2 = new GenericUDFOPAnd();
    List<ExprNodeDesc> childExpr2 = Lists.newArrayList();
    childExpr2.add(en);
    childExpr2.add(en1);
    ExprNodeGenericFuncDesc en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

    String sargStr = Utilities.serializeExpression(en2);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(2, splits.length);

    con = new ExprNodeConstantDesc(2);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

    con1 = new ExprNodeConstantDesc(0.0);
    childExpr1.set(1, con1);
    en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

    childExpr2.set(0, en);
    childExpr2.set(1, en1);
    en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

    sargStr = Utilities.serializeExpression(en2);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // no stripe will satisfy the predicate
    assertEquals(0, splits.length);

    con = new ExprNodeConstantDesc(2);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

    con1 = new ExprNodeConstantDesc(1.0);
    childExpr1.set(1, con1);
    en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

    childExpr2.set(0, en);
    childExpr2.set(1, en1);
    en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

    sargStr = Utilities.serializeExpression(en2);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // only first stripe will satisfy condition and hence single split
    assertEquals(1, splits.length);

    udf = new GenericUDFOPEqual();
    con = new ExprNodeConstantDesc(13);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

    con1 = new ExprNodeConstantDesc(80.0);
    childExpr1.set(1, con1);
    en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

    childExpr2.set(0, en);
    childExpr2.set(1, en1);
    en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

    sargStr = Utilities.serializeExpression(en2);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // first two stripes will satisfy condition and hence single split
    assertEquals(2, splits.length);

    udf = new GenericUDFOPEqual();
    con = new ExprNodeConstantDesc(13);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

    udf1 = new GenericUDFOPEqual();
    con1 = new ExprNodeConstantDesc(80.0);
    childExpr1.set(1, con1);
    en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

    childExpr2.set(0, en);
    childExpr2.set(1, en1);
    en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

    sargStr = Utilities.serializeExpression(en2);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // only second stripes will satisfy condition and hence single split
    assertEquals(1, splits.length);
  }

  private void writeData(Writer writer) throws IOException {
    for (int i = 0; i < 25000; i++) {
      if (i == 0) {
        writer.addRow(new AllTypesRow(2L, "foo", 0.8, HiveDecimal.create("1.2"), new Timestamp(0)));
      } else if (i == 5000) {
        writer.addRow(new AllTypesRow(13L, "bar", 80.0, HiveDecimal.create("2.2"), new Timestamp(
            5000)));
      } else if (i == 10000) {
        writer.addRow(new AllTypesRow(29L, "cat", 8.0, HiveDecimal.create("3.3"), new Timestamp(
            10000)));
      } else if (i == 15000) {
        writer.addRow(new AllTypesRow(70L, "dog", 1.8, HiveDecimal.create("4.4"), new Timestamp(
            15000)));
      } else if (i == 20000) {
        writer.addRow(new AllTypesRow(5L, "eat", 0.8, HiveDecimal.create("5.5"), new Timestamp(
            20000)));
      } else {
        writer.addRow(new AllTypesRow(100L, "zebra", 8.0, HiveDecimal.create("0.0"), new Timestamp(
            250000)));
      }
    }
  }
}
