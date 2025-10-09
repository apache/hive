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

package org.apache.hadoop.hive.ql.exec;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;



import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.ScriptDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.TextInputFormat;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 * Mimics the actual query compiler in generating end to end plans and testing
 * them out.
 *
 */
public class TestExecDriver {

  static QueryState queryState;
  static HiveConf conf;

  private static final String TMPDIR;
  private static final Logger LOG = LoggerFactory.getLogger(TestExecDriver.class);
  private static final Path tmppath;
  private static Hive db;
  private static FileSystem fs;
  private static CompilationOpContext ctx = null;

  static {
    try {
      queryState = new QueryState.Builder().withHiveConf(new HiveConf(ExecDriver.class)).build();
      conf = queryState.getConf();
      // this test is mr specific
      conf.set(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "mr");
      conf.setBoolVar(HiveConf.ConfVars.SUBMIT_VIA_CHILD, true);
      conf.setBoolVar(HiveConf.ConfVars.SUBMIT_LOCAL_TASK_VIA_CHILD, true);
      conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
          "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");

      SessionState.start(conf);

      TMPDIR = System.getProperty("test.tmp.dir");
      tmppath = new Path(TMPDIR);

      fs = FileSystem.get(conf);
      if (fs.exists(tmppath) &&
          !ShimLoader.getHadoopShims().isDirectory(fs.getFileStatus(tmppath))) {
        throw new RuntimeException(TMPDIR + " exists but is not a directory");
      }

      if (!fs.exists(tmppath)) {
        if (!fs.mkdirs(tmppath)) {
          throw new RuntimeException("Could not make scratch directory "
              + TMPDIR);
        }
      }
      LOG.info("Directory of actual files: " + tmppath);
      for (Object one : Utilities.makeList("mapplan1.out", "mapplan2.out",
          "mapredplan1.out", "mapredplan2.out", "mapredplan3.out",
          "mapredplan4.out", "mapredplan5.out", "mapredplan6.out")) {
        Path onedir = new Path(tmppath, (String) one);
        if (fs.exists(onedir)) {
          fs.delete(onedir, true);
        }
      }

      // copy the test files into hadoop if required.
      int i = 0;
      Path[] hadoopDataFile = new Path[2];
      String[] testFiles = {"kv1.txt", "kv2.txt"};
      String testFileDir = new Path(conf.get("test.data.files")).toUri().getPath();
      LOG.info("Directory of expected files: " + testFileDir);
      for (String oneFile : testFiles) {
        Path localDataFile = new Path(testFileDir, oneFile);
        hadoopDataFile[i] = new Path(tmppath, oneFile);
        fs.copyFromLocalFile(false, true, localDataFile, hadoopDataFile[i]);
        i++;
      }

      // load the test files into tables
      i = 0;
      db = Hive.get(conf);
      String[] srctables = {"src", "src2"};
      LinkedList<String> cols = new LinkedList<String>();
      cols.add("key");
      cols.add("value");
      for (String src : srctables) {
        db.dropTable(Warehouse.DEFAULT_DATABASE_NAME, src, true, true);
        db.createTable(src, cols, null, TextInputFormat.class,
            HiveIgnoreKeyTextOutputFormat.class);
        db.loadTable(hadoopDataFile[i], src, LoadFileType.KEEP_EXISTING,
           true, false, false, true, null, 0, false, false);
        i++;
      }

    } catch (Throwable e) {
      throw new RuntimeException("Encountered throwable", e);
    }
  }

  MapredWork mr;

  @Before
  public void setUp() {
    mr = PlanUtils.getMapRedWork();
    ctx = new CompilationOpContext();
  }

  public static void addMapWork(MapredWork mr, Table tbl, String alias, Operator<?> work) {
    mr.getMapWork().addMapWork(tbl.getDataLocation(), alias, work, new PartitionDesc(
        Utilities.getTableDesc(tbl), null));
  }

  private static void fileDiff(String datafile, String testdir) throws Exception {
    String testFileDir = conf.get("test.data.files");

    // inbuilt assumption that the testdir has only one output file.
    Path diTest = new Path(tmppath, testdir);
    if (!fs.exists(diTest)) {
      throw new RuntimeException(TMPDIR + File.separator + testdir + " does not exist");
    }
    if (!ShimLoader.getHadoopShims().isDirectory(fs.getFileStatus(diTest))) {
      throw new RuntimeException(TMPDIR + File.separator + testdir + " is not a directory");
    }
    FSDataInputStream fiTest = fs.open((fs.listStatus(diTest))[0].getPath());

    FileInputStream fiGold = new FileInputStream(new File(testFileDir, datafile));
    if (!Utilities.contentsEqual(fiGold, fiTest, false)) {
      LOG.error(diTest.toString() + " does not match " + datafile);
      assertEquals(false, true);
    }
  }

  private FilterDesc getTestFilterDesc(String column) throws Exception {
    ArrayList<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>();
    children1.add(new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo,
        column, "", false));
    ExprNodeDesc lhs = new ExprNodeGenericFuncDesc(
        TypeInfoFactory.doubleTypeInfo, FunctionRegistry.getFunctionInfo(
        serdeConstants.DOUBLE_TYPE_NAME).getGenericUDF(), children1);

    ArrayList<ExprNodeDesc> children2 = new ArrayList<ExprNodeDesc>();
    children2.add(new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, Long
        .valueOf(100)));
    ExprNodeDesc rhs = new ExprNodeGenericFuncDesc(
        TypeInfoFactory.doubleTypeInfo, FunctionRegistry.getFunctionInfo(
        serdeConstants.DOUBLE_TYPE_NAME).getGenericUDF(), children2);

    ArrayList<ExprNodeDesc> children3 = new ArrayList<ExprNodeDesc>();
    children3.add(lhs);
    children3.add(rhs);

    ExprNodeDesc desc = new ExprNodeGenericFuncDesc(
        TypeInfoFactory.booleanTypeInfo, FunctionRegistry.getFunctionInfo("<")
        .getGenericUDF(), children3);

    return new FilterDesc(desc, false);
  }

  @SuppressWarnings("unchecked")
  private void populateMapPlan1(Table src) throws Exception {

    Operator<FileSinkDesc> op2 = OperatorFactory.get(ctx, new FileSinkDesc(new Path(TMPDIR + File.separator
        + "mapplan1.out"), Utilities.defaultTd, true));
    Operator<FilterDesc> op1 = OperatorFactory.get(getTestFilterDesc("key"), op2);

    addMapWork(mr, src, "a", op1);
  }

  @SuppressWarnings("unchecked")
  private void populateMapPlan2(Table src) throws Exception {

    Operator<FileSinkDesc> op3 = OperatorFactory.get(ctx, new FileSinkDesc(new Path(TMPDIR + File.separator
        + "mapplan2.out"), Utilities.defaultTd, false));

    Operator<ScriptDesc> op2 = OperatorFactory.get(new ScriptDesc("cat",
        PlanUtils.getDefaultTableDesc("" + Utilities.tabCode, "key,value"),
        TextRecordWriter.class, PlanUtils.getDefaultTableDesc(""
        + Utilities.tabCode, "key,value"), TextRecordReader.class,
        TextRecordReader.class, PlanUtils.getDefaultTableDesc("" + Utilities.tabCode, "key")), op3);

    Operator<FilterDesc> op1 = OperatorFactory.get(getTestFilterDesc("key"),
        op2);

    addMapWork(mr, src, "a", op1);
  }

  @SuppressWarnings("unchecked")
  private void populateMapRedPlan1(Table src) throws SemanticException {

    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < 2; i++) {
      outputColumns.add("_col" + i);
    }
    // map-side work
    Operator<ReduceSinkDesc> op1 = OperatorFactory.get(ctx, PlanUtils
        .getReduceSinkDesc(Utilities.makeList(getStringColumn("key")),
        Utilities.makeList(getStringColumn("value")), outputColumns, true,
        -1, 1, -1, AcidUtils.Operation.NOT_ACID, NullOrdering.NULLS_LAST));

    addMapWork(mr, src, "a", op1);
    ReduceWork rWork = new ReduceWork();
    rWork.setNumReduceTasks(Integer.valueOf(1));
    rWork.setKeyDesc(op1.getConf().getKeySerializeInfo());
    rWork.getTagToValueDesc().add(op1.getConf().getValueSerializeInfo());
    mr.setReduceWork(rWork);

    // reduce side work
    Operator<FileSinkDesc> op3 = OperatorFactory.get(ctx, new FileSinkDesc(new Path(TMPDIR + File.separator
        + "mapredplan1.out"), Utilities.defaultTd, false));

    List<ExprNodeDesc> cols = new ArrayList<ExprNodeDesc>();
    cols.add(getStringColumn(Utilities.ReduceField.VALUE.toString()+"."+outputColumns.get(1)));
    List<String> colNames = new ArrayList<String>();
    colNames.add(HiveConf.getColumnInternalName(2));
    Operator<SelectDesc> op2 = OperatorFactory.get(new SelectDesc(cols, colNames), op3);

    rWork.setReducer(op2);
  }

  @SuppressWarnings("unchecked")
  private void populateMapRedPlan2(Table src) throws Exception {
    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < 2; i++) {
      outputColumns.add("_col" + i);
    }
    // map-side work
    Operator<ReduceSinkDesc> op1 = OperatorFactory.get(ctx, PlanUtils
        .getReduceSinkDesc(Utilities.makeList(getStringColumn("key")),
        Utilities
        .makeList(getStringColumn("key"), getStringColumn("value")),
        outputColumns, false, -1, 1, -1, AcidUtils.Operation.NOT_ACID, NullOrdering.NULLS_LAST));

    addMapWork(mr, src, "a", op1);
    ReduceWork rWork = new ReduceWork();
    rWork.setNumReduceTasks(Integer.valueOf(1));
    rWork.setKeyDesc(op1.getConf().getKeySerializeInfo());
    rWork.getTagToValueDesc().add(op1.getConf().getValueSerializeInfo());
    mr.setReduceWork(rWork);

    // reduce side work
    Operator<FileSinkDesc> op4 = OperatorFactory.get(ctx, new FileSinkDesc(new Path(TMPDIR + File.separator
        + "mapredplan2.out"), Utilities.defaultTd, false));

    Operator<FilterDesc> op3 = OperatorFactory.get(getTestFilterDesc("0"), op4);

    List<ExprNodeDesc> cols = new ArrayList<ExprNodeDesc>();
    cols.add(getStringColumn(Utilities.ReduceField.KEY + ".reducesinkkey" + 0));
    cols.add(getStringColumn(Utilities.ReduceField.VALUE.toString()+"."+outputColumns.get(1)));
    Operator<SelectDesc> op2 = OperatorFactory.get(new SelectDesc(cols, outputColumns), op3);

    rWork.setReducer(op2);
  }

  /**
   * test reduce with multiple tagged inputs.
   */
  @SuppressWarnings("unchecked")
  private void populateMapRedPlan3(Table src, Table src2) throws SemanticException {
    List<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < 2; i++) {
      outputColumns.add("_col" + i);
    }
    // map-side work
    Operator<ReduceSinkDesc> op1 = OperatorFactory.get(ctx, PlanUtils
        .getReduceSinkDesc(Utilities.makeList(getStringColumn("key")),
        Utilities.makeList(getStringColumn("value")), outputColumns, true,
        Byte.valueOf((byte) 0), 1, -1, AcidUtils.Operation.NOT_ACID, NullOrdering.NULLS_LAST));

    addMapWork(mr, src, "a", op1);

    Operator<ReduceSinkDesc> op2 = OperatorFactory.get(ctx, PlanUtils
        .getReduceSinkDesc(Utilities.makeList(getStringColumn("key")),
        Utilities.makeList(getStringColumn("key")), outputColumns, true,
        Byte.valueOf((byte) 1), Integer.MAX_VALUE, -1, AcidUtils.Operation.NOT_ACID, NullOrdering.NULLS_LAST));

    addMapWork(mr, src2, "b", op2);
    ReduceWork rWork = new ReduceWork();
    rWork.setNumReduceTasks(Integer.valueOf(1));
    rWork.setNeedsTagging(true);
    rWork.setKeyDesc(op1.getConf().getKeySerializeInfo());
    rWork.getTagToValueDesc().add(op1.getConf().getValueSerializeInfo());

    mr.setReduceWork(rWork);
    rWork.getTagToValueDesc().add(op2.getConf().getValueSerializeInfo());

    // reduce side work
    Operator<FileSinkDesc> op4 = OperatorFactory.get(ctx, new FileSinkDesc(new Path(TMPDIR + File.separator
        + "mapredplan3.out"), Utilities.defaultTd, false));

    Operator<SelectDesc> op5 = OperatorFactory.get(new SelectDesc(Utilities
        .makeList(new ExprNodeFieldDesc(TypeInfoFactory.stringTypeInfo,
        new ExprNodeColumnDesc(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo),
        Utilities.ReduceField.VALUE.toString(), "", false), "0", false)),
        Utilities.makeList(outputColumns.get(0))), op4);

    rWork.setReducer(op5);
  }

  @SuppressWarnings("unchecked")
  private void populateMapRedPlan4(Table src) throws SemanticException {

    // map-side work
    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < 2; i++) {
      outputColumns.add("_col" + i);
    }
    Operator<ReduceSinkDesc> op1 = OperatorFactory.get(ctx, PlanUtils
        .getReduceSinkDesc(Utilities.makeList(getStringColumn("tkey")),
        Utilities.makeList(getStringColumn("tkey"),
        getStringColumn("tvalue")), outputColumns, false, -1, 1, -1,
                AcidUtils.Operation.NOT_ACID, NullOrdering.NULLS_LAST));

    Operator<ScriptDesc> op0 = OperatorFactory.get(new ScriptDesc("cat",
        PlanUtils.getDefaultTableDesc("" + Utilities.tabCode, "key,value"),
        TextRecordWriter.class, PlanUtils.getDefaultTableDesc(""
        + Utilities.tabCode, "tkey,tvalue"), TextRecordReader.class,
        TextRecordReader.class, PlanUtils.getDefaultTableDesc("" + Utilities.tabCode, "key")), op1);

    Operator<SelectDesc> op4 = OperatorFactory.get(new SelectDesc(Utilities
        .makeList(getStringColumn("key"), getStringColumn("value")),
        outputColumns), op0);

    addMapWork(mr, src, "a", op4);
    ReduceWork rWork = new ReduceWork();
    rWork.setKeyDesc(op1.getConf().getKeySerializeInfo());
    rWork.getTagToValueDesc().add(op1.getConf().getValueSerializeInfo());
    rWork.setNumReduceTasks(Integer.valueOf(1));
    mr.setReduceWork(rWork);

    // reduce side work
    Operator<FileSinkDesc> op3 = OperatorFactory.get(ctx, new FileSinkDesc(new Path(TMPDIR + File.separator
        + "mapredplan4.out"), Utilities.defaultTd, false));
    List<ExprNodeDesc> cols = new ArrayList<ExprNodeDesc>();
    cols.add(getStringColumn(Utilities.ReduceField.KEY + ".reducesinkkey" + 0));
    cols.add(getStringColumn(Utilities.ReduceField.VALUE.toString()+"."+outputColumns.get(1)));
    Operator<SelectDesc> op2 = OperatorFactory.get(new SelectDesc(cols, outputColumns), op3);
    rWork.setReducer(op2);
  }

  public static ExprNodeColumnDesc getStringColumn(String columnName) {
    return new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, columnName,
        "", false);
  }

  @SuppressWarnings("unchecked")
  private void populateMapRedPlan5(Table src) throws SemanticException {

    // map-side work
    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < 2; i++) {
      outputColumns.add("_col" + i);
    }
    Operator<ReduceSinkDesc> op0 = OperatorFactory.get(ctx, PlanUtils
        .getReduceSinkDesc(Utilities.makeList(getStringColumn("0")), Utilities
        .makeList(getStringColumn("0"), getStringColumn("1")),
        outputColumns, false, -1, 1, -1, AcidUtils.Operation.NOT_ACID, NullOrdering.NULLS_LAST));

    Operator<SelectDesc> op4 = OperatorFactory.get(new SelectDesc(Utilities
        .makeList(getStringColumn("key"), getStringColumn("value")),
        outputColumns), op0);

    addMapWork(mr, src, "a", op4);
    ReduceWork rWork = new ReduceWork();
    mr.setReduceWork(rWork);
    rWork.setNumReduceTasks(Integer.valueOf(1));
    rWork.setKeyDesc(op0.getConf().getKeySerializeInfo());
    rWork.getTagToValueDesc().add(op0.getConf().getValueSerializeInfo());

    // reduce side work
    Operator<FileSinkDesc> op3 = OperatorFactory.get(ctx, new FileSinkDesc(new Path(TMPDIR + File.separator
        + "mapredplan5.out"), Utilities.defaultTd, false));

    List<ExprNodeDesc> cols = new ArrayList<ExprNodeDesc>();
    cols.add(getStringColumn(Utilities.ReduceField.KEY + ".reducesinkkey" + 0));
    cols.add(getStringColumn(Utilities.ReduceField.VALUE.toString()+"."+outputColumns.get(1)));
    Operator<SelectDesc> op2 = OperatorFactory.get(new SelectDesc(cols, outputColumns), op3);
    rWork.setReducer(op2);
  }

  @SuppressWarnings("unchecked")
  private void populateMapRedPlan6(Table src) throws Exception {

    // map-side work
    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < 2; i++) {
      outputColumns.add("_col" + i);
    }
    Operator<ReduceSinkDesc> op1 = OperatorFactory.get(ctx, PlanUtils
        .getReduceSinkDesc(Utilities.makeList(getStringColumn("tkey")),
        Utilities.makeList(getStringColumn("tkey"),
        getStringColumn("tvalue")), outputColumns, false, -1, 1, -1,
                AcidUtils.Operation.NOT_ACID, NullOrdering.NULLS_LAST));

    Operator<ScriptDesc> op0 = OperatorFactory.get(new ScriptDesc(
        "\'cat\'", PlanUtils.getDefaultTableDesc("" + Utilities.tabCode,
        "tkey,tvalue"), TextRecordWriter.class, PlanUtils
        .getDefaultTableDesc("" + Utilities.tabCode, "tkey,tvalue"),
        TextRecordReader.class,
        TextRecordReader.class, PlanUtils.getDefaultTableDesc("" + Utilities.tabCode, "key")), op1);

    Operator<SelectDesc> op4 = OperatorFactory.get(new SelectDesc(Utilities
        .makeList(getStringColumn("key"), getStringColumn("value")),
        outputColumns), op0);

    addMapWork(mr, src, "a", op4);
    ReduceWork rWork = new ReduceWork();
    mr.setReduceWork(rWork);
    rWork.setNumReduceTasks(Integer.valueOf(1));
    rWork.setKeyDesc(op1.getConf().getKeySerializeInfo());
    rWork.getTagToValueDesc().add(op1.getConf().getValueSerializeInfo());

    // reduce side work
    Operator<FileSinkDesc> op3 = OperatorFactory.get(ctx, new FileSinkDesc(new Path(TMPDIR + File.separator
        + "mapredplan6.out"), Utilities.defaultTd, false));

    Operator<FilterDesc> op2 = OperatorFactory.get(getTestFilterDesc("0"), op3);

    List<ExprNodeDesc> cols = new ArrayList<ExprNodeDesc>();
    cols.add(getStringColumn(Utilities.ReduceField.KEY + ".reducesinkkey" + 0));
    cols.add(getStringColumn(Utilities.ReduceField.VALUE.toString()+"."+outputColumns.get(1)));
    Operator<SelectDesc> op5 = OperatorFactory.get(new SelectDesc(cols, outputColumns), op2);

    rWork.setReducer(op5);
  }

  private void executePlan() throws Exception {
    String testName = new Exception().getStackTrace()[1].getMethodName();
    MapRedTask mrtask = new MapRedTask();
    TaskQueue taskQueue = new TaskQueue();
    mrtask.setWork(mr);
    mrtask.initialize(queryState, null, taskQueue, null);
    int exitVal = mrtask.execute();

    if (exitVal != 0) {
      LOG.error(testName + " execution failed with exit status: "
          + exitVal);
      assertEquals(true, false);
    }
    LOG.info(testName + " execution completed successfully");
  }

  @Test
  public void testMapPlan1() throws Exception {

    LOG.info("Beginning testMapPlan1");
    populateMapPlan1(db.getTable(Warehouse.DEFAULT_DATABASE_NAME, "src"));
    executePlan();
    fileDiff("lt100.txt.deflate", "mapplan1.out");
  }

  @Test
  public void testMapPlan2() throws Exception {

    LOG.info("Beginning testMapPlan2");
    populateMapPlan2(db.getTable(Warehouse.DEFAULT_DATABASE_NAME, "src"));
    executePlan();
    fileDiff("lt100.txt", "mapplan2.out");
  }

  @Test
  public void testMapRedPlan1() throws Exception {

    LOG.info("Beginning testMapRedPlan1");
    populateMapRedPlan1(db.getTable(Warehouse.DEFAULT_DATABASE_NAME,
        "src"));
    executePlan();
    fileDiff("kv1.val.sorted.txt", "mapredplan1.out");
  }

  @Test
  public void testMapRedPlan2() throws Exception {

    LOG.info("Beginning testMapPlan2");
    populateMapRedPlan2(db.getTable(Warehouse.DEFAULT_DATABASE_NAME,
        "src"));
    executePlan();
    fileDiff("lt100.sorted.txt", "mapredplan2.out");
  }

  @Test
  public void testMapRedPlan3() throws Exception {

    LOG.info("Beginning testMapPlan3");
    populateMapRedPlan3(db.getTable(Warehouse.DEFAULT_DATABASE_NAME,
        "src"), db.getTable(Warehouse.DEFAULT_DATABASE_NAME, "src2"));
    executePlan();
    fileDiff("kv1kv2.cogroup.txt", "mapredplan3.out");
  }

  @Test
  public void testMapRedPlan4() throws Exception {

    LOG.info("Beginning testMapPlan4");
    populateMapRedPlan4(db.getTable(Warehouse.DEFAULT_DATABASE_NAME,
        "src"));
    executePlan();
    fileDiff("kv1.string-sorted.txt", "mapredplan4.out");
  }

  @Test
  public void testMapRedPlan5() throws Exception {

    LOG.info("Beginning testMapPlan5");
    populateMapRedPlan5(db.getTable(Warehouse.DEFAULT_DATABASE_NAME,
        "src"));
    executePlan();
    fileDiff("kv1.string-sorted.txt", "mapredplan5.out");
  }

  @Test
  public void testMapRedPlan6() throws Exception {

    LOG.info("Beginning testMapPlan6");
    populateMapRedPlan6(db.getTable(Warehouse.DEFAULT_DATABASE_NAME,
        "src"));
    executePlan();
    fileDiff("lt100.sorted.txt", "mapredplan6.out");
  }
}
