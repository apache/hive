/**
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
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.Utilities.StreamPrinter;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFuncDesc;
import org.apache.hadoop.hive.ql.plan.extractDesc;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.plan.filterDesc;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.scriptDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.TextInputFormat;


/**
 * Mimics the actual query compiler in generating end to end plans and testing them out
 *
 */
public class TestExecDriver extends TestCase {

  static HiveConf conf;

  static private String tmpdir = "/tmp/"+System.getProperty("user.name")+"/";
  static private Path tmppath = new Path(tmpdir);
  static private Hive db;
  static private FileSystem fs;

  static {
    try {
      conf = new HiveConf(ExecDriver.class);

      fs = FileSystem.get(conf);
      if(fs.exists(tmppath) && !fs.getFileStatus(tmppath).isDir()) {
        throw new RuntimeException (tmpdir + " exists but is not a directory");
      }

      if(!fs.exists(tmppath)) {
        if(!fs.mkdirs(tmppath)) {
          throw new RuntimeException ("Could not make scratch directory " + tmpdir);
        }
      }

      for(Object one: Utilities.makeList("mapplan1.out", "mapplan2.out",
                                         "mapredplan1.out", "mapredplan2.out", "mapredplan3.out", "mapredplan4.out",
                                         "mapredplan5.out", "mapredplan6.out")) {
        Path onedir = new Path(tmppath, (String)one);
        if(fs.exists(onedir)) {
          fs.delete(onedir, true);
        }
      }

      // copy the test files into hadoop if required.
      int i = 0;
      Path [] hadoopDataFile = new Path [2];
      String [] testFiles = {"kv1.txt", "kv2.txt"};
      String testFileDir = "file://" + conf.get("test.data.files").replace('\\', '/').replace("c:", "");
      for(String oneFile: testFiles) {
        Path localDataFile = new Path(testFileDir, oneFile);
        hadoopDataFile[i] = new Path(tmppath, oneFile);
        fs.copyFromLocalFile(false, true, localDataFile, hadoopDataFile[i]);
        i++;
      }

      // load the test files into tables
      i = 0;
      db = Hive.get(conf);
      String [] srctables = {"src", "src2"};
      LinkedList<String> cols = new LinkedList<String>();
      cols.add("key");
      cols.add("value");
      for(String src: srctables) {
        db.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, src, true, true);
        db.createTable(src, cols, null, TextInputFormat.class, IgnoreKeyTextOutputFormat.class);
        db.loadTable(hadoopDataFile[i], src, false, null);
        i++;
      }

    } catch (Throwable e) {
      e.printStackTrace();
      throw new RuntimeException ("Encountered throwable");
    }
  }


  mapredWork mr;

  protected void setUp() {
    mr =  PlanUtils.getMapRedWork();
  }

  private static void fileDiff(String datafile, String testdir) throws Exception {
    String testFileDir = conf.get("test.data.files");
    System.out.println(testFileDir);
    FileInputStream fi_gold = new FileInputStream(new File(testFileDir,
                                                           datafile));

    // inbuilt assumption that the testdir has only one output file.
    Path di_test = new Path (tmppath, testdir);
    if(!fs.exists(di_test)) {
      throw new RuntimeException (tmpdir + testdir + " does not exist");
    }
    if(!fs.getFileStatus(di_test).isDir()) {
      throw new RuntimeException (tmpdir + testdir + " is not a directory");
    }

    FSDataInputStream fi_test = fs.open( (fs.listStatus(di_test))[0].getPath() );

    if(!Utilities.contentsEqual(fi_gold, fi_test, false)) {
      System.out.println(di_test.toString() + " does not match " + datafile);
      assertEquals(false, true);
    }
  }


  private filterDesc getTestFilterDesc(String column) {
    ArrayList<exprNodeDesc> children1 = new ArrayList<exprNodeDesc>();
    children1.add(new exprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, column));
    exprNodeDesc lhs = new exprNodeFuncDesc(
        Constants.DOUBLE_TYPE_NAME,
        TypeInfoFactory.doubleTypeInfo,
        FunctionRegistry.getUDFClass(Constants.DOUBLE_TYPE_NAME),
        FunctionRegistry.getUDFMethod(Constants.DOUBLE_TYPE_NAME, TypeInfoFactory.stringTypeInfo),
        children1);
    
    ArrayList<exprNodeDesc> children2 = new ArrayList<exprNodeDesc>();
    children2.add(new exprNodeConstantDesc(TypeInfoFactory.longTypeInfo, Long.valueOf(100)));
    exprNodeDesc rhs = new exprNodeFuncDesc(
        Constants.DOUBLE_TYPE_NAME,
        TypeInfoFactory.doubleTypeInfo,
        FunctionRegistry.getUDFClass(Constants.DOUBLE_TYPE_NAME),
        FunctionRegistry.getUDFMethod(Constants.DOUBLE_TYPE_NAME, TypeInfoFactory.longTypeInfo),
        children2);
    
    ArrayList<exprNodeDesc> children3 = new ArrayList<exprNodeDesc>();
    children3.add(lhs);
    children3.add(rhs);
    
    exprNodeDesc desc = new exprNodeFuncDesc(
        "<",
        TypeInfoFactory.booleanTypeInfo,
        FunctionRegistry.getUDFClass("<"),
        FunctionRegistry.getUDFMethod("<", TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.doubleTypeInfo),
        children3);
    
    return new filterDesc(desc);
  }

  @SuppressWarnings("unchecked")
  private void populateMapPlan1(Table src) {
    mr.setNumReduceTasks(Integer.valueOf(0));

    Operator<fileSinkDesc> op2 = OperatorFactory.get(new fileSinkDesc
                                                     (tmpdir + "mapplan1.out",
                                                      Utilities.defaultTd, true));
    Operator<filterDesc> op1 =
      OperatorFactory.get(getTestFilterDesc("key"), op2);


    Utilities.addMapWork(mr, src, "a", op1);
  }

  @SuppressWarnings("unchecked")
  private void populateMapPlan2(Table src) {
    mr.setNumReduceTasks(Integer.valueOf(0));

    Operator<fileSinkDesc> op3 = OperatorFactory.get(new fileSinkDesc
                                                     (tmpdir + "mapplan2.out",
                                                      Utilities.defaultTd, false));

    Operator<scriptDesc> op2 = OperatorFactory.get
      (new scriptDesc("/bin/cat",
          PlanUtils.getDefaultTableDesc("" + Utilities.tabCode, "key,value"),
          PlanUtils.getDefaultTableDesc("" + Utilities.tabCode, "key,value")),
       op3);


    Operator<filterDesc> op1 =
      OperatorFactory.get(getTestFilterDesc("key"), op2);


    Utilities.addMapWork(mr, src, "a", op1);
  }

  @SuppressWarnings("unchecked")
  private void populateMapRedPlan1(Table src) {
    mr.setNumReduceTasks(Integer.valueOf(1));
    
    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < 2; i++)
      outputColumns.add("_col" + i);
    // map-side work
    Operator<reduceSinkDesc> op1 = OperatorFactory.get
      (PlanUtils.getReduceSinkDesc
       (Utilities.makeList(getStringColumn("key")),
        Utilities.makeList(getStringColumn("value")), outputColumns, true, -1, 1, -1));

    Utilities.addMapWork(mr, src, "a", op1);
    mr.setKeyDesc(op1.getConf().getKeySerializeInfo());
    mr.getTagToValueDesc().add(op1.getConf().getValueSerializeInfo());

    // reduce side work
    Operator<fileSinkDesc> op3 = OperatorFactory.get(new fileSinkDesc
                                                     (tmpdir + "mapredplan1.out",
                                                      Utilities.defaultTd, false));

    Operator<extractDesc> op2 =  OperatorFactory.get
      (new extractDesc(getStringColumn(Utilities.ReduceField.VALUE.toString())), op3);

    mr.setReducer(op2);
  }

  @SuppressWarnings("unchecked")
  private void populateMapRedPlan2(Table src) {
    mr.setNumReduceTasks(Integer.valueOf(1));
    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < 2; i++)
      outputColumns.add("_col" + i);
    // map-side work
    Operator<reduceSinkDesc> op1 = OperatorFactory.get
      (PlanUtils.getReduceSinkDesc
       (Utilities.makeList(getStringColumn("key")),
        Utilities.makeList(getStringColumn("key"),
                           getStringColumn("value")), outputColumns, false, -1, 1, -1));

    Utilities.addMapWork(mr, src, "a", op1);
    mr.setKeyDesc(op1.getConf().getKeySerializeInfo());
    mr.getTagToValueDesc().add(op1.getConf().getValueSerializeInfo());

    // reduce side work
    Operator<fileSinkDesc> op4 = OperatorFactory.get(new fileSinkDesc
                                                     (tmpdir + "mapredplan2.out",
                                                      Utilities.defaultTd, false));

    Operator<filterDesc> op3 =
      OperatorFactory.get(getTestFilterDesc("0"), op4);

    Operator<extractDesc> op2 =  OperatorFactory.get
      (new extractDesc(getStringColumn(Utilities.ReduceField.VALUE.toString())), op3);

    mr.setReducer(op2);
  }

  /**
   * test reduce with multiple tagged inputs
   */
  @SuppressWarnings("unchecked")
    private void populateMapRedPlan3(Table src, Table src2) {
    mr.setNumReduceTasks(Integer.valueOf(5));
    mr.setNeedsTagging(true);
    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < 2; i++)
      outputColumns.add("_col" + i);
    // map-side work
    Operator<reduceSinkDesc> op1 = OperatorFactory.get
      (PlanUtils.getReduceSinkDesc
       (Utilities.makeList(getStringColumn("key")),
        Utilities.makeList
        (getStringColumn("value")), outputColumns, true, Byte.valueOf((byte)0), 1, -1));

    Utilities.addMapWork(mr, src, "a", op1);
    mr.setKeyDesc(op1.getConf().getKeySerializeInfo());
    mr.getTagToValueDesc().add(op1.getConf().getValueSerializeInfo());

    Operator<reduceSinkDesc> op2 = OperatorFactory.get
      (PlanUtils.getReduceSinkDesc
       (Utilities.makeList(getStringColumn("key")),
        Utilities.makeList(getStringColumn("key")),
        outputColumns, true,
        Byte.valueOf((byte)1),
        Integer.MAX_VALUE, -1));

    Utilities.addMapWork(mr, src2, "b", op2);
    mr.getTagToValueDesc().add(op2.getConf().getValueSerializeInfo());

    // reduce side work
    Operator<fileSinkDesc> op4 = OperatorFactory.get(new fileSinkDesc
                                                     (tmpdir + "mapredplan3.out",
                                                      Utilities.defaultTd, false));

    Operator<selectDesc> op5 =  OperatorFactory.get
      (new selectDesc
       (Utilities.makeList
        (getStringColumn(Utilities.ReduceField.ALIAS.toString()),
         new exprNodeFieldDesc(TypeInfoFactory.stringTypeInfo,
             new exprNodeColumnDesc(TypeInfoFactory.getListTypeInfo(
                 TypeInfoFactory.stringTypeInfo),
                 Utilities.ReduceField.VALUE.toString()),
             "0",
             false)), outputColumns), op4);

    mr.setReducer(op5);
  }

  @SuppressWarnings("unchecked")
  private void populateMapRedPlan4(Table src) {
    mr.setNumReduceTasks(Integer.valueOf(1));

    // map-side work
    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < 2; i++)
      outputColumns.add("_col" + i);
    Operator<reduceSinkDesc> op1 = OperatorFactory.get
      (PlanUtils.getReduceSinkDesc
       (Utilities.makeList(getStringColumn("tkey")),
        Utilities.makeList(getStringColumn("tkey"),
                           getStringColumn("tvalue")),
                           outputColumns, false,
        -1, 1, -1));

    Operator<scriptDesc> op0 = OperatorFactory.get
    (new scriptDesc("/bin/cat",
        PlanUtils.getDefaultTableDesc("" + Utilities.tabCode, "tkey,tvalue"),
        PlanUtils.getDefaultTableDesc("" + Utilities.tabCode, "key,value")),
     op1);

    Operator<selectDesc> op4 = OperatorFactory.get(new selectDesc(
                                     Utilities.makeList(getStringColumn("key"),
                                                        getStringColumn("value")), outputColumns), op0);

    Utilities.addMapWork(mr, src, "a", op4);
    mr.setKeyDesc(op1.getConf().getKeySerializeInfo());
    mr.getTagToValueDesc().add(op1.getConf().getValueSerializeInfo());

    // reduce side work
    Operator<fileSinkDesc> op3 = OperatorFactory.get(new fileSinkDesc
                                                     (tmpdir + "mapredplan4.out",
                                                      Utilities.defaultTd, false));

    Operator<extractDesc> op2 =  OperatorFactory.get
      (new extractDesc(getStringColumn(Utilities.ReduceField.VALUE.toString())), op3);

    mr.setReducer(op2);
  }

  public static exprNodeColumnDesc getStringColumn(String columnName) {
    return new exprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, columnName);
  }
  
  @SuppressWarnings("unchecked")
  private void populateMapRedPlan5(Table src) {
    mr.setNumReduceTasks(Integer.valueOf(1));

    // map-side work
    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < 2; i++)
      outputColumns.add("_col" + i);
    Operator<reduceSinkDesc> op0 = OperatorFactory.get
      (PlanUtils.getReduceSinkDesc
       (Utilities.makeList(getStringColumn("0")),
        Utilities.makeList(getStringColumn("0"),
                           getStringColumn("1")),
                           outputColumns, false,
        -1, 1, -1));

    Operator<selectDesc> op4 = OperatorFactory.get(new selectDesc(
                                     Utilities.makeList(getStringColumn("key"),
                                                        getStringColumn("value")), outputColumns), op0);

    Utilities.addMapWork(mr, src, "a", op4);
    mr.setKeyDesc(op0.getConf().getKeySerializeInfo());
    mr.getTagToValueDesc().add(op0.getConf().getValueSerializeInfo());

    // reduce side work
    Operator<fileSinkDesc> op3 = OperatorFactory.get(new fileSinkDesc
                                                     (tmpdir + "mapredplan5.out",
                                                      Utilities.defaultTd, false));

    Operator<extractDesc> op2 =  OperatorFactory.get
      (new extractDesc(getStringColumn(Utilities.ReduceField.VALUE.toString())), op3);

    mr.setReducer(op2);
  }

  @SuppressWarnings("unchecked")
  private void populateMapRedPlan6(Table src) {
    mr.setNumReduceTasks(Integer.valueOf(1));

    // map-side work
    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < 2; i++)
      outputColumns.add("_col" + i);
    Operator<reduceSinkDesc> op1 = OperatorFactory.get
      (PlanUtils.getReduceSinkDesc(
        Utilities.makeList(getStringColumn("tkey")),
        Utilities.makeList(getStringColumn("tkey"),
                           getStringColumn("tvalue")),
                           outputColumns, false,
        -1, 1, -1));

    Operator<scriptDesc> op0 = OperatorFactory.get
      (new scriptDesc("\'/bin/cat\'",
          PlanUtils.getDefaultTableDesc("" + Utilities.tabCode, "tkey,tvalue"),
          PlanUtils.getDefaultTableDesc("" + Utilities.tabCode, "tkey,tvalue")),
       op1);

    Operator<selectDesc> op4 = OperatorFactory.get(new selectDesc(
                                     Utilities.makeList(getStringColumn("key"),
                                                        getStringColumn("value")), outputColumns), op0);

    Utilities.addMapWork(mr, src, "a", op4);
    mr.setKeyDesc(op1.getConf().getKeySerializeInfo());
    mr.getTagToValueDesc().add(op1.getConf().getValueSerializeInfo());

    // reduce side work
    Operator<fileSinkDesc> op3 = OperatorFactory.get(new fileSinkDesc
                                                     (tmpdir + "mapredplan6.out",
                                                      Utilities.defaultTd, false));

    Operator<filterDesc> op2 =
      OperatorFactory.get(getTestFilterDesc("0"), op3);

    Operator<extractDesc> op5 =  OperatorFactory.get
      (new extractDesc(getStringColumn(Utilities.ReduceField.VALUE.toString())), op2);

    mr.setReducer(op5);
  }

  private File generatePlanFile() throws Exception {
    File planFile = File.createTempFile("plan", ".xml");
    System.out.println("Generating plan file " + planFile.toString());
    FileOutputStream out = new FileOutputStream(planFile);
    Utilities.serializeMapRedWork(mr, out);
    return planFile;
  }

  private void executePlan(File planFile) throws Exception {
    String testName = new Exception().getStackTrace()[1].getMethodName();
    String cmdLine = conf.getVar(HiveConf.ConfVars.HADOOPBIN) + " jar " + conf.getJar() +
      " org.apache.hadoop.hive.ql.exec.ExecDriver -plan " +
      planFile.toString() + " " + ExecDriver.generateCmdLine(conf);
    System.out.println("Executing: " + cmdLine);
    Process executor = Runtime.getRuntime().exec(cmdLine);

    StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out);
    StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, System.err);

    outPrinter.start();
    errPrinter.start();

    int exitVal = executor.waitFor();

    if(exitVal != 0) {
      System.out.println(testName + " execution failed with exit status: " + exitVal);
      assertEquals(true, false);
    }
    System.out.println(testName + " execution completed successfully");
  }

  public void testMapPlan1() throws Exception {
    
    System.out.println("Beginning testMapPlan1");
    
    try {
      populateMapPlan1(db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, "src"));
      File planFile = generatePlanFile();
      executePlan(planFile);
      fileDiff("lt100.txt.deflate", "mapplan1.out");
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Got Throwable");
    }
  }

  public void testMapPlan2() throws Exception {

    System.out.println("Beginning testMapPlan2");

    try {
      populateMapPlan2(db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, "src"));
      File planFile = generatePlanFile();
      executePlan(planFile);
      fileDiff("lt100.txt", "mapplan2.out");
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Got Throwable");
    }
  }

  public void testMapRedPlan1() throws Exception {

    System.out.println("Beginning testMapRedPlan1");

    try {
      populateMapRedPlan1(db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, "src"));
      File planFile = generatePlanFile();
      executePlan(planFile);
      fileDiff("kv1.val.sorted.txt", "mapredplan1.out");
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Got Throwable");
    }
  }

  public void testMapRedPlan2() throws Exception {

    System.out.println("Beginning testMapPlan2");

    try {
      populateMapRedPlan2(db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, "src"));
      File planFile = generatePlanFile();
      executePlan(planFile);
      fileDiff("lt100.sorted.txt", "mapredplan2.out");
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Got Throwable");
    }
  }

  public void testMapRedPlan3() throws Exception {

    System.out.println("Beginning testMapPlan3");

    try {
      populateMapRedPlan3(db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, "src"),
          db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, "src2"));
      File planFile = generatePlanFile();
      executePlan(planFile);
      fileDiff("kv1kv2.cogroup.txt", "mapredplan3.out");
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Got Throwable");
    }
  }


  public void testMapRedPlan4() throws Exception {

    System.out.println("Beginning testMapPlan4");

    try {
      populateMapRedPlan4(db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, "src"));
      File planFile = generatePlanFile();
      executePlan(planFile);
      fileDiff("kv1.string-sorted.txt", "mapredplan4.out");
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Got Throwable");
    }
  }

  public void testMapRedPlan5() throws Exception {

    System.out.println("Beginning testMapPlan5");

    try {
      populateMapRedPlan5(db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, "src"));
      File planFile = generatePlanFile();
      executePlan(planFile);
      fileDiff("kv1.string-sorted.txt", "mapredplan5.out");
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Got Throwable");
    }
  }

  public void testMapRedPlan6() throws Exception {

    System.out.println("Beginning testMapPlan6");

    try {
      populateMapRedPlan6(db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, "src"));
      File planFile = generatePlanFile();
      executePlan(planFile);
      fileDiff("lt100.sorted.txt", "mapredplan6.out");
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Got Throwable");
    }
  }
}
