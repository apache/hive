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

package org.apache.hadoop.hive.ql;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.reflect.Method;
import java.lang.reflect.Constructor;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Utilities.StreamPrinter;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ThriftDeserializer;
import org.apache.hadoop.hive.serde2.thrift.test.Complex;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.MiniMRCluster;

import com.facebook.thrift.protocol.TBinaryProtocol;

public class QTestUtil {

  private String testWarehouse;
  private String tmpdir =  System.getProperty("user.dir")+"/../build/ql/tmp";
  private Path tmppath = new Path(tmpdir);
  private String testFiles;
  private String outDir;
  private String logDir;
  private TreeMap<String, String> qMap;
  private LinkedList<String> srcTables;

  private ParseDriver pd;
  private Hive db;
  private HiveConf conf;
  private Driver drv;
  private SemanticAnalyzer sem;
  private FileSystem fs;
  private boolean overWrite;
  private CliDriver cliDriver;
  private MiniMRCluster mr = null;
  private Object dfs = null;
  private boolean miniMr = false;
  private Class<?> dfsClass = null;
  
  public boolean deleteDirectory(File path) {
    if (path.exists()) {
      File[] files = path.listFiles();
      for(int i=0; i<files.length; i++) {
         if(files[i].isDirectory()) {
           deleteDirectory(files[i]);
         }
         else {
           files[i].delete();
         }
      }
    }
    return(path.delete());
  }
  
  public void copyDirectoryToLocal(Path src, Path dest) throws Exception {

    FileSystem srcFs = src.getFileSystem(conf);
    FileSystem destFs = dest.getFileSystem(conf);
    if (srcFs.exists(src)) {
      FileStatus[] files = srcFs.listStatus(src);
      for(int i=0; i<files.length; i++) {
        String name = files[i].getPath().getName();
        Path dfs_path = files[i].getPath();
        Path local_path = new Path(dest, name);

        // If this is a source table we do not copy it out
        if (srcTables.contains(name)) {
          continue;
        }

        if(files[i].isDir()) {
          if (!destFs.exists(local_path)) {
            destFs.mkdirs(local_path);
          }
          copyDirectoryToLocal(dfs_path, local_path);
        }
        else {
          srcFs.copyToLocalFile(dfs_path, local_path);
        }
      }
    }
  }
  
  static Pattern mapTok = Pattern.compile("(\\.?)(.*)_map_(.*)");
  static Pattern reduceTok = Pattern.compile("(.*)(reduce_[^\\.]*)((\\..*)?)");

  public void normalizeNames(File path) throws Exception {
    if(path.isDirectory()) {
      File[] files = path.listFiles();
      for(int i=0; i<files.length; i++) {
        normalizeNames(files[i]);
      }
    }
    else {
      // System.out.println("Trying to match: " + path.getPath());
      Matcher m = reduceTok.matcher(path.getName());
      if (m.matches()) {
        String name = m.group(1) + "reduce" + m.group(3);
        // System.out.println("Matched new name: " + name);
        path.renameTo(new File(path.getParent(), name));
      } else {
        m = mapTok.matcher(path.getName());
        if (m.matches()) {
          String name = m.group(1) + "map_" + m.group(3);
          // System.out.println("Matched new name: " + name);
          path.renameTo(new File(path.getParent(), name));
        }
      }
    }
  }

  public QTestUtil(String outDir, String logDir) throws Exception {
    this(outDir, logDir, false);
  }

  public QTestUtil(String outDir, String logDir, boolean miniMr) throws Exception {
    this.outDir = outDir;
    this.logDir = logDir;
    conf = new HiveConf(Driver.class);
    this.miniMr = miniMr;
    qMap = new TreeMap<String, String>();

    if (miniMr) {
      dfsClass = null;

      // The path for MiniDFSCluster has changed, so look in both 17 and 19
      // In hadoop 17, the path is org.apache.hadoop.dfs.MiniDFSCluster, whereas
      // it is org.apache.hadoop.hdfs.MiniDFSCluster in hadoop 19. Due to this anamonly,
      // use reflection to invoke the methods.
      try {
        dfsClass = Class.forName("org.apache.hadoop.dfs.MiniDFSCluster");
      } catch (ClassNotFoundException e) {
        dfsClass = null;
      }

      if (dfsClass == null) {
        dfsClass = Class.forName("org.apache.hadoop.hdfs.MiniDFSCluster");
      }

      Constructor<?> dfsCons = 
        dfsClass.getDeclaredConstructor(new Class<?>[] {Configuration.class, Integer.TYPE, 
                                            Boolean.TYPE, (new String[] {}).getClass()});

      dfs = dfsCons.newInstance(conf, 4, true, null);
      Method m = dfsClass.getDeclaredMethod("getFileSystem", new Class[]{});
      FileSystem fs = (FileSystem)m.invoke(dfs, new Object[] {});

      mr = new MiniMRCluster(4, fs.getUri().toString(), 1);
      
      // hive.metastore.warehouse.dir needs to be set relative to the jobtracker
      String fsName = conf.get("fs.default.name");
      assert fsName != null;
      conf.set("hive.metastore.warehouse.dir", fsName.concat("/build/ql/test/data/warehouse/"));
      
      conf.set("mapred.job.tracker", "localhost:" + mr.getJobTrackerPort());
    }    

    // System.out.println(conf.toString());
    testFiles = conf.get("test.data.files").replace('\\', '/').replace("c:", "");

    String ow = System.getProperty("test.output.overwrite");
    overWrite = false;
    if ((ow != null) && ow.equalsIgnoreCase("true")){
      overWrite = true;
    }

    srcTables = new LinkedList<String>();
    init();
  }
  
  public void shutdown() throws Exception {
    cleanUp();

    if (dfs != null) {
      Method m = dfsClass.getDeclaredMethod("shutdown", new Class[]{});
      m.invoke(dfs, new Object[]{});
      dfs = null;
      dfsClass = null;
    }
    
    if (mr != null) {
      mr.shutdown();
      mr = null;
    }
  }

  public void addFile(String qFile) throws Exception {

    File qf = new File(qFile);
    addFile(qf);
  }

  public void addFile(File qf) throws Exception {

    FileInputStream fis = new FileInputStream(qf);
    BufferedInputStream bis = new BufferedInputStream(fis);
    DataInputStream dis = new DataInputStream(bis);
    StringBuffer qsb = new StringBuffer();
      
    // Read the entire query
    while(dis.available() != 0) {
      qsb.append(dis.readLine() + "\n");
    }
    qMap.put(qf.getName(), qsb.toString());
  }

  public void cleanUp() throws Exception {
    String warehousePath = ((new URI(testWarehouse)).getPath());
    // Drop any tables that remain due to unsuccessful runs
    for(String s: new String [] {"src", "src1", "src_json", "src_thrift", "src_sequencefile", 
                                 "srcpart", "srcbucket", "dest1", "dest2", 
                                 "dest3", "dest4", "dest4_sequencefile",
                                 "dest_j1", "dest_j2", "dest_g1", "dest_g2"}) {
      db.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, s);
    }
    for(String s: new String [] {"dest4.out", "union.out"}) {
      deleteDirectory(new File(warehousePath, s));
    }
  }

  private void runLoadCmd(String loadCmd) throws Exception {
    int ecode = 0;
    ecode = drv.run(loadCmd);
    if(ecode != 0) {
       throw new Exception("load command: " + loadCmd + " failed with exit code= " + ecode);
    }

    return;
  }

  public void createSources() throws Exception {
    // Next create the three tables src, dest1 and dest2 each with two columns
    // key and value
    LinkedList<String> cols = new LinkedList<String>();
    cols.add("key");
    cols.add("value");
    
    LinkedList<String> part_cols = new LinkedList<String>();
    part_cols.add("ds");
    part_cols.add("hr");
    db.createTable("srcpart", cols, part_cols, TextInputFormat.class, IgnoreKeyTextOutputFormat.class);
    srcTables.add("srcpart");
    
    Path fpath;
    Path newfpath;
    HashMap<String, String> part_spec = new HashMap<String, String>();
    for (String ds: new String[]{"2008-04-08", "2008-04-09"}) {
      for (String hr: new String[]{"11", "12"}) {
        part_spec.clear();
        part_spec.put("ds", ds);
        part_spec.put("hr", hr);
        // System.out.println("Loading partition with spec: " + part_spec);
        //db.createPartition(srcpart, part_spec);
        fpath = new Path(testFiles, "kv1.txt");
        newfpath = new Path(tmppath, "kv1.txt");
        fs.copyFromLocalFile(false, true, fpath, newfpath);
        fpath = newfpath;
        //db.loadPartition(fpath, srcpart.getName(), part_spec, true);
        runLoadCmd("LOAD DATA INPATH '" +  newfpath.toString() +
                   "' OVERWRITE INTO TABLE srcpart PARTITION (ds='" + ds + "',hr='" + hr +"')");
      }
    }
    ArrayList<String> bucketCols = new ArrayList<String>();
    bucketCols.add("key");
    db.createTable("srcbucket", cols, null, TextInputFormat.class, IgnoreKeyTextOutputFormat.class, 2, bucketCols);
    srcTables.add("srcbucket");
    for (String fname: new String [] {"kv1.txt", "kv2.txt"}) {
      fpath = new Path(testFiles, fname);
      newfpath = new Path(tmppath, fname);
      fs.copyFromLocalFile(false, true, fpath, newfpath);
      runLoadCmd("LOAD DATA INPATH '" +  newfpath.toString() + "' INTO TABLE srcbucket");
    }
    
    for (String tname: new String [] {"src", "src1"}) {
      db.createTable(tname, cols, null, TextInputFormat.class, IgnoreKeyTextOutputFormat.class);
      srcTables.add(tname);
    }
    db.createTable("src_sequencefile", cols, null, SequenceFileInputFormat.class, SequenceFileOutputFormat.class);
    srcTables.add("src_sequencefile");
    
    Table srcThrift = new Table("src_thrift");
    srcThrift.setInputFormatClass(SequenceFileInputFormat.class.getName());
    srcThrift.setOutputFormatClass(SequenceFileOutputFormat.class.getName());
    srcThrift.setSerializationLib(ThriftDeserializer.class.getName());
    srcThrift.setSerdeParam(Constants.SERIALIZATION_CLASS, Complex.class.getName());
    srcThrift.setSerdeParam(Constants.SERIALIZATION_FORMAT, TBinaryProtocol.class.getName());
    db.createTable(srcThrift);
    srcTables.add("src_thrift");
    
    LinkedList<String> json_cols = new LinkedList<String>();
    json_cols.add("json");
    db.createTable("src_json", json_cols, null, TextInputFormat.class, IgnoreKeyTextOutputFormat.class);
    srcTables.add("src_json");

    // load the input data into the src table
    fpath = new Path(testFiles, "kv1.txt");
    newfpath = new Path(tmppath, "kv1.txt");
    fs.copyFromLocalFile(false, true, fpath, newfpath);
    //db.loadTable(newfpath, "src", false);
    runLoadCmd("LOAD DATA INPATH '" +  newfpath.toString() + "' INTO TABLE src");
    
    // load the input data into the src table
    fpath = new Path(testFiles, "kv3.txt");
    newfpath = new Path(tmppath, "kv3.txt");
    fs.copyFromLocalFile(false, true, fpath, newfpath);
    //db.loadTable(newfpath, "src1", false);
    runLoadCmd("LOAD DATA INPATH '" +  newfpath.toString() + "' INTO TABLE src1");
    
    // load the input data into the src_sequencefile table
    fpath = new Path(testFiles, "kv1.seq");
    newfpath = new Path(tmppath, "kv1.seq");
    fs.copyFromLocalFile(false, true, fpath, newfpath);
    //db.loadTable(newfpath, "src_sequencefile", true);
    runLoadCmd("LOAD DATA INPATH '" +  newfpath.toString() + "' INTO TABLE src_sequencefile");
    
    // load the input data into the src_thrift table
    fpath = new Path(testFiles, "complex.seq");
    newfpath = new Path(tmppath, "complex.seq");
    fs.copyFromLocalFile(false, true, fpath, newfpath);
    //db.loadTable(newfpath, "src_thrift", true);    
    runLoadCmd("LOAD DATA INPATH '" +  newfpath.toString() + "' INTO TABLE src_thrift");
    
    // load the json data into the src_json table
    fpath = new Path(testFiles, "json.txt");
    newfpath = new Path(tmppath, "json.txt");
    fs.copyFromLocalFile(false, true, fpath, newfpath);
    //db.loadTable(newfpath, "src_json", false);
    runLoadCmd("LOAD DATA INPATH '" +  newfpath.toString() + "' INTO TABLE src_json");
 
  }

  public void init() throws Exception {
    // System.out.println(conf.toString());
    testWarehouse = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
    // conf.logVars(System.out);
    // System.out.flush();
    
    db = Hive.get(conf);
    fs = FileSystem.get(conf);
    drv = new Driver(conf);
    pd = new ParseDriver();
    sem = new SemanticAnalyzer(conf);
  }

  public void init(String tname) throws Exception {
    cleanUp();
    createSources();

    LinkedList<String> cols = new LinkedList<String>();
    cols.add("key");
    cols.add("value");
   
    LinkedList<String> part_cols = new LinkedList<String>();
    part_cols.add("ds");
    part_cols.add("hr");

    db.createTable("dest1", cols, null, TextInputFormat.class, IgnoreKeyTextOutputFormat.class);
    db.createTable("dest2", cols, null, TextInputFormat.class, IgnoreKeyTextOutputFormat.class);
   
    db.createTable("dest3", cols, part_cols, TextInputFormat.class, IgnoreKeyTextOutputFormat.class);
    Table dest3 = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, "dest3");

    HashMap<String, String> part_spec = new HashMap<String, String>();
    part_spec.put("ds", "2008-04-08");
    part_spec.put("hr", "12");
    db.createPartition(dest3, part_spec);
   
    db.createTable("dest4", cols, null, TextInputFormat.class, IgnoreKeyTextOutputFormat.class);
    db.createTable("dest4_sequencefile", cols, null, SequenceFileInputFormat.class, SequenceFileOutputFormat.class);
  }

  public void cliInit(String tname) throws Exception {
    cliInit(tname, true);
  }

  public void cliInit(String tname, boolean recreate) throws Exception {
    if (miniMr || recreate) {
      cleanUp();
      createSources();
    }
    
    CliSessionState ss = new CliSessionState(conf);
    assert ss!= null;
    ss.in = System.in;

    File qf = new File(outDir, tname);
    File outf = null;
    outf = new File(logDir);
    outf = new File(outf, qf.getName().concat(".out"));
    FileOutputStream fo = new FileOutputStream(outf);
    ss.out = new PrintStream(fo, true, "UTF-8");
    ss.err = ss.out;
    ss.setIsSilent(true);
    SessionState.start(ss);
    cliDriver = new CliDriver();
  }

  public int executeOne(String tname) {
    String q = qMap.get(tname);

    if(q.indexOf(";") == -1)
      return -1;

    String q1 = q.substring(0, q.indexOf(";") + 1);
    String qrest = q.substring(q.indexOf(";")+1);
    qMap.put(tname, qrest);

    System.out.println("Executing " + q1);
    return cliDriver.processLine(q1);
  }

  public int execute(String tname) {
    return drv.run(qMap.get(tname));
  }

  public int executeClient(String tname) {
    return cliDriver.processLine(qMap.get(tname));
  }

  public void convertSequenceFileToTextFile() throws Exception {
    // Create an instance of hive in order to create the tables
    testWarehouse = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
    db = Hive.get(conf);
    // Create dest4 to replace dest4_sequencefile
    LinkedList<String> cols = new LinkedList<String>();
    cols.add("key");
    cols.add("value");
    
    // Move all data from dest4_sequencefile to dest4 
    drv.run("FROM dest4_sequencefile INSERT OVERWRITE TABLE dest4 SELECT dest4_sequencefile.*");
    
    // Drop dest4_sequencefile
    db.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, "dest4_sequencefile", true, true);
  }

  public int checkNegativeResults(String tname, Exception e) throws Exception {

    File qf = new File(outDir, tname);
    File expf = new File(outDir);
    expf = new File(expf, qf.getName().concat(".out"));
    
    File outf = null;
    outf = new File(logDir);
    outf = new File(outf, qf.getName().concat(".out"));

    FileWriter outfd = new FileWriter(outf);
    if (e instanceof ParseException) {
      outfd.write("Parse Error: ");
    }
    else if (e instanceof SemanticException) {
      outfd.write("Semantic Exception: \n");
    }
    else {
      throw e;
    }
    
    outfd.write(e.getMessage());
    outfd.close();
    
    String cmdLine = "diff " + outf.getPath() + " " + expf.getPath();
    System.out.println(cmdLine);
    
    Process executor = Runtime.getRuntime().exec(cmdLine);
    
    StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out);
    StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, System.err);
    
    outPrinter.start();
    errPrinter.start();
    
    int exitVal = executor.waitFor();

    if(exitVal != 0 && overWrite) {
      System.out.println("Overwriting results");
      cmdLine = "cp " + outf.getPath() + " " + expf.getPath();
      executor = Runtime.getRuntime().exec(cmdLine);
      exitVal = executor.waitFor();
    }

    return exitVal;
  }

  public int checkParseResults(String tname, ASTNode tree) throws Exception {

    if (tree != null) {
      File parseDir = new File(outDir, "parse");
      File expf = new File(parseDir, tname.concat(".out"));
  
      File outf = null;
      outf = new File(logDir);
      outf = new File(outf, tname.concat(".out"));

      FileWriter outfd = new FileWriter(outf);
      outfd.write(tree.toStringTree());
      outfd.close();
  
      String cmdLine = "diff " + outf.getPath() + " " + expf.getPath();
      System.out.println(cmdLine);
      
      Process executor = Runtime.getRuntime().exec(cmdLine);
      
      StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out);
      StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, System.err);
      
      outPrinter.start();
      errPrinter.start();
      
      int exitVal = executor.waitFor();

      if(exitVal != 0 && overWrite) {
        System.out.println("Overwriting results");
        cmdLine = "cp " + outf.getPath() + " " + expf.getPath();
        executor = Runtime.getRuntime().exec(cmdLine);
        exitVal = executor.waitFor();
      }

      return exitVal;
    }
    else {
      throw new Exception("Parse tree is null");
    }
  }

  public int checkPlan(String tname,  List<Task<? extends Serializable>> tasks) throws Exception {

    if (tasks != null) {
      File planDir = new File(outDir, "plan");
      File planFile = new File(planDir, tname.concat(".xml"));
  
      File outf = null;
      outf = new File(logDir);
      outf = new File(outf, tname.concat(".xml"));

      FileOutputStream ofs = new FileOutputStream(outf);
      for(Task<? extends Serializable> plan: tasks) {
        Utilities.serializeTasks(plan, ofs);
      }
  
      String [] cmdArray = new String[6];
      cmdArray[0] = "diff";
      cmdArray[1] = "-b";
      cmdArray[2] = "-I";
      cmdArray[3] = "\\(\\(<java version=\".*\" class=\"java.beans.XMLDecoder\">\\)" +
        "\\|\\(<string>.*/tmp/.*</string>\\)" +
        "\\|\\(<string>file:.*</string>\\)" +
        "\\|\\(<string>/.*/warehouse/.*</string>\\)\\)";
      cmdArray[4] = outf.getPath();
      cmdArray[5] = planFile.getPath();
      System.out.println(cmdArray[0] + " " + cmdArray[1] + " " + cmdArray[2] + "\'" + cmdArray[3] + "\'" +
                         " " + cmdArray[4] + " " + cmdArray[5]);

      Process executor = Runtime.getRuntime().exec(cmdArray);

      StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out);
      StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, System.err);
    
      outPrinter.start();
      errPrinter.start();
      
      int exitVal = executor.waitFor();
      
      if(exitVal != 0 && overWrite) {
        System.out.println("Overwriting results");
        String cmdLine = "cp " + outf.getPath() + " " + planFile.getPath();
        executor = Runtime.getRuntime().exec(cmdLine);
        exitVal = executor.waitFor();
      }

      return exitVal;
    }
    else {
      throw new Exception("Plan is null");
    }

  }
    
  public int checkResults(String tname) throws Exception {
    Path warehousePath = new Path(FileSystem.get(conf).getUri().getPath());
    warehousePath = new Path(warehousePath, (new URI(testWarehouse)).getPath());

    Path localPath = new Path(FileSystem.getLocal(conf).getUri().getPath());
    localPath = new Path(localPath, logDir);
    localPath = new Path(localPath, "warehouse_local_copy");
    System.out.println("warehousePath = " + warehousePath.toString() + " localPath = " + localPath.toString());

    if (FileSystem.getLocal(conf).exists(localPath)) {
      FileSystem.getLocal(conf).delete(localPath, true);
    }

    copyDirectoryToLocal(warehousePath, localPath);
    normalizeNames(new File(localPath.toUri().getPath()));

    String [] cmdArray;
    if (overWrite == false) {
      cmdArray = new String[6];
      cmdArray[0] = "diff";
      cmdArray[1] = "-r";
      cmdArray[2] = "--exclude=tmp";
      cmdArray[3] = "--exclude=.svn";
      cmdArray[4] = localPath.toUri().getPath();
      cmdArray[5] = (new File(outDir, tname)).getPath() + "/warehouse";
      System.out.println(cmdArray[0] + " " + cmdArray[1] + " " + cmdArray[2] + " " + 
                         cmdArray[3] + " " + cmdArray[4] + " " + cmdArray[5]);
    }
    else {
      System.out.println("overwritting");
      // Remove any existing output
      String [] cmdArray1 = new String[5];
      cmdArray1[0] = "rm";
      cmdArray1[1] = "-rf";
      cmdArray1[2] = (new File(outDir, tname)).getPath();
      System.out.println(cmdArray1[0] + " " + cmdArray1[1] + " " + cmdArray1[2]);

      Process executor = Runtime.getRuntime().exec(cmdArray1);
      
      StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out);
      StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, System.err);
      
      outPrinter.start();
      errPrinter.start();
      int exitVal = executor.waitFor();
      if (exitVal != 0) {
        return exitVal;
      }

      // Capture code
      cmdArray = new String[5];
      cmdArray[0] = "cp";
      cmdArray[1] = "-r";
      cmdArray[2] = localPath.toUri().getPath();
      cmdArray[3] = (new File(outDir, tname)).getPath();
      System.out.println(cmdArray[0] + " " + cmdArray[1] + " " + cmdArray[2] + " " + cmdArray[3]);
    }

    Process executor = Runtime.getRuntime().exec(cmdArray);

    StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out);
    StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, System.err);
    
    outPrinter.start();
    errPrinter.start();
    
    int exitVal = executor.waitFor();

    return exitVal;
  }

  public int checkCliDriverResults(String tname) throws Exception {
    String [] cmdArray;

    cmdArray = new String[6];
    cmdArray[0] = "diff";
    cmdArray[1] = "-a";
    cmdArray[2] = "-I";
    cmdArray[3] = "\\(file:\\)\\|\\(/tmp/.*\\)";
    cmdArray[4] = (new File(logDir, tname + ".out")).getPath();
    cmdArray[5] = (new File(outDir, tname + ".out")).getPath();
    System.out.println(cmdArray[0] + " " + cmdArray[1] + " " + cmdArray[2] + " " +
                       cmdArray[3] + " " + cmdArray[4] + " " + cmdArray[5]);

    Process executor = Runtime.getRuntime().exec(cmdArray);

    StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out);
    StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, System.err);
    
    outPrinter.start();
    errPrinter.start();
    
    int exitVal = executor.waitFor();

    if(exitVal != 0 && overWrite) {
      System.out.println("Overwriting results");
      cmdArray = new String[3];
      cmdArray[0] = "cp";
      cmdArray[1] = (new File(logDir, tname + ".out")).getPath();
      cmdArray[2] = (new File(outDir, tname + ".out")).getPath();
      executor = Runtime.getRuntime().exec(cmdArray);
      exitVal = executor.waitFor();
    }

    return exitVal;
  }

  public ASTNode parseQuery(String tname) throws Exception {

    return pd.parse(qMap.get(tname));
  }

  public List<Task<? extends Serializable>> analyzeAST(ASTNode ast) throws Exception {

    // Do semantic analysis and plan generation
    Context ctx = new Context(conf);
    while((ast.getToken() == null) && (ast.getChildCount() > 0)) {
      ast = (ASTNode)ast.getChild(0);
    }
    
    sem.analyze(ast, ctx);
    ctx.clear();
    return sem.getRootTasks();
  }

  
  public TreeMap<String, String> getQMap() {
    return qMap;
  }


  /**
   * QTRunner: Runnable class for running a a single query file
   * 
   **/
  public static class QTRunner implements Runnable {
    private QTestUtil qt;
    private String fname;

    public QTRunner(QTestUtil qt, String fname) {
      this.qt = qt;
      this.fname = fname;
    }
    
    public void run() {
      try {
        // assumption is that environment has already been cleaned once globally
        // hence each thread does not call cleanUp() and createSources() again
        qt.cliInit(fname, false);
        qt.executeClient(fname);
      } catch (Throwable e) {
        System.err.println("Query file " + fname + " failed with exception " + e.getMessage());
        e.printStackTrace();
        System.err.flush();
      }
    }
  }

  /**
   * executes a set of query files either in sequence or in parallel.
   * Uses QTestUtil to do so
   *
   * @param qfiles array of input query files containing arbitrary number of hive queries
   * @param resDirs array of output directories one corresponding to each input query file
   * @param mt whether to run in multithreaded mode or not
   * @return true if all the query files were executed successfully, else false
   *
   * In multithreaded mode each query file is run in a separate thread. the caller has to 
   * arrange that different query files do not collide (in terms of destination tables)
   */
  public static boolean queryListRunner(File [] qfiles, String [] resDirs, String[] logDirs, boolean mt) {

    assert(qfiles.length == resDirs.length);
    assert(qfiles.length == logDirs.length);
    boolean failed = false;        

    try {
      QTestUtil[] qt = new QTestUtil [qfiles.length];
      for(int i=0; i<qfiles.length; i++) {
        qt[i] = new QTestUtil(resDirs[i], logDirs[i]);
        qt[i].addFile(qfiles[i]);
      }

      if (mt) {
        // in multithreaded mode - do cleanup/initialization just once

        qt[0].cleanUp();
        qt[0].createSources();

        QTRunner [] qtRunners = new QTestUtil.QTRunner [qfiles.length];
        Thread [] qtThread = new Thread [qfiles.length];

        for(int i=0; i<qfiles.length; i++) {
          qtRunners[i] = new QTestUtil.QTRunner (qt[i], qfiles[i].getName());
          qtThread[i] = new Thread (qtRunners[i]);
        }

        for(int i=0; i<qfiles.length; i++) {
          qtThread[i].start();
        }

        for(int i=0; i<qfiles.length; i++) {
          qtThread[i].join();
          int ecode = qt[i].checkCliDriverResults(qfiles[i].getName());
          if (ecode != 0) {
            failed = true;
            System.err.println("Test " + qfiles[i].getName() + " results check failed with error code " + ecode);
          }
        }

      } else {
        
        for(int i=0; i<qfiles.length && !failed; i++) {
          qt[i].cliInit(qfiles[i].getName());
          qt[i].executeClient(qfiles[i].getName());
          int ecode = qt[i].checkCliDriverResults(qfiles[i].getName());
          if (ecode != 0) {
            failed = true;
            System.err.println("Test " + qfiles[i].getName() + " results check failed with error code " + ecode);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    return (!failed);
  }
}

