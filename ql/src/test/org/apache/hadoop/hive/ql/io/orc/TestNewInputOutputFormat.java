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
package org.apache.hadoop.hive.ql.io.orc;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde.OrcSerdeRow;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.common.util.HiveTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestNewInputOutputFormat {
  
  Path workDir = new Path(System.getProperty("test.tmp.dir",
    "target" + File.separator + "test" + File.separator + "tmp"));
  
  Configuration conf;
  FileSystem localFs;
  
  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    conf.set("mapred.job.tracker", "local");
    conf.set("fs.default.name", "local");
    localFs = FileSystem.get(conf);
  }
  
  @Rule
  public TestName testCaseName = new TestName();
  
  public static class OrcTestMapper1 extends
      Mapper<Object, Writable, Text, Text> {
    @Override
    public void map(Object key, Writable value, Context context)
        throws IOException, InterruptedException {
      context.write(null, new Text(value.toString()));
    }
  }

  @Test
  // Test regular inputformat
  public void testNewInputFormat() throws Exception {
    Job job = new Job(conf, "orc test");
    job.setInputFormatClass(OrcNewInputFormat.class);
    job.setJarByClass(TestNewInputOutputFormat.class);
    job.setMapperClass(OrcTestMapper1.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job,
        new Path(HiveTestUtils.getFileFromClasspath("orc-file-11-format.orc")));
    Path outputPath = new Path(workDir,
        "TestOrcFile." + testCaseName.getMethodName() + ".txt");
    localFs.delete(outputPath, true);
    FileOutputFormat.setOutputPath(job, outputPath);
    boolean result = job.waitForCompletion(true);
    assertTrue(result);
    Path outputFilePath = new Path(outputPath, "part-m-00000");

    assertTrue(localFs.exists(outputFilePath));
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(localFs.open(outputFilePath)));
    int count=0;
    String line;
    String lastLine=null;
    while ((line=reader.readLine()) != null) {
      count++;
      lastLine = line;
    }
    reader.close();
    assertEquals(count, 7500);
    assertEquals(lastLine, "{true, 100, 2048, 65536," +
        " 9223372036854775807, 2.0, -5.0" + 
        ", , bye, {[{1, bye}, {2, sigh}]}, [{100000000, cat}," +
        " {-100000, in}, {1234, hat}]," +
        " {chani={5, chani}, mauddib={1, mauddib}}," +
        " 2000-03-12 15:00:01, 12345678.6547457}");
    localFs.delete(outputPath, true);
  }
  
  public static class OrcTestMapper2 extends Mapper<Object, Text, Object, Writable> {
    private final TypeInfo typeInfo = TypeInfoUtils
        .getTypeInfoFromTypeString("struct<a:int,b:string>");
    private final ObjectInspector oip = TypeInfoUtils
        .getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    private final OrcSerde serde = new OrcSerde();
    private Writable row;
    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] items = value.toString().split(",");
      List<Object> struct = new ArrayList<Object>(2);
      struct.add(0, Integer.parseInt(items[0]));
      struct.add(1, items[1]);
      row = serde.serialize(struct, oip);
      context.write(null, row);
    }
  }
  
  @Test
  //Test regular outputformat
  public void testNewOutputFormat() throws Exception {
    int rownum=1000;
    
    Path inputPath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".txt");
    Path outputPath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    localFs.delete(outputPath, true);
    PrintWriter pw = new PrintWriter(
        new OutputStreamWriter(localFs.create(inputPath)));
    Random r = new Random(1000L);
    boolean firstRow = true;
    int firstIntValue = 0;
    String firstStringValue = null;
    for (int i=0;i<rownum;i++) {
      int intValue = r.nextInt();
      String stringValue = UUID.randomUUID().toString();
      if (firstRow) {
        firstRow = false;
        firstIntValue = intValue;
        firstStringValue = stringValue;
      }
      pw.println(intValue + "," + stringValue);
    }
    pw.close();

    Job job = new Job(conf, "orc test");
    job.setOutputFormatClass(OrcNewOutputFormat.class);
    job.setJarByClass(TestNewInputOutputFormat.class);
    job.setMapperClass(OrcTestMapper2.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Writable.class);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    boolean result = job.waitForCompletion(true);
    assertTrue(result);
    
    Path outputFilePath = new Path(outputPath, "part-m-00000");
    assertTrue(localFs.exists(outputFilePath));
    Reader reader = OrcFile.createReader(outputFilePath,
        OrcFile.readerOptions(conf).filesystem(localFs));
    assertTrue(reader.getNumberOfRows() == rownum);
    assertEquals(reader.getCompression(), CompressionKind.ZLIB);
    StructObjectInspector soi =
        (StructObjectInspector)reader.getObjectInspector();
    StructTypeInfo ti =
        (StructTypeInfo)TypeInfoUtils.getTypeInfoFromObjectInspector(soi);
    assertEquals(((PrimitiveTypeInfo)ti.getAllStructFieldTypeInfos().get(0))
        .getPrimitiveCategory(),
        PrimitiveObjectInspector.PrimitiveCategory.INT);
    assertEquals(((PrimitiveTypeInfo)ti.getAllStructFieldTypeInfos().get(1))
        .getPrimitiveCategory(),
        PrimitiveObjectInspector.PrimitiveCategory.STRING);
    
    RecordReader rows = reader.rows();
    Object row = rows.next(null);
    
    IntWritable intWritable = (IntWritable)soi.getStructFieldData(row,
        soi.getAllStructFieldRefs().get(0));
    Text text = (Text)soi.getStructFieldData(row,
        soi.getAllStructFieldRefs().get(1));
    
    assertEquals(intWritable.get(), firstIntValue);
    assertEquals(text.toString(), firstStringValue);
    
    localFs.delete(outputPath, true);
  }
  
  @Test
  //Test outputformat with compression
  public void testNewOutputFormatWithCompression() throws Exception {
    conf.set("hive.exec.orc.default.compress", "SNAPPY");
    
    Path inputPath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".txt");
    Path outputPath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    localFs.delete(outputPath, true);
    PrintWriter pw = new PrintWriter(
        new OutputStreamWriter(localFs.create(inputPath)));
    pw.println("1,hello");
    pw.println("2,world");
    pw.close();

    Job job = new Job(conf, "orc test");
    job.setOutputFormatClass(OrcNewOutputFormat.class);
    job.setJarByClass(TestNewInputOutputFormat.class);
    job.setMapperClass(OrcTestMapper2.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(OrcSerdeRow.class);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    boolean result = job.waitForCompletion(true);
    assertTrue(result);
    
    Path outputFilePath = new Path(outputPath, "part-m-00000");
    Reader reader = OrcFile.createReader(outputFilePath,
        OrcFile.readerOptions(conf).filesystem(localFs));
    assertEquals(reader.getCompression(), CompressionKind.SNAPPY);
    
    localFs.delete(outputPath, true);
  }
  
  public static class OrcTestMapper3 extends
      Mapper<Object, Text, IntWritable, Text> {
    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String items[] = value.toString().split("\\s+");
      context.write(new IntWritable(items.length), value);
    }
  }

  public static class OrcTestReducer3 extends
      Reducer<IntWritable, Text, NullWritable, Writable> {
    final static TypeInfo typeInfo =
        TypeInfoUtils.getTypeInfoFromTypeString(
        "struct<length:int,count:int,list:array" +
        "<struct<lastword:string,lastwordlength:int>>," +
        "wordcounts:map<string,int>>");
    private final ObjectInspector oip =
        TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    private final OrcSerde serde = new OrcSerde();
    private Writable row;

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      List<String> lastwords = new ArrayList<String>();
      Map<String, Integer> wordCounts = new HashMap<String, Integer>();
      int count = 0;
      for (Text val : values) {
        String[] items = val.toString().toLowerCase().split("\\s+");
        lastwords.add(items[items.length-1]);
        for (String item : items) {
          if (wordCounts.containsKey(item)) {
            wordCounts.put(item, wordCounts.get(item)+1);
          } else {
            wordCounts.put(item, 1);
          }
        }
        count++;
      }
      List<Object> struct = new ArrayList<Object>(4);
      struct.add(0, key.get());
      struct.add(1, count);
      List<List<Object>> lastWordInfoList = new ArrayList<List<Object>>();
      Collections.sort(lastwords);
      for (String word : lastwords) {
        List<Object> info = new ArrayList<Object>(2);
        info.add(0, word);
        info.add(1, word.length());
        lastWordInfoList.add(info);
      }
      struct.add(2, lastWordInfoList);
      struct.add(3, wordCounts);
      row = serde.serialize(struct, oip);
      context.write(NullWritable.get(), row);
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  //Test outputformat with complex data type, and with reduce
  public void testNewOutputFormatComplex() throws Exception {
    Path inputPath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".txt");
    Path outputPath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    localFs.delete(outputPath, true);
    PrintWriter pw = new PrintWriter(
        new OutputStreamWriter(localFs.create(inputPath)));
    pw.println("I have eaten");
    pw.println("the plums");
    pw.println("that were in");
    pw.println("the icebox");
    pw.println("and which");
    pw.println("you were probably");
    pw.println("saving");
    pw.println("for breakfast");
    pw.println("Forgive me");
    pw.println("they were delicious");
    pw.println("so sweet");
    pw.println("and so cold");
    pw.close();

    Job job = new Job(conf, "orc test");
    job.setOutputFormatClass(OrcNewOutputFormat.class);
    job.setJarByClass(TestNewInputOutputFormat.class);
    job.setMapperClass(OrcTestMapper3.class);
    job.setReducerClass(OrcTestReducer3.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(OrcSerdeRow.class);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    boolean result = job.waitForCompletion(true);
    assertTrue(result);
    
    Path outputFilePath = new Path(outputPath, "part-r-00000");
    Reader reader = OrcFile.createReader(outputFilePath,
        OrcFile.readerOptions(conf).filesystem(localFs));
    
    RecordReader rows = reader.rows();
    ObjectInspector orcOi = reader.getObjectInspector();
    ObjectInspector stoi = TypeInfoUtils
        .getStandardJavaObjectInspectorFromTypeInfo(OrcTestReducer3.typeInfo);
    ObjectInspectorConverters.Converter converter = ObjectInspectorConverters
        .getConverter(orcOi, stoi);
    
    Object row = rows.next(null);
    List<Object> converted = (List<Object>)converter.convert(row);
    assertEquals(1, converted.get(0));
    assertEquals(1, converted.get(1));
    List<Object> list = (List<Object>)converted.get(2);
    assertEquals(list.size(), 1);
    assertEquals("saving", ((List<Object>)list.get(0)).get(0));
    assertEquals(6, ((List<Object>)list.get(0)).get(1));
    Map<String, Integer> map = (Map<String, Integer>)converted.get(3);
    assertEquals(map.size(), 1);
    assertEquals(map.get("saving"), new Integer(1));
    
    row = rows.next(null);
    converted = (List<Object>)converter.convert(row);
    assertEquals(2, converted.get(0));
    assertEquals(6, converted.get(1));
    list = (List<Object>)converted.get(2);
    assertEquals(list.size(), 6);
    assertEquals("breakfast", ((List<Object>)list.get(0)).get(0));
    assertEquals(9, ((List<Object>)list.get(0)).get(1));
    map = (Map<String, Integer>)converted.get(3);
    assertEquals(map.size(), 11);
    assertEquals(map.get("the"), new Integer(2));
    
    row = rows.next(null);
    converted = (List<Object>)converter.convert(row);
    assertEquals(3, converted.get(0));
    assertEquals(5, converted.get(1));
    list = (List<Object>)converted.get(2);
    assertEquals(list.size(), 5);
    assertEquals("cold", ((List<Object>)list.get(0)).get(0));
    assertEquals(4, ((List<Object>)list.get(0)).get(1));
    map = (Map<String, Integer>)converted.get(3);
    assertEquals(map.size(), 13);
    assertEquals(map.get("were"), new Integer(3));
    
    assertFalse(rows.hasNext());
    
    localFs.delete(outputPath, true);
  }
  
  @Test
  // Test inputformat with column prune
  public void testNewInputFormatPruning() throws Exception {
    conf.set("hive.io.file.read.all.columns", "false");
    conf.set("hive.io.file.readcolumn.ids", "1,3");
    Job job = new Job(conf, "orc test");
    job.setInputFormatClass(OrcNewInputFormat.class);
    job.setJarByClass(TestNewInputOutputFormat.class);
    job.setMapperClass(OrcTestMapper1.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(HiveTestUtils
        .getFileFromClasspath("orc-file-11-format.orc")));
    Path outputPath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".txt");
    localFs.delete(outputPath, true);
    FileOutputFormat.setOutputPath(job, outputPath);
    boolean result = job.waitForCompletion(true);
    assertTrue(result);
    Path outputFilePath = new Path(outputPath, "part-m-00000");

    BufferedReader reader = new BufferedReader(
        new InputStreamReader(localFs.open(outputFilePath)));
    String line=reader.readLine();
    
    assertEquals(line, "{null, 1, null, 65536, null, null, null, " +
        "null, null, null, null, null, null, null}");

    localFs.delete(outputPath, true);
  }
}
