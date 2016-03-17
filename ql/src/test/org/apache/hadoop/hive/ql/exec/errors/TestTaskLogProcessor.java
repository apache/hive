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

package org.apache.hadoop.hive.ql.exec.errors;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestTaskLogProcessor {
  
  private final List<File> toBeDeletedList = new LinkedList<File>();
  
  @After
  public void after() {
    for (File f: toBeDeletedList) {
      f.delete();
    }
    toBeDeletedList.clear();
  }
  
  private File writeTestLog(String id, String content) throws IOException {
    // Put the script content in a temp file
    File scriptFile = File.createTempFile(getClass().getName() + "-" + id + "-", ".log");
    scriptFile.deleteOnExit();
    toBeDeletedList.add(scriptFile);
    PrintStream os = new PrintStream(new FileOutputStream(scriptFile));
    try {
      os.print(content);
    } finally {
      os.close();
    }
    return scriptFile;
  }
  
  private String toString(Throwable t) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, false); 
    t.printStackTrace(pw);
    pw.close();
    return sw.toString();
  }
  
  /*
   * returns number of lines in the printed throwable stack trace.
   */
  private String writeThrowableAsFile(String before, Throwable t, String after, 
      String fileSuffix, TaskLogProcessor taskLogProcessor) throws IOException {
    // compose file text:
    StringBuilder sb = new StringBuilder();
    if (before != null) {
      sb.append(before);
    }
    final String stackTraceStr = toString(t);
    sb.append(stackTraceStr);
    if (after != null) {
      sb.append(after);
    }
    
    // write it to file:
    File file = writeTestLog(fileSuffix, sb.toString());
    // add it to the log processor:
    taskLogProcessor.addTaskAttemptLogUrl(file.toURI().toURL().toString());
    return stackTraceStr;
  }

  @Test
  public void testGetStackTraces() throws Exception {
    JobConf jobConf = new JobConf();
    HiveConf.setQueryString(jobConf, "select * from foo group by moo;");

    final TaskLogProcessor taskLogProcessor = new TaskLogProcessor(jobConf);

    Throwable oome = new OutOfMemoryError("java heap space");
    String oomeStr = writeThrowableAsFile("Some line in the beginning\n", oome, null, "1", taskLogProcessor);

    Throwable compositeException = new InvocationTargetException(new IOException(new NullPointerException()));
    String compositeStr = writeThrowableAsFile(null, compositeException, "Some line in the end.\n", "2", taskLogProcessor);

    Throwable eofe = new EOFException();
    String eofeStr = writeThrowableAsFile("line a\nlineb\n", eofe, " line c\nlineD\n", "3", taskLogProcessor);
    
    List<List<String>> stackTraces = taskLogProcessor.getStackTraces();
    assertEquals(3, stackTraces.size());
    
    // Assert the actual stack traces are exactly equal to the written ones, 
    // and are contained in "stackTraces" list in the submission order:
    checkException(oomeStr, stackTraces.get(0));
    checkException(compositeStr, stackTraces.get(1));
    checkException(eofeStr, stackTraces.get(2));
  }
  
  private void checkException(String writenText, List<String> actualTrace) throws IOException {
    List<String> expectedLines = getLines(writenText);
    String expected, actual; 
    for (int i=0; i<expectedLines.size(); i++) {
      expected = expectedLines.get(i);
      actual = actualTrace.get(i);
      assertEquals(expected, actual);
    }
  }
  
  private List<String> getLines(String text) throws IOException{
    BufferedReader br = new BufferedReader(new StringReader(text));
    List<String> list = new ArrayList<String>(48);
    String string;
    while (true) {
      string = br.readLine();
      if (string == null) {
        break;
      } else {
        list.add(string);
      }
    }
    br.close();
    return list;
  }
  
  @Test
  public void testScriptErrorHeuristic() throws Exception {
    JobConf jobConf = new JobConf();
    HiveConf.setQueryString(jobConf, "select * from foo group by moo;");

    final TaskLogProcessor taskLogProcessor = new TaskLogProcessor(jobConf);
    
    String errorCode = "7874"; // example code
    String content = "line a\nlineb\n" + "Script failed with code " + errorCode + " line c\nlineD\n";
    File log3File = writeTestLog("1", content);
    taskLogProcessor.addTaskAttemptLogUrl(log3File.toURI().toURL().toString());
    
    List<ErrorAndSolution> errList = taskLogProcessor.getErrors();
    assertEquals(1, errList.size());
    
    final ErrorAndSolution eas = errList.get(0);
    
    String error = eas.getError();
    assertNotNull(error);
    // check that the error code is present in the error description: 
    assertTrue(error.indexOf(errorCode) >= 0);
    
    String solution = eas.getSolution();
    assertNotNull(solution);
    assertTrue(solution.length() > 0);
  }

  @Test
  public void testDataCorruptErrorHeuristic() throws Exception {
    JobConf jobConf = new JobConf();
    HiveConf.setQueryString(jobConf, "select * from foo group by moo;");

    final TaskLogProcessor taskLogProcessor = new TaskLogProcessor(jobConf);
    
    String badFile1 = "hdfs://localhost/foo1/moo1/zoo1"; 
    String badFile2 = "hdfs://localhost/foo2/moo2/zoo2"; 
    String content = "line a\nlineb\n" 
       + "split: " + badFile1 + " is very bad.\n" 
       + " line c\nlineD\n" 
       + "split: " + badFile2 + " is also very bad.\n" 
       + " java.io.EOFException: null \n" 
       + "line E\n";
    File log3File = writeTestLog("1", content);
    taskLogProcessor.addTaskAttemptLogUrl(log3File.toURI().toURL().toString());
    
    List<ErrorAndSolution> errList = taskLogProcessor.getErrors();
    assertEquals(1, errList.size());
    
    final ErrorAndSolution eas = errList.get(0);
    
    String error = eas.getError();
    assertNotNull(error);
    // check that the error code is present in the error description: 
    assertTrue(error.contains(badFile1) || error.contains(badFile2));
    
    String solution = eas.getSolution();
    assertNotNull(solution);
    assertTrue(solution.length() > 0);
  }
  
  @Test
  public void testMapAggrMemErrorHeuristic() throws Exception {
    JobConf jobConf = new JobConf();
    HiveConf.setQueryString(jobConf, "select * from foo group by moo;");

    final TaskLogProcessor taskLogProcessor = new TaskLogProcessor(jobConf);

    Throwable oome = new OutOfMemoryError("java heap space");
    File log1File = writeTestLog("1", toString(oome));
    taskLogProcessor.addTaskAttemptLogUrl(log1File.toURI().toURL().toString());
    
    List<ErrorAndSolution> errList = taskLogProcessor.getErrors();
    assertEquals(1, errList.size());
    
    final ErrorAndSolution eas = errList.get(0);
    
    String error = eas.getError();
    assertNotNull(error);
    // check that the error code is present in the error description: 
    assertTrue(error.contains("memory"));
    
    String solution = eas.getSolution();
    assertNotNull(solution);
    assertTrue(solution.length() > 0);
    String confName = HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY.toString();
    assertTrue(solution.contains(confName));
  }
  
}
