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
package org.apache.hadoop.hive.ql.io.orc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcFile.WriterContext;
import org.apache.orc.impl.OrcAcidUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class TestFixAcidKeyIndex {
  public final static Logger LOG = LoggerFactory.getLogger(TestFixAcidKeyIndex.class);

  @Rule
  public TestName testCaseName = new TestName();
  Path workDir = new Path(System.getProperty("test.tmp.dir","target/tmp"));
  Configuration conf;
  Path testFilePath;
  FileSystem fs;

  @Before
  public void openFileSystem () throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestFixAcidKeyIndex." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  static abstract class TestKeyIndexBuilder
      extends OrcRecordUpdater.KeyIndexBuilder
      implements OrcFile.WriterCallback {

    // Will be called before closing the ORC file to stop writing any additional information
    // to the acid key index.
    abstract void stopWritingKeyIndex();
  }

  void createTestAcidFile(Path path, int numRows, TestKeyIndexBuilder indexBuilder) throws Exception {
    FileSystem fs = path.getFileSystem(conf);
    fs.delete(path, true);
    String typeStr = "struct<operation:int," +
        "originalTransaction:bigint,bucket:int,rowId:bigint," +
        "currentTransaction:bigint," +
        "row:struct<a:int,b:struct<c:int>,d:string>>";
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
    Writer writer = OrcFile.createWriter(path,
        OrcFile.writerOptions(conf)
            .fileSystem(fs)
            .inspector(OrcStruct.createObjectInspector(typeInfo))
            .compress(CompressionKind.NONE)
            .callback(indexBuilder)
            .stripeSize(128));
    // Create ORC file with small stripe size so we can write multiple stripes.
    OrcStruct row = new OrcStruct(6);
    row.setFieldValue(0, new IntWritable(0));
    row.setFieldValue(1, new LongWritable(1));
    row.setFieldValue(2, new IntWritable(0));
    LongWritable rowId = new LongWritable();
    row.setFieldValue(3, rowId);
    row.setFieldValue(4, new LongWritable(1));
    OrcStruct rowField = new OrcStruct(3);
    row.setFieldValue(5, rowField);
    IntWritable a = new IntWritable();
    rowField.setFieldValue(0, a);
    OrcStruct b = new OrcStruct(1);
    rowField.setFieldValue(1, b);
    IntWritable c = new IntWritable();
    b.setFieldValue(0, c);
    Text d = new Text();
    rowField.setFieldValue(2, d);

    // Minimum 5000 rows per stripe.
    for(int r=0; r < numRows; r++) {
      // row id
      rowId.set(r);
      // a
      a.set(r * 42);
      // b.c
      c.set(r * 10001);
      // d
      d.set(Integer.toHexString(r));
      indexBuilder.addKey(OrcRecordUpdater.INSERT_OPERATION, 1, 0, rowId.get());
      writer.addRow(row);
    }

    indexBuilder.stopWritingKeyIndex();

    writer.close();
  }

  void runIndexCheck(Path orcFile, File outFile) throws Exception {
    // Run with --check-index and save the output to file so it can be checked.
    PrintStream origOut = System.out;
    FileOutputStream myOut = new FileOutputStream(outFile);

    System.setOut(new PrintStream(myOut));
    String[] checkArgs = new String[] {
        "--check-only",
        orcFile.toString()
    };
    FixAcidKeyIndex.main(checkArgs);
    System.out.flush();
    System.setOut(origOut);
  }

  void checkValidKeyIndex(Path orcFile) throws Exception {
    String outputFilename = "fixAcidKeyIndex.out";
    File outFile = new File(workDir.toString(), outputFilename);
    runIndexCheck(orcFile, outFile);

    // Check the output of FixAcidKeyIndex - it should indicate the index was valid.
    String outputAsString = FileUtils.readFileToString(outFile);
    System.out.println(outputAsString);
    assertTrue(outputAsString.contains("acid key index is valid"));
  }

  void checkInvalidKeyIndex(Path orcFile) throws Exception {
    String outputFilename = "fixAcidKeyIndex.out";
    File outFile = new File(workDir.toString(), outputFilename);
    runIndexCheck(orcFile, outFile);

    // Check the output of FixAcidKeyIndex - it should indicate the index was invalid.
    String outputAsString = FileUtils.readFileToString(outFile);
    System.out.println(outputAsString);
    assertTrue(outputAsString.contains("acid key index is invalid"));
  }

  void runFixIndex(Path orcFile, File outFile) throws Exception {
    // Run with --recover and save the output to a file so it can be checked.
    PrintStream origOut = System.out;
    FileOutputStream myOut = new FileOutputStream(outFile);

    System.setOut(new PrintStream(myOut));
    String[] checkArgs = new String[] {
        "--recover",
        orcFile.toString()
    };
    FixAcidKeyIndex.main(checkArgs);
    System.out.flush();
    System.setOut(origOut);
  }

  void fixInvalidIndex(Path orcFile) throws Exception {
    String outputFilename = "fixAcidKeyIndex.out";
    File outFile = new File(workDir.toString(), outputFilename);
    runFixIndex(orcFile, outFile);

    // Check the output of FixAcidKeyIndex - it should indicate the index was fixed.
    String outputAsString = FileUtils.readFileToString(outFile);
    System.out.println(outputAsString);
    assertTrue(outputAsString.contains("Fixed acid key index"));
  }

  void fixValidIndex(Path orcFile) throws Exception {
    String outputFilename = "fixAcidKeyIndex.out";
    File outFile = new File(workDir.toString(), outputFilename);
    runFixIndex(orcFile, outFile);

    // Check the output of FixAcidKeyIndex - it should indicate nothing required fixing.
    String outputAsString = FileUtils.readFileToString(outFile);
    System.out.println(outputAsString);
    assertTrue(outputAsString.contains("No need to recover"));
  }

  @Test
  public void testValidKeyIndex() throws Exception {
    // Try with 0 row file.
    createTestAcidFile(testFilePath, 0, new GoodKeyIndexBuilder());
    checkValidKeyIndex(testFilePath);
    // Attempting to fix a valid - should not result in a new file.
    fixValidIndex(testFilePath);

    // Try single stripe
    createTestAcidFile(testFilePath, 100, new GoodKeyIndexBuilder());
    checkValidKeyIndex(testFilePath);
    // Attempting to fix a valid - should not result in a new file.
    fixValidIndex(testFilePath);

    // Multiple stripes
    createTestAcidFile(testFilePath, 12000, new GoodKeyIndexBuilder());
    checkValidKeyIndex(testFilePath);
    // Attempting to fix a valid - should not result in a new file.
    fixValidIndex(testFilePath);
  }

  @Test
  public void testInvalidKeyIndex() throws Exception {
    // Try single stripe
    createTestAcidFile(testFilePath, 100, new BadKeyIndexBuilder());
    checkInvalidKeyIndex(testFilePath);
    // Try fixing, this should result in new fixed file.
    fixInvalidIndex(testFilePath);

    // Multiple stripes
    createTestAcidFile(testFilePath, 12000, new BadKeyIndexBuilder());
    checkInvalidKeyIndex(testFilePath);
    // Try fixing, this should result in new fixed file.
    fixInvalidIndex(testFilePath);

    // Multiple stripes
    createTestAcidFile(testFilePath, 12000, new FaultyKeyIndexBuilder());
    checkInvalidKeyIndex(testFilePath);
    // Try fixing, this should result in new fixed file.
    fixInvalidIndex(testFilePath);
  }

  @Test
  public void testMissingKeyIndex() throws Exception {
    // Try single stripe
    createTestAcidFile(testFilePath, 100, new MissingKeyIndexBuilder());
    checkInvalidKeyIndex(testFilePath);
    // Try fixing, this should result in new fixed file.
    fixInvalidIndex(testFilePath);

    // Multiple stripes
    createTestAcidFile(testFilePath, 12000, new MissingKeyIndexBuilder());
    checkInvalidKeyIndex(testFilePath);
    // Try fixing, this should result in new fixed file.
    fixInvalidIndex(testFilePath);
  }

  @Test
  public void testNonAcidOrcFile() throws Exception {
    // Copy data/files/alltypesorc to workDir
    Path baseSrcDir = new Path(System.getProperty("basedir")).getParent();
    Path dataFilesPath = new Path(new Path(baseSrcDir, "data"), "files");
    File origOrcFile = new File(dataFilesPath.toString(), "alltypesorc");
    File testOrcFile = new File(workDir.toString(), "alltypesorc");
    FileUtils.copyFile(origOrcFile, testOrcFile);

    String outputFilename = "fixAcidKeyIndex.out";
    File outFile = new File(workDir.toString(), outputFilename);
    runIndexCheck(new Path(testOrcFile.getPath()), outFile);
    String outputAsString = FileUtils.readFileToString(outFile);
    System.out.println(outputAsString);
    assertTrue(outputAsString.contains("is not an acid file"));
  }

  /**
   * Version of KeyIndexBuilder that does not generate any key index
   */
  static class MissingKeyIndexBuilder extends TestKeyIndexBuilder {

    @Override
    public void stopWritingKeyIndex() {
      // Do nothing - this should generate proper index.
    }

    @Override
    public void preFooterWrite(OrcFile.WriterContext context) throws IOException {
      if(numKeysCurrentStripe > 0) {
        preStripeWrite(context);
      }
      context.getWriter().addUserMetadata(
          OrcAcidUtils.ACID_STATS, StandardCharsets.UTF_8.encode(acidStats.serialize()));
      // here we don't generate the "hive.acid.key.index" metadata entry
    }
  }

  /**
   * Version of KeyIndexBuilder that generates a valid key index
   */
  static class GoodKeyIndexBuilder extends TestKeyIndexBuilder {

    @Override
    public void stopWritingKeyIndex() {
      // Do nothing - this should generate proper index.
    }
  }

  /**
   * Bad version of KeyIndexBuilder which builds an invalid acid key index
   * by not including the key index info once stopWritingKeyIndex() is called.
   */
  static class BadKeyIndexBuilder extends TestKeyIndexBuilder {

    boolean writeAcidIndexInfo = true;

    public void stopWritingKeyIndex() {
      LOG.info("*** Stop writing index!");
      writeAcidIndexInfo = false;
    }

    @Override
    public void preStripeWrite(OrcFile.WriterContext context) throws IOException {
      LOG.info("*** writeAcidIndexInfo: " + writeAcidIndexInfo);
      if (!writeAcidIndexInfo) {
        return;
      }

      super.preStripeWrite(context);
    }
  }

  /**
   * Another bad version of KeyIndexBuilder which builds an invalid acid key index
   * by inserting wrong values.
   */
  static class FaultyKeyIndexBuilder extends TestKeyIndexBuilder {

    @Override
    public void preStripeWrite(WriterContext context) throws IOException {
      this.lastRowId = lastRowId - 5;
      super.preStripeWrite(context);
    }

    @Override
    void stopWritingKeyIndex() {
      //NOOP
    }
  }
}
