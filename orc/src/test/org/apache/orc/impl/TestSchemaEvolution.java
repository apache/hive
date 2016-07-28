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
package org.apache.orc.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestSchemaEvolution {

  @Rule
  public TestName testCaseName = new TestName();

  Configuration conf;
  Path testFilePath;
  FileSystem fs;
  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testDataTypeConversion1() throws IOException {
    TypeDescription fileStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution same1 = new SchemaEvolution(fileStruct1, null);
    assertFalse(same1.hasConversion());
    TypeDescription readerStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution both1 = new SchemaEvolution(fileStruct1, readerStruct1, null);
    assertFalse(both1.hasConversion());
    TypeDescription readerStruct1diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution both1diff = new SchemaEvolution(fileStruct1, readerStruct1diff, null);
    assertTrue(both1diff.hasConversion());
    TypeDescription readerStruct1diffPrecision = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(12).withScale(10));
    SchemaEvolution both1diffPrecision = new SchemaEvolution(fileStruct1, readerStruct1diffPrecision, null);
    assertTrue(both1diffPrecision.hasConversion());
  }

  @Test
  public void testDataTypeConversion2() throws IOException {
    TypeDescription fileStruct2 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createBoolean()))
        .addField("f6", TypeDescription.createChar().withMaxLength(100));
    SchemaEvolution same2 = new SchemaEvolution(fileStruct2, null);
    assertFalse(same2.hasConversion());
    TypeDescription readerStruct2 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createBoolean()))
        .addField("f6", TypeDescription.createChar().withMaxLength(100));
    SchemaEvolution both2 = new SchemaEvolution(fileStruct2, readerStruct2, null);
    assertFalse(both2.hasConversion());
    TypeDescription readerStruct2diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createByte()))
        .addField("f6", TypeDescription.createChar().withMaxLength(100));
    SchemaEvolution both2diff = new SchemaEvolution(fileStruct2, readerStruct2diff, null);
    assertTrue(both2diff.hasConversion());
    TypeDescription readerStruct2diffChar = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createBoolean()))
        .addField("f6", TypeDescription.createChar().withMaxLength(80));
    SchemaEvolution both2diffChar = new SchemaEvolution(fileStruct2, readerStruct2diffChar, null);
    assertTrue(both2diffChar.hasConversion());
  }

  @Test
  public void testFloatToDoubleEvolution() throws Exception {
    testFilePath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    TypeDescription schema = TypeDescription.createFloat();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
            .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    DoubleColumnVector dcv = new DoubleColumnVector(1024);
    batch.cols[0] = dcv;
    batch.reset();
    batch.size = 1;
    dcv.vector[0] = 74.72f;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDouble();
    RecordReader rows = reader.rows(new Reader.Options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatch();
    rows.nextBatch(batch);
    assertEquals(74.72, ((DoubleColumnVector) batch.cols[0]).vector[0], 0.00000000001);
    rows.close();
  }

  @Test
  public void testSafePpdEvaluation() throws IOException {
    TypeDescription fileStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution same1 = new SchemaEvolution(fileStruct1, null);
    assertTrue(same1.isPPDSafeConversion(0));
    assertFalse(same1.hasConversion());
    TypeDescription readerStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution both1 = new SchemaEvolution(fileStruct1, readerStruct1, null);
    assertFalse(both1.hasConversion());
    assertTrue(both1.isPPDSafeConversion(0));
    assertTrue(both1.isPPDSafeConversion(1));
    assertTrue(both1.isPPDSafeConversion(2));
    assertTrue(both1.isPPDSafeConversion(3));

    // int -> long
    TypeDescription readerStruct1diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution both1diff = new SchemaEvolution(fileStruct1, readerStruct1diff, null);
    assertTrue(both1diff.hasConversion());
    assertFalse(both1diff.isPPDSafeConversion(0));
    assertTrue(both1diff.isPPDSafeConversion(1));
    assertTrue(both1diff.isPPDSafeConversion(2));
    assertTrue(both1diff.isPPDSafeConversion(3));

    // decimal(38,10) -> decimal(12, 10)
    TypeDescription readerStruct1diffPrecision = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(12).withScale(10));
    SchemaEvolution both1diffPrecision = new SchemaEvolution(fileStruct1, readerStruct1diffPrecision,
        new boolean[] {true, false, false, true});
    assertTrue(both1diffPrecision.hasConversion());
    assertFalse(both1diffPrecision.isPPDSafeConversion(0));
    assertFalse(both1diffPrecision.isPPDSafeConversion(1)); // column not included
    assertFalse(both1diffPrecision.isPPDSafeConversion(2)); // column not included
    assertFalse(both1diffPrecision.isPPDSafeConversion(3));

    // add columns
    readerStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10))
        .addField("f4", TypeDescription.createBoolean());
    both1 = new SchemaEvolution(fileStruct1, readerStruct1, null);
    assertTrue(both1.hasConversion());
    assertFalse(both1.isPPDSafeConversion(0));
    assertTrue(both1.isPPDSafeConversion(1));
    assertTrue(both1.isPPDSafeConversion(2));
    assertTrue(both1.isPPDSafeConversion(3));
    assertFalse(both1.isPPDSafeConversion(4));
  }

  @Test
  public void testSafePpdEvaluationForInts() throws IOException {
    // byte -> short -> int -> long
    TypeDescription fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createByte());
    SchemaEvolution schemaEvolution = new SchemaEvolution(fileSchema, null);
    assertFalse(schemaEvolution.hasConversion());

    // byte -> short
    TypeDescription readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // byte -> int
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // byte -> long
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // short -> int -> long
    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort());
    schemaEvolution = new SchemaEvolution(fileSchema, null);
    assertFalse(schemaEvolution.hasConversion());

    // unsafe conversion short -> byte
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createByte());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // short -> int
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // short -> long
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // int -> long
    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, null);
    assertFalse(schemaEvolution.hasConversion());

    // unsafe conversion int -> byte
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createByte());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // unsafe conversion int -> short
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // int -> long
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // long
    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong());
    schemaEvolution = new SchemaEvolution(fileSchema, null);
    assertTrue(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.hasConversion());

    // unsafe conversion long -> byte
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createByte());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // unsafe conversion long -> short
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // unsafe conversion long -> int
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createString());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createFloat());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createTimestamp());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));
  }

  @Test
  public void testSafePpdEvaluationForStrings() throws IOException {
    TypeDescription fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createString());
    SchemaEvolution schemaEvolution = new SchemaEvolution(fileSchema, null);
    assertTrue(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.hasConversion());

    // string -> char
    TypeDescription readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createChar());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // string -> varchar
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createVarchar());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createChar());
    schemaEvolution = new SchemaEvolution(fileSchema, null);
    assertTrue(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.hasConversion());

    // char -> string
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createString());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // char -> varchar
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createVarchar());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createVarchar());
    schemaEvolution = new SchemaEvolution(fileSchema, null);
    assertTrue(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.hasConversion());

    // varchar -> string
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createString());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // varchar -> char
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createChar());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createDecimal());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createDate());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, null);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));
  }
}
