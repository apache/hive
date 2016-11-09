package org.apache.hadoop.hive.ql.io.parquet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestVectorizedDictionaryEncodingColumnReader extends TestVectorizedColumnReaderBase {
  static boolean isDictionaryEncoding = true;

  @BeforeClass
  public static void setup() throws IOException {
    removeFile();
    writeData(initWriterFromFile(), isDictionaryEncoding);
  }

  @AfterClass
  public static void cleanup() throws IOException {
    removeFile();
  }

  @Test
  public void testIntRead() throws Exception {
    intRead(isDictionaryEncoding);
  }

  @Test
  public void testLongRead() throws Exception {
    longRead(isDictionaryEncoding);
  }

  @Test
  public void testDoubleRead() throws Exception {
    doubleRead(isDictionaryEncoding);
  }

  @Test
  public void testFloatRead() throws Exception {
    floatRead(isDictionaryEncoding);
  }

  @Test
  public void testBooleanRead() throws Exception {
    booleanRead(isDictionaryEncoding);
  }

  @Test
  public void testBinaryRead() throws Exception {
    binaryRead(isDictionaryEncoding);
  }

  @Test
  public void testStructRead() throws Exception {
    structRead(isDictionaryEncoding);
  }
}
