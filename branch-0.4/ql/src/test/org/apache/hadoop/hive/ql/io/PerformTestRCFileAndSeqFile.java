package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;

public class PerformTestRCFileAndSeqFile extends TestCase {

  private Configuration conf = new Configuration();

  private Path testRCFile;
  private Path testSeqFile;

  private FileSystem fs;

  int columnMaxSize = 30;

  Random randomCharGenerator = new Random(3);

  Random randColLenGenerator = new Random(20);

  public PerformTestRCFileAndSeqFile(boolean local, String file)
      throws IOException {
    if (local)
      fs = FileSystem.getLocal(conf);
    else
      fs = FileSystem.get(conf);
    conf.setInt(RCFile.Writer.COLUMNS_BUFFER_SIZE_CONF_STR, 1 * 1024 * 1024);
    if (file == null) {
      Path dir = new Path(System.getProperty("test.data.dir", ".") + "/mapred");
      this.testRCFile = new Path(dir, "test_rcfile");
      this.testSeqFile = new Path(dir, "test_seqfile");
    } else {
      this.testRCFile = new Path(file + "-rcfile");
      this.testSeqFile = new Path(file + "-seqfile");
    }
    fs.delete(testRCFile, true);
    fs.delete(this.testSeqFile, true);
    System.out.println("RCFile:" + this.testRCFile.toString());
    System.out.println("SequenceFile:" + this.testSeqFile.toString());
  }

  private void writeSeqenceFileTest(FileSystem fs, int rowCount, Path file,
      int columnNum, CompressionCodec codec) throws IOException {

    byte[][] columnRandom;

    resetRandomGenerators();

    BytesRefArrayWritable bytes = new BytesRefArrayWritable(columnNum);
    columnRandom = new byte[columnNum][];
    for (int i = 0; i < columnNum; i++) {
      BytesRefWritable cu = new BytesRefWritable();
      bytes.set(i, cu);
    }

    // zero length key is not allowed by block compress writer, so we use a byte
    // writable
    ByteWritable key = new ByteWritable();
    SequenceFile.Writer seqWriter = SequenceFile.createWriter(fs, conf, file,
        ByteWritable.class, BytesRefArrayWritable.class, CompressionType.BLOCK,
        codec);

    for (int i = 0; i < rowCount; i++) {
      nextRandomRow(columnRandom, bytes);
      seqWriter.append(key, bytes);
    }
    seqWriter.close();
  }

  private void resetRandomGenerators() {
    randomCharGenerator = new Random(3);
    randColLenGenerator = new Random(20);
  }

  private void writeRCFileTest(FileSystem fs, int rowCount, Path file,
      int columnNum, CompressionCodec codec) throws IOException {
    fs.delete(file, true);

    resetRandomGenerators();

    RCFileOutputFormat.setColumnNumber(conf, columnNum);
    RCFile.Writer writer = new RCFile.Writer(fs, conf, file, null, codec);

    byte[][] columnRandom;

    BytesRefArrayWritable bytes = new BytesRefArrayWritable(columnNum);
    columnRandom = new byte[columnNum][];
    for (int i = 0; i < columnNum; i++) {
      BytesRefWritable cu = new BytesRefWritable();
      bytes.set(i, cu);
    }

    for (int i = 0; i < rowCount; i++) {
      nextRandomRow(columnRandom, bytes);
      writer.append(bytes);
    }
    writer.close();
  }

  private void nextRandomRow(byte[][] row, BytesRefArrayWritable bytes) {
    bytes.resetValid(row.length);
    for (int i = 0; i < row.length; i++) {
      int len = Math.abs(randColLenGenerator.nextInt(this.columnMaxSize));
      row[i] = new byte[len];
      for (int j = 0; j < len; j++)
        row[i][j] = getRandomChar(this.randomCharGenerator);
      bytes.get(i).set(row[i], 0, len);
    }
  }

  private static int CHAR_END = 122 - 7;

  private byte getRandomChar(Random random) {
    byte b = 0;
    do {
      b = (byte) random.nextInt(CHAR_END);
    } while ((b < 65));
    if (b > 90)
      b += 7;
    return b;
  }

  private static String usage = "Usage: PerformTestRCFileAndSeqFile "
      + "[-count N]" + " [file]";

  public static void main(String[] args) throws Exception {
    int count = 1000;
    String file = null;

    try {
      for (int i = 0; i < args.length; ++i) { // parse command line
        if (args[i] == null) {
          continue;
        } else if (args[i].equals("-count")) {
          count = Integer.parseInt(args[++i]);
        } else {
          // file is required parameter
          file = args[i];
        }
      }

      // change it to choose the appropriate file system
      boolean isLocalFS = true;

      PerformTestRCFileAndSeqFile testcase = new PerformTestRCFileAndSeqFile(
          isLocalFS, file);

      // change these parameters
      boolean checkCorrect = true;
      CompressionCodec codec = new DefaultCodec();
      testcase.columnMaxSize = 30;

      // testcase.testWithColumnNumber(count, 2, checkCorrect, codec);
      // testcase.testWithColumnNumber(count, 10, checkCorrect, codec);
      // testcase.testWithColumnNumber(count, 25, checkCorrect, codec);
      testcase.testWithColumnNumber(count, 40, checkCorrect, codec);
      // testcase.testWithColumnNumber(count, 50, checkCorrect, codec);
      // testcase.testWithColumnNumber(count, 80, checkCorrect, codec);

    } finally {
    }
  }

  private void testWithColumnNumber(int rowCount, int columnNum,
      boolean checkCorrect, CompressionCodec codec) throws IOException {
    // rcfile

    // rcfile write
    long start = System.currentTimeMillis();
    writeRCFileTest(fs, rowCount, testRCFile, columnNum, codec);
    long cost = System.currentTimeMillis() - start;
    long fileLen = fs.getFileStatus(testRCFile).getLen();
    System.out.println("Write RCFile with " + columnNum
        + " random string columns and " + rowCount + " rows cost " + cost
        + " milliseconds. And the file's on disk size is " + fileLen);

    // sequence file write
    start = System.currentTimeMillis();
    writeSeqenceFileTest(fs, rowCount, this.testSeqFile, columnNum, codec);
    cost = System.currentTimeMillis() - start;
    fileLen = fs.getFileStatus(this.testSeqFile).getLen();
    System.out.println("Write SequenceFile with " + columnNum
        + " random string columns and " + rowCount + " rows cost " + cost
        + " milliseconds. And the file's on disk size is " + fileLen);

    // rcfile read
    start = System.currentTimeMillis();
    int readRows = performRCFileReadFirstColumnTest(fs, testRCFile, columnNum,
        checkCorrect);
    cost = System.currentTimeMillis() - start;
    System.out.println("Read only one column of a RCFile with " + columnNum
        + " random string columns and " + rowCount + " rows cost " + cost
        + " milliseconds.");
    if (rowCount != readRows)
      throw new IllegalStateException("Compare read and write row count error.");
    assertEquals("", rowCount, readRows);

    if (isLocalFileSystem() && !checkCorrect) {
      // make some noisy to avoid disk caches data.
      performSequenceFileRead(fs, rowCount, this.testSeqFile);
    }

    start = System.currentTimeMillis();
    readRows = performRCFileReadFirstAndLastColumnTest(fs, testRCFile,
        columnNum, checkCorrect);
    cost = System.currentTimeMillis() - start;
    System.out.println("Read only first and last columns of a RCFile with "
        + columnNum + " random string columns and " + rowCount + " rows cost "
        + cost + " milliseconds.");
    if (rowCount != readRows)
      throw new IllegalStateException("Compare read and write row count error.");
    assertEquals("", rowCount, readRows);

    if (isLocalFileSystem() && !checkCorrect) {
      // make some noisy to avoid disk caches data.
      performSequenceFileRead(fs, rowCount, this.testSeqFile);
    }

    start = System.currentTimeMillis();
    performRCFileFullyReadColumnTest(fs, testRCFile, columnNum, checkCorrect);
    cost = System.currentTimeMillis() - start;
    System.out.println("Read all columns of a RCFile with " + columnNum
        + " random string columns and " + rowCount + " rows cost " + cost
        + " milliseconds.");
    if (rowCount != readRows)
      throw new IllegalStateException("Compare read and write row count error.");
    assertEquals("", rowCount, readRows);

    // sequence file read
    start = System.currentTimeMillis();
    performSequenceFileRead(fs, rowCount, this.testSeqFile);
    cost = System.currentTimeMillis() - start;
    System.out.println("Read SequenceFile with " + columnNum
        + "  random string columns and " + rowCount + " rows cost " + cost
        + " milliseconds.");
  }

  public boolean isLocalFileSystem() {
    return fs.getUri().toString().startsWith("file://");
  }

  public void performSequenceFileRead(FileSystem fs, int count, Path file)
      throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
    ByteWritable key = new ByteWritable();
    BytesRefArrayWritable val = new BytesRefArrayWritable();
    for (int i = 0; i < count; i++)
      reader.next(key, val);
  }

  public int performRCFileReadFirstColumnTest(FileSystem fs, Path file,
      int allColumnsNumber, boolean chechCorrect) throws IOException {

    byte[][] checkBytes = null;
    BytesRefArrayWritable checkRow = new BytesRefArrayWritable(allColumnsNumber);
    if (chechCorrect) {
      this.resetRandomGenerators();
      checkBytes = new byte[allColumnsNumber][];
    }

    int actualReadCount = 0;

    java.util.ArrayList<Integer> readCols = new java.util.ArrayList<Integer>();
    readCols.add(Integer.valueOf(0));
    HiveFileFormatUtils.setReadColumnIDs(conf, readCols);
    RCFile.Reader reader = new RCFile.Reader(fs, file, conf);

    LongWritable rowID = new LongWritable();
    BytesRefArrayWritable cols = new BytesRefArrayWritable();
    while (reader.next(rowID)) {
      reader.getCurrentRow(cols);
      boolean ok = true;
      if (chechCorrect) {
        this.nextRandomRow(checkBytes, checkRow);
        ok = ok && (checkRow.get(0).equals(cols.get(0)));
      }
      if (!ok)
        throw new IllegalStateException("Compare read and write error.");
      actualReadCount++;
    }
    return actualReadCount;
  }

  public int performRCFileReadFirstAndLastColumnTest(FileSystem fs, Path file,
      int allColumnsNumber, boolean chechCorrect) throws IOException {

    byte[][] checkBytes = null;
    BytesRefArrayWritable checkRow = new BytesRefArrayWritable(allColumnsNumber);
    if (chechCorrect) {
      this.resetRandomGenerators();
      checkBytes = new byte[allColumnsNumber][];
    }

    int actualReadCount = 0;

    java.util.ArrayList<Integer> readCols = new java.util.ArrayList<Integer>();
    readCols.add(Integer.valueOf(0));
    readCols.add(Integer.valueOf(allColumnsNumber - 1));
    HiveFileFormatUtils.setReadColumnIDs(conf, readCols);
    RCFile.Reader reader = new RCFile.Reader(fs, file, conf);

    LongWritable rowID = new LongWritable();
    BytesRefArrayWritable cols = new BytesRefArrayWritable();
    while (reader.next(rowID)) {
      reader.getCurrentRow(cols);
      boolean ok = true;
      if (chechCorrect) {
        this.nextRandomRow(checkBytes, checkRow);
        ok = ok && (checkRow.get(0).equals(cols.get(0)));
        ok = ok
            && checkRow.get(allColumnsNumber - 1).equals(
                cols.get(allColumnsNumber - 1));
      }
      if (!ok)
        throw new IllegalStateException("Compare read and write error.");
      actualReadCount++;
    }
    return actualReadCount;
  }

  public int performRCFileFullyReadColumnTest(FileSystem fs, Path file,
      int allColumnsNumber, boolean chechCorrect) throws IOException {

    byte[][] checkBytes = null;
    BytesRefArrayWritable checkRow = new BytesRefArrayWritable(allColumnsNumber);
    if (chechCorrect) {
      this.resetRandomGenerators();
      checkBytes = new byte[allColumnsNumber][];
    }

    int actualReadCount = 0;

    HiveFileFormatUtils.setFullyReadColumns(conf);
    RCFile.Reader reader = new RCFile.Reader(fs, file, conf);

    LongWritable rowID = new LongWritable();
    BytesRefArrayWritable cols = new BytesRefArrayWritable();
    while (reader.next(rowID)) {
      reader.getCurrentRow(cols);
      boolean ok = true;
      if (chechCorrect) {
        this.nextRandomRow(checkBytes, checkRow);
        ok = ok && checkRow.equals(cols);
      }
      if (!ok)
        throw new IllegalStateException("Compare read and write error.");
      actualReadCount++;
    }
    return actualReadCount;
  }

}
