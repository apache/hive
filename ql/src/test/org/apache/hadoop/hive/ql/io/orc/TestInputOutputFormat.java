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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.FSRecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestInputOutputFormat {

  Path workDir = new Path(System.getProperty("test.tmp.dir","target/tmp"));

  public static class MyRow implements Writable {
    int x;
    int y;
    MyRow(int x, int y) {
      this.x = x;
      this.y = y;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      throw new UnsupportedOperationException("no write");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
     throw new UnsupportedOperationException("no read");
    }
  }

  @Rule
  public TestName testCaseName = new TestName();
  JobConf conf;
  FileSystem fs;
  Path testFilePath;

  @Before
  public void openFileSystem () throws Exception {
    conf = new JobConf();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestInputOutputFormat." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testOverlap() throws Exception {
    assertEquals(0, OrcInputFormat.SplitGenerator.getOverlap(100, 100,
        200, 100));
    assertEquals(0, OrcInputFormat.SplitGenerator.getOverlap(0, 1000,
        2000, 100));
    assertEquals(100, OrcInputFormat.SplitGenerator.getOverlap(1000, 1000,
        1500, 100));
    assertEquals(250, OrcInputFormat.SplitGenerator.getOverlap(1000, 250,
        500, 2000));
    assertEquals(100, OrcInputFormat.SplitGenerator.getOverlap(1000, 1000,
        1900, 1000));
    assertEquals(500, OrcInputFormat.SplitGenerator.getOverlap(2000, 1000,
        2500, 2000));
  }

  @Test
  public void testGetInputPaths() throws Exception {
    conf.set("mapred.input.dir", "a,b,c");
    assertArrayEquals(new Path[]{new Path("a"), new Path("b"), new Path("c")},
        OrcInputFormat.getInputPaths(conf));
    conf.set("mapred.input.dir", "/a/b/c/d/e");
    assertArrayEquals(new Path[]{new Path("/a/b/c/d/e")},
        OrcInputFormat.getInputPaths(conf));
    conf.set("mapred.input.dir", "/a/b/c\\,d,/e/f\\,g/h");
    assertArrayEquals(new Path[]{new Path("/a/b/c,d"), new Path("/e/f,g/h")},
        OrcInputFormat.getInputPaths(conf));
  }

  static class TestContext extends OrcInputFormat.Context {
    List<Runnable> queue = new ArrayList<Runnable>();

    TestContext(Configuration conf) {
      super(conf);
    }

    @Override
    public void schedule(Runnable runnable) {
      queue.add(runnable);
    }
  }

  @Test
  public void testFileGenerator() throws Exception {
    TestContext context = new TestContext(conf);
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("/a/b/part-00", 1000, new byte[0]),
        new MockFile("/a/b/part-01", 1000, new byte[0]),
        new MockFile("/a/b/_part-02", 1000, new byte[0]),
        new MockFile("/a/b/.part-03", 1000, new byte[0]),
        new MockFile("/a/b/part-04", 1000, new byte[0]));
    OrcInputFormat.FileGenerator gen =
      new OrcInputFormat.FileGenerator(context, fs, new Path("/a/b"));
    gen.run();
    if (context.getErrors().size() > 0) {
      for(Throwable th: context.getErrors()) {
        System.out.println(StringUtils.stringifyException(th));
      }
      throw new IOException("Errors during file generation");
    }
    assertEquals(-1, context.getSchedulers());
    assertEquals(3, context.queue.size());
    assertEquals(new Path("/a/b/part-00"),
        ((OrcInputFormat.SplitGenerator) context.queue.get(0)).getPath());
    assertEquals(new Path("/a/b/part-01"),
        ((OrcInputFormat.SplitGenerator) context.queue.get(1)).getPath());
    assertEquals(new Path("/a/b/part-04"),
        ((OrcInputFormat.SplitGenerator) context.queue.get(2)).getPath());
  }

  static final Charset UTF8 = Charset.forName("UTF-8");

  static class MockBlock {
    int offset;
    int length;
    final String[] hosts;

    MockBlock(String... hosts) {
      this.hosts = hosts;
    }
  }

  static class MockFile {
    final Path path;
    final int blockSize;
    final int length;
    final MockBlock[] blocks;
    final byte[] content;

    MockFile(String path, int blockSize, byte[] content, MockBlock... blocks) {
      this.path = new Path(path);
      this.blockSize = blockSize;
      this.blocks = blocks;
      this.content = content;
      this.length = content.length;
      int offset = 0;
      for(MockBlock block: blocks) {
        block.offset = offset;
        block.length = Math.min(length - offset, blockSize);
        offset += block.length;
      }
    }
  }

  static class MockInputStream extends FSInputStream {
    final MockFile file;
    int offset = 0;

    public MockInputStream(MockFile file) throws IOException {
      this.file = file;
    }

    @Override
    public void seek(long offset) throws IOException {
      this.offset = (int) offset;
    }

    @Override
    public long getPos() throws IOException {
      return offset;
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
      return false;
    }

    @Override
    public int read() throws IOException {
      if (offset < file.length) {
        return file.content[offset++] & 0xff;
      }
      return -1;
    }
  }

  static class MockFileSystem extends FileSystem {
    final MockFile[] files;
    Path workingDir = new Path("/");

    MockFileSystem(Configuration conf, MockFile... files) {
      setConf(conf);
      this.files = files;
    }

    @Override
    public URI getUri() {
      try {
        return new URI("mock:///");
      } catch (URISyntaxException err) {
        throw new IllegalArgumentException("huh?", err);
      }
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
      for(MockFile file: files) {
        if (file.path.equals(path)) {
          return new FSDataInputStream(new MockInputStream(file));
        }
      }
      return null;
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission,
                                     boolean b, int i, short i2, long l,
                                     Progressable progressable
                                     ) throws IOException {
      throw new UnsupportedOperationException("no writes");
    }

    @Override
    public FSDataOutputStream append(Path path, int i,
                                     Progressable progressable
                                     ) throws IOException {
      throw new UnsupportedOperationException("no writes");
    }

    @Override
    public boolean rename(Path path, Path path2) throws IOException {
      return false;
    }

    @Override
    public boolean delete(Path path) throws IOException {
      return false;
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
      return false;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
      List<FileStatus> result = new ArrayList<FileStatus>();
      for(MockFile file: files) {
        if (file.path.getParent().equals(path)) {
          result.add(getFileStatus(file.path));
        }
      }
      return result.toArray(new FileStatus[result.size()]);
    }

    @Override
    public void setWorkingDirectory(Path path) {
      workingDir = path;
    }

    @Override
    public Path getWorkingDirectory() {
      return workingDir;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) {
      return false;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      for(MockFile file: files) {
        if (file.path.equals(path)) {
          return new FileStatus(file.length, false, 1, file.blockSize, 0, 0,
              FsPermission.createImmutable((short) 644), "owen", "group",
              file.path);
        }
      }
      return null;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus stat,
                                                 long start, long len) {
      List<BlockLocation> result = new ArrayList<BlockLocation>();
      for(MockFile file: files) {
        if (file.path.equals(stat.getPath())) {
          for(MockBlock block: file.blocks) {
            if (OrcInputFormat.SplitGenerator.getOverlap(block.offset,
                block.length, start, len) > 0) {
              result.add(new BlockLocation(block.hosts, block.hosts,
                  block.offset, block.length));
            }
          }
          return result.toArray(new BlockLocation[result.size()]);
        }
      }
      return new BlockLocation[0];
    }
  }

  static void fill(DataOutputBuffer out, long length) throws IOException {
    for(int i=0; i < length; ++i) {
      out.write(0);
    }
  }

  /**
   * Create the binary contents of an ORC file that just has enough information
   * to test the getInputSplits.
   * @param stripeLengths the length of each stripe
   * @return the bytes of the file
   * @throws IOException
   */
  static byte[] createMockOrcFile(long... stripeLengths) throws IOException {
    OrcProto.Footer.Builder footer = OrcProto.Footer.newBuilder();
    final long headerLen = 3;
    long offset = headerLen;
    DataOutputBuffer buffer = new DataOutputBuffer();
    for(long stripeLength: stripeLengths) {
      footer.addStripes(OrcProto.StripeInformation.newBuilder()
                          .setOffset(offset)
                          .setIndexLength(0)
                          .setDataLength(stripeLength-10)
                          .setFooterLength(10)
                          .setNumberOfRows(1000));
      offset += stripeLength;
    }
    fill(buffer, offset);
    footer.addTypes(OrcProto.Type.newBuilder()
                     .setKind(OrcProto.Type.Kind.STRUCT)
                     .addFieldNames("col1")
                     .addSubtypes(1));
    footer.addTypes(OrcProto.Type.newBuilder()
        .setKind(OrcProto.Type.Kind.STRING));
    footer.setNumberOfRows(1000 * stripeLengths.length)
          .setHeaderLength(headerLen)
          .setContentLength(offset - headerLen);
    footer.build().writeTo(buffer);
    int footerEnd = buffer.getLength();
    OrcProto.PostScript ps =
      OrcProto.PostScript.newBuilder()
        .setCompression(OrcProto.CompressionKind.NONE)
        .setFooterLength(footerEnd - offset)
        .setMagic("ORC")
        .build();
    ps.writeTo(buffer);
    buffer.write(buffer.getLength() - footerEnd);
    byte[] result = new byte[buffer.getLength()];
    System.arraycopy(buffer.getData(), 0, result, 0, buffer.getLength());
    return result;
  }

  @Test
  public void testAddSplit() throws Exception {
    // create a file with 5 blocks spread around the cluster
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("/a/file", 500,
          createMockOrcFile(197, 300, 600, 200, 200, 100, 100, 100, 100, 100),
          new MockBlock("host1-1", "host1-2", "host1-3"),
          new MockBlock("host2-1", "host0", "host2-3"),
          new MockBlock("host0", "host3-2", "host3-3"),
          new MockBlock("host4-1", "host4-2", "host4-3"),
          new MockBlock("host5-1", "host5-2", "host5-3")));
    OrcInputFormat.Context context = new OrcInputFormat.Context(conf);
    OrcInputFormat.SplitGenerator splitter =
        new OrcInputFormat.SplitGenerator(context, fs,
            fs.getFileStatus(new Path("/a/file")));
    splitter.createSplit(0, 200);
    FileSplit result = context.getResult(-1);
    assertEquals(0, result.getStart());
    assertEquals(200, result.getLength());
    assertEquals("/a/file", result.getPath().toString());
    String[] locs = result.getLocations();
    assertEquals(3, locs.length);
    assertEquals("host1-1", locs[0]);
    assertEquals("host1-2", locs[1]);
    assertEquals("host1-3", locs[2]);
    splitter.createSplit(500, 600);
    result = context.getResult(-1);
    locs = result.getLocations();
    assertEquals(3, locs.length);
    assertEquals("host2-1", locs[0]);
    assertEquals("host0", locs[1]);
    assertEquals("host2-3", locs[2]);
    splitter.createSplit(0, 2500);
    result = context.getResult(-1);
    locs = result.getLocations();
    assertEquals(1, locs.length);
    assertEquals("host0", locs[0]);
  }

  @Test
  public void testSplitGenerator() throws Exception {
    // create a file with 5 blocks spread around the cluster
    long[] stripeSizes =
        new long[]{197, 300, 600, 200, 200, 100, 100, 100, 100, 100};
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("/a/file", 500,
          createMockOrcFile(stripeSizes),
          new MockBlock("host1-1", "host1-2", "host1-3"),
          new MockBlock("host2-1", "host0", "host2-3"),
          new MockBlock("host0", "host3-2", "host3-3"),
          new MockBlock("host4-1", "host4-2", "host4-3"),
          new MockBlock("host5-1", "host5-2", "host5-3")));
    conf.setInt(OrcInputFormat.MAX_SPLIT_SIZE, 300);
    conf.setInt(OrcInputFormat.MIN_SPLIT_SIZE, 200);
    OrcInputFormat.Context context = new OrcInputFormat.Context(conf);
    OrcInputFormat.SplitGenerator splitter =
        new OrcInputFormat.SplitGenerator(context, fs,
            fs.getFileStatus(new Path("/a/file")));
    splitter.run();
    if (context.getErrors().size() > 0) {
      for(Throwable th: context.getErrors()) {
        System.out.println(StringUtils.stringifyException(th));
      }
      throw new IOException("Errors during splitting");
    }
    FileSplit result = context.getResult(0);
    assertEquals(3, result.getStart());
    assertEquals(497, result.getLength());
    result = context.getResult(1);
    assertEquals(500, result.getStart());
    assertEquals(600, result.getLength());
    result = context.getResult(2);
    assertEquals(1100, result.getStart());
    assertEquals(400, result.getLength());
    result = context.getResult(3);
    assertEquals(1500, result.getStart());
    assertEquals(300, result.getLength());
    result = context.getResult(4);
    assertEquals(1800, result.getStart());
    assertEquals(200, result.getLength());
    // test min = 0, max = 0 generates each stripe
    conf.setInt(OrcInputFormat.MIN_SPLIT_SIZE, 0);
    conf.setInt(OrcInputFormat.MAX_SPLIT_SIZE, 0);
    context = new OrcInputFormat.Context(conf);
    splitter = new OrcInputFormat.SplitGenerator(context, fs,
      fs.getFileStatus(new Path("/a/file")));
    splitter.run();
    if (context.getErrors().size() > 0) {
      for(Throwable th: context.getErrors()) {
        System.out.println(StringUtils.stringifyException(th));
      }
      throw new IOException("Errors during splitting");
    }
    for(int i=0; i < stripeSizes.length; ++i) {
      assertEquals("checking stripe " + i + " size",
          stripeSizes[i], context.getResult(i).getLength());
    }
  }

  @Test
  public void testInOutFormat() throws Exception {
    Properties properties = new Properties();
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    SerDe serde = new OrcSerde();
    HiveOutputFormat<?, ?> outFormat = new OrcOutputFormat();
    FSRecordWriter writer =
        outFormat.getHiveRecordWriter(conf, testFilePath, MyRow.class, true,
            properties, Reporter.NULL);
    writer.write(serde.serialize(new MyRow(1,2), inspector));
    writer.write(serde.serialize(new MyRow(2,2), inspector));
    writer.write(serde.serialize(new MyRow(3,2), inspector));
    writer.close(true);
    serde = new OrcSerde();
    properties.setProperty("columns", "x,y");
    properties.setProperty("columns.types", "int:int");
    serde.initialize(conf, properties);
    assertEquals(OrcSerde.OrcSerdeRow.class, serde.getSerializedClass());
    inspector = (StructObjectInspector) serde.getObjectInspector();
    assertEquals("struct<x:int,y:int>", inspector.getTypeName());
    InputFormat<?,?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(1, splits.length);

    // the the validate input method
    ArrayList<FileStatus> fileList = new ArrayList<FileStatus>();
    assertEquals(false,
        ((InputFormatChecker) in).validateInput(fs, new HiveConf(), fileList));
    fileList.add(fs.getFileStatus(testFilePath));
    assertEquals(true,
        ((InputFormatChecker) in).validateInput(fs, new HiveConf(), fileList));
    fileList.add(fs.getFileStatus(workDir));
    assertEquals(false,
        ((InputFormatChecker) in).validateInput(fs, new HiveConf(), fileList));


    // read the whole file
    org.apache.hadoop.mapred.RecordReader reader =
        in.getRecordReader(splits[0], conf, Reporter.NULL);
    Object key = reader.createKey();
    Writable value = (Writable) reader.createValue();
    int rowNum = 0;
    List<? extends StructField> fields =inspector.getAllStructFieldRefs();
    IntObjectInspector intInspector =
        (IntObjectInspector) fields.get(0).getFieldObjectInspector();
    assertEquals(0.0, reader.getProgress(), 0.00001);
    assertEquals(3, reader.getPos());
    while (reader.next(key, value)) {
      assertEquals(++rowNum, intInspector.get(inspector.
          getStructFieldData(serde.deserialize(value), fields.get(0))));
      assertEquals(2, intInspector.get(inspector.
          getStructFieldData(serde.deserialize(value), fields.get(1))));
    }
    assertEquals(3, rowNum);
    assertEquals(1.0, reader.getProgress(), 0.00001);
    reader.close();

    // read just the first column
    ColumnProjectionUtils.appendReadColumns(conf, Collections.singletonList(0));
    reader = in.getRecordReader(splits[0], conf, Reporter.NULL);
    key = reader.createKey();
    value = (Writable) reader.createValue();
    rowNum = 0;
    fields = inspector.getAllStructFieldRefs();
    while (reader.next(key, value)) {
      assertEquals(++rowNum, intInspector.get(inspector.
          getStructFieldData(value, fields.get(0))));
      assertEquals(null, inspector.getStructFieldData(value, fields.get(1)));
    }
    assertEquals(3, rowNum);
    reader.close();

    // test the mapping of empty string to all columns
    ColumnProjectionUtils.setReadAllColumns(conf);
    reader = in.getRecordReader(splits[0], conf, Reporter.NULL);
    key = reader.createKey();
    value = (Writable) reader.createValue();
    rowNum = 0;
    fields = inspector.getAllStructFieldRefs();
    while (reader.next(key, value)) {
      assertEquals(++rowNum, intInspector.get(inspector.
          getStructFieldData(value, fields.get(0))));
      assertEquals(2, intInspector.get(inspector.
          getStructFieldData(serde.deserialize(value), fields.get(1))));
    }
    assertEquals(3, rowNum);
    reader.close();
  }

  static class NestedRow implements Writable {
    int z;
    MyRow r;
    NestedRow(int x, int y, int z) {
      this.z = z;
      this.r = new MyRow(x,y);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      throw new UnsupportedOperationException("unsupported");
    }
  }

  @Test
  public void testMROutput() throws Exception {
    JobConf job = new JobConf(conf);
    Properties properties = new Properties();
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(NestedRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    SerDe serde = new OrcSerde();
    OutputFormat<?, ?> outFormat = new OrcOutputFormat();
    RecordWriter writer =
        outFormat.getRecordWriter(fs, conf, testFilePath.toString(),
            Reporter.NULL);
    writer.write(NullWritable.get(),
        serde.serialize(new NestedRow(1,2,3), inspector));
    writer.write(NullWritable.get(),
        serde.serialize(new NestedRow(4,5,6), inspector));
    writer.write(NullWritable.get(),
        serde.serialize(new NestedRow(7,8,9), inspector));
    writer.close(Reporter.NULL);
    serde = new OrcSerde();
    properties.setProperty("columns", "z,r");
    properties.setProperty("columns.types", "int:struct<x:int,y:int>");
    serde.initialize(conf, properties);
    inspector = (StructObjectInspector) serde.getObjectInspector();
    InputFormat<?,?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(1, splits.length);
    ColumnProjectionUtils.appendReadColumns(conf, Collections.singletonList(1));
    org.apache.hadoop.mapred.RecordReader reader =
        in.getRecordReader(splits[0], conf, Reporter.NULL);
    Object key = reader.createKey();
    Object value = reader.createValue();
    int rowNum = 0;
    List<? extends StructField> fields = inspector.getAllStructFieldRefs();
    StructObjectInspector inner = (StructObjectInspector)
        fields.get(1).getFieldObjectInspector();
    List<? extends StructField> inFields = inner.getAllStructFieldRefs();
    IntObjectInspector intInspector =
        (IntObjectInspector) fields.get(0).getFieldObjectInspector();
    while (reader.next(key, value)) {
      assertEquals(null, inspector.getStructFieldData(value, fields.get(0)));
      Object sub = inspector.getStructFieldData(value, fields.get(1));
      assertEquals(3*rowNum+1, intInspector.get(inner.getStructFieldData(sub,
          inFields.get(0))));
      assertEquals(3*rowNum+2, intInspector.get(inner.getStructFieldData(sub,
          inFields.get(1))));
      rowNum += 1;
    }
    assertEquals(3, rowNum);
    reader.close();

  }

  @Test
  public void testEmptyFile() throws Exception {
    JobConf job = new JobConf(conf);
    Properties properties = new Properties();
    HiveOutputFormat<?, ?> outFormat = new OrcOutputFormat();
    FSRecordWriter writer =
        outFormat.getHiveRecordWriter(conf, testFilePath, MyRow.class, true,
            properties, Reporter.NULL);
    writer.close(true);
    properties.setProperty("columns", "x,y");
    properties.setProperty("columns.types", "int:int");
    SerDe serde = new OrcSerde();
    serde.initialize(conf, properties);
    InputFormat<?,?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(0, splits.length);
    assertEquals(null, serde.getSerDeStats());
  }

  static class StringRow implements Writable {
    String str;
    String str2;
    StringRow(String s) {
      str = s;
      str2 = s;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
      throw new UnsupportedOperationException("no write");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      throw new UnsupportedOperationException("no read");
    }
  }

  @Test
  public void testDefaultTypes() throws Exception {
    JobConf job = new JobConf(conf);
    Properties properties = new Properties();
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(StringRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    SerDe serde = new OrcSerde();
    HiveOutputFormat<?, ?> outFormat = new OrcOutputFormat();
    FSRecordWriter writer =
        outFormat.getHiveRecordWriter(conf, testFilePath, StringRow.class,
            true, properties, Reporter.NULL);
    writer.write(serde.serialize(new StringRow("owen"), inspector));
    writer.write(serde.serialize(new StringRow("beth"), inspector));
    writer.write(serde.serialize(new StringRow("laurel"), inspector));
    writer.write(serde.serialize(new StringRow("hazen"), inspector));
    writer.write(serde.serialize(new StringRow("colin"), inspector));
    writer.write(serde.serialize(new StringRow("miles"), inspector));
    writer.close(true);
    serde = new OrcSerde();
    properties.setProperty("columns", "str,str2");
    serde.initialize(conf, properties);
    inspector = (StructObjectInspector) serde.getObjectInspector();
    assertEquals("struct<str:string,str2:string>", inspector.getTypeName());
    InputFormat<?,?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(1, splits.length);

    // read the whole file
    org.apache.hadoop.mapred.RecordReader reader =
        in.getRecordReader(splits[0], conf, Reporter.NULL);
    Object key = reader.createKey();
    Writable value = (Writable) reader.createValue();
    List<? extends StructField> fields =inspector.getAllStructFieldRefs();
    StringObjectInspector strInspector = (StringObjectInspector)
        fields.get(0).getFieldObjectInspector();
    assertEquals(true, reader.next(key, value));
    assertEquals("owen", strInspector.getPrimitiveJavaObject(inspector.
        getStructFieldData(value, fields.get(0))));
    assertEquals(true, reader.next(key, value));
    assertEquals("beth", strInspector.getPrimitiveJavaObject(inspector.
        getStructFieldData(value, fields.get(0))));
    assertEquals(true, reader.next(key, value));
    assertEquals("laurel", strInspector.getPrimitiveJavaObject(inspector.
        getStructFieldData(value, fields.get(0))));
    assertEquals(true, reader.next(key, value));
    assertEquals("hazen", strInspector.getPrimitiveJavaObject(inspector.
        getStructFieldData(value, fields.get(0))));
    assertEquals(true, reader.next(key, value));
    assertEquals("colin", strInspector.getPrimitiveJavaObject(inspector.
        getStructFieldData(value, fields.get(0))));
    assertEquals(true, reader.next(key, value));
    assertEquals("miles", strInspector.getPrimitiveJavaObject(inspector.
        getStructFieldData(value, fields.get(0))));
    assertEquals(false, reader.next(key, value));
    reader.close();
  }
}
