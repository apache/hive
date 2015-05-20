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
import static junit.framework.Assert.assertNotNull;

import java.io.File;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hive.common.util.HiveTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.Lists;

/**
 *
 */
@RunWith(Parameterized.class)
public class TestOrcTimezone1 {
  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  String writerTimeZone;
  String readerTimeZone;
  static TimeZone defaultTimeZone = TimeZone.getDefault();

  public TestOrcTimezone1(String writerTZ, String readerTZ) {
    this.writerTimeZone = writerTZ;
    this.readerTimeZone = readerTZ;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> result = Arrays.asList(new Object[][]{
        /* Extreme timezones */
        {"GMT-12:00", "GMT+14:00"},
        /* No difference in DST */
        {"America/Los_Angeles", "America/Los_Angeles"}, /* same timezone both with DST */
        {"Europe/Berlin", "Europe/Berlin"}, /* same as above but europe */
        {"America/Phoenix", "Asia/Kolkata"} /* Writer no DST, Reader no DST */,
        {"Europe/Berlin", "America/Los_Angeles"} /* Writer DST, Reader DST */,
        {"Europe/Berlin", "America/Chicago"} /* Writer DST, Reader DST */,
        /* With DST difference */
        {"Europe/Berlin", "UTC"},
        {"UTC", "Europe/Berlin"} /* Writer no DST, Reader DST */,
        {"America/Los_Angeles", "Asia/Kolkata"} /* Writer DST, Reader no DST */,
        {"Europe/Berlin", "Asia/Kolkata"} /* Writer DST, Reader no DST */,
        /* Timezone offsets for the reader has changed historically */
        {"Asia/Saigon", "Pacific/Enderbury"},
        {"UTC", "Asia/Jerusalem"},

        // NOTE:
        // "1995-01-01 03:00:00.688888888" this is not a valid time in Pacific/Enderbury timezone.
        // On 1995-01-01 00:00:00 GMT offset moved from -11:00 hr to +13:00 which makes all values
        // on 1995-01-01 invalid. Try this with joda time
        // new MutableDateTime("1995-01-01", DateTimeZone.forTimeZone(readerTimeZone));
    });
    return result;
  }

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @After
  public void restoreTimeZone() {
    TimeZone.setDefault(defaultTimeZone);
  }

  @Test
  public void testTimestampWriter() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Timestamp.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    TimeZone.setDefault(TimeZone.getTimeZone(writerTimeZone));
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000).bufferSize(10000));
    assertEquals(writerTimeZone, TimeZone.getDefault().getID());
    List<String> ts = Lists.newArrayList();
    ts.add("2003-01-01 01:00:00.000000222");
    ts.add("1996-08-02 09:00:00.723100809");
    ts.add("1999-01-01 02:00:00.999999999");
    ts.add("1995-01-02 03:00:00.688888888");
    ts.add("2002-01-01 04:00:00.1");
    ts.add("2010-03-02 05:00:00.000009001");
    ts.add("2005-01-01 06:00:00.000002229");
    ts.add("2006-01-01 07:00:00.900203003");
    ts.add("2003-01-01 08:00:00.800000007");
    ts.add("1998-11-02 10:00:00.857340643");
    ts.add("2008-10-02 11:00:00.0");
    ts.add("2037-01-01 00:00:00.000999");
    ts.add("2014-03-28 00:00:00.0");
    for (String t : ts) {
      writer.addRow(Timestamp.valueOf(t));
    }
    writer.close();

    TimeZone.setDefault(TimeZone.getTimeZone(readerTimeZone));
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(readerTimeZone, TimeZone.getDefault().getID());
    RecordReader rows = reader.rows(null);
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Timestamp got = ((TimestampWritable) row).getTimestamp();
      assertEquals(ts.get(idx++), got.toString());
    }
    rows.close();
  }

  @Test
  public void testReadTimestampFormat_0_11() throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone(readerTimeZone));
    Path oldFilePath =
        new Path(HiveTestUtils.getFileFromClasspath("orc-file-11-format.orc"));
    Reader reader = OrcFile.createReader(oldFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    StructObjectInspector readerInspector = (StructObjectInspector) reader
        .getObjectInspector();
    List<? extends StructField> fields = readerInspector
        .getAllStructFieldRefs();
    TimestampObjectInspector tso = (TimestampObjectInspector) readerInspector
        .getStructFieldRef("ts").getFieldObjectInspector();
    
    RecordReader rows = reader.rows();
    Object row = rows.next(null);
    assertNotNull(row);
    assertEquals(Timestamp.valueOf("2000-03-12 15:00:00"),
        tso.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
            fields.get(12))));
    
    // check the contents of second row
    assertEquals(true, rows.hasNext());
    rows.seekToRow(7499);
    row = rows.next(null);
    assertEquals(Timestamp.valueOf("2000-03-12 15:00:01"),
        tso.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
            fields.get(12))));
    
    // handle the close up
    assertEquals(false, rows.hasNext());
    rows.close();
  }
}
