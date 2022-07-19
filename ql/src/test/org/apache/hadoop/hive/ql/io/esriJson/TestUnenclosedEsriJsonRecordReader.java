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
package org.apache.hadoop.hive.ql.io.esriJson;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class TestUnenclosedEsriJsonRecordReader {  // MRv2

  private TaskAttemptContext createTaskAttemptContext(Configuration conf, TaskAttemptID taid)
      throws Exception {       //shim
    try {                     // Hadoop-1
      return TaskAttemptContext.class.
          getConstructor(Configuration.class, TaskAttemptID.class).
          newInstance(conf, taid);
    } catch (Exception e) {   // Hadoop-2
      Class<?> clazz = Class.forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
      return (TaskAttemptContext) clazz.getConstructor(Configuration.class, TaskAttemptID.class).
          newInstance(conf, taid);
    }
  }

  private UnenclosedEsriJsonRecordReader getReader() throws IOException {
    return new UnenclosedEsriJsonRecordReader();
  }

  int[] getRecordIndexesInFile(String resource, int start, int end) throws Exception {
    return getRecordIndexesInFile(getReader(), resource, start, end);
  }

  int[] getRecordIndexesInFile(String resource, int start, int end, boolean flag) throws Exception {
    return getRecordIndexesInFile(getReader(), resource, start, end, flag);
  }

  int[] getRecordIndexesInFile(UnenclosedEsriJsonRecordReader reader, String resource, int start, int end)
      throws Exception {
    return getRecordIndexesInFile(reader, resource, start, end, false);
  }

  int[] getRecordIndexesInFile(UnenclosedEsriJsonRecordReader reader, String resource, int start, int end, boolean flag)
      throws Exception {
    Path path = new Path(this.getClass().getResource("/json/" + resource).getFile());
    FileSplit split = new FileSplit(path, start, end - start, new String[0]);
    try {
      TaskAttemptContext tac = createTaskAttemptContext(new Configuration(), new TaskAttemptID());
      reader.initialize(split, tac);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
    List<Integer> linesList = new LinkedList<Integer>();

    LongWritable key = null;
    Text value = null;

    try {
      while (reader.nextKeyValue()) {
        key = reader.getCurrentKey();
        value = reader.getCurrentValue();
        int line = flag ? (int) (key.get()) : value.toString().charAt(23) - '0';
        linesList.add(line);
        //System.out.println(key.get() + " - " + value.toString());
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }

    int[] lines = new int[linesList.size()];
    for (int i = 0; i < linesList.size(); i++) {
      lines[i] = linesList.get(i);
    }
    return lines;
  }

  @Test
  public void TestArbitrarySplitLocations() throws Exception {

    //int totalSize = 415;

    //int [] recordBreaks = new int[] { 0, 40, 80, 120, 160, 200, 240, 280, 320, 372 };

    Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile("unenclosed-json-simple.json", 0, 40));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-simple.json", 0, 41));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-simple.json", 0, 42));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-simple.json", 39, 123));

    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-simple.json", 20, 123));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-simple.json", 40, 123));
    Assert.assertArrayEquals(new int[] { 2, 3 }, getRecordIndexesInFile("unenclosed-json-simple.json", 41, 123));
    Assert.assertArrayEquals(new int[] { 6, 7, 8 }, getRecordIndexesInFile("unenclosed-json-simple.json", 240, 340));
    Assert.assertArrayEquals(new int[] { 9 }, getRecordIndexesInFile("unenclosed-json-simple.json", 353, 415));
    Assert.assertArrayEquals(new int[] { 9 }, getRecordIndexesInFile("unenclosed-json-simple.json", 354, 415));
    Assert.assertArrayEquals(new int[] { 9 }, getRecordIndexesInFile("unenclosed-json-simple.json", 355, 415));
  }

  @Test
  public void TestEachOnce() throws Exception {
    //Each record exactly once - see commit b8f6d6dfaf11cce7d8cba54e6011e8684ade0e85, issue #68
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-simple.json", 0, 63));
    Assert.assertArrayEquals(new int[] { 2, 3 }, getRecordIndexesInFile("unenclosed-json-simple.json", 63, 121));
    Assert.assertArrayEquals(new int[] { 4 }, getRecordIndexesInFile("unenclosed-json-simple.json", 121, 187));
    Assert.assertArrayEquals(new int[] { 5, 6 }, getRecordIndexesInFile("unenclosed-json-simple.json", 187, 264));
    Assert.assertArrayEquals(new int[] { 7, 8 }, getRecordIndexesInFile("unenclosed-json-simple.json", 264, 352));
    Assert.assertArrayEquals(new int[] { 9 }, getRecordIndexesInFile("unenclosed-json-simple.json", 352, 412));

    Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile("unenclosed-json-simple.json", 0, 23));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-json-simple.json", 23, 41));
    // Assert.assertArrayEquals(new int[] { 2, 3 }, getRecordIndexesInFile("unenclosed-json-simple.json", 41, 123));
  }

  @Test
  public void TestWhitespace() throws Exception {
    //int [] recordBreaks = new int[] { 0, 57, 111, ,  };
    int[] rslt = getRecordIndexesInFile("unenclosed-json-return.json", 0, 222, true);
    Assert.assertEquals(4, rslt.length);
    int[] before = null, after = null;
    before = getRecordIndexesInFile("unenclosed-json-return.json", 0, 56, true);
    after = getRecordIndexesInFile("unenclosed-json-return.json", 56, 222, true);
    Assert.assertEquals(4, before.length + after.length);
    before = getRecordIndexesInFile("unenclosed-json-return.json", 0, 57, true);
    after = getRecordIndexesInFile("unenclosed-json-return.json", 57, 222, true);
    Assert.assertEquals(4, before.length + after.length);
    before = getRecordIndexesInFile("unenclosed-json-return.json", 0, 58, true);
    after = getRecordIndexesInFile("unenclosed-json-return.json", 58, 222, true);
    Assert.assertEquals(4, before.length + after.length);
  }

  @Ignore  // May not be guaranteed behavior
  public void TestComma() throws Exception {
    //int [] recordBreaks = new int[] { 0, 57, 111, ,  };
    int[] rslt = getRecordIndexesInFile("unenclosed-json-comma.json", 0, 222, true);
    Assert.assertEquals(4, rslt.length);
    int[] before = null, after = null;
    before = getRecordIndexesInFile("unenclosed-json-comma.json", 0, 56, true);
    after = getRecordIndexesInFile("unenclosed-json-comma.json", 56, 222, true);
    Assert.assertEquals(4, before.length + after.length);
    before = getRecordIndexesInFile("unenclosed-json-comma.json", 0, 57, true);
    after = getRecordIndexesInFile("unenclosed-json-comma.json", 57, 222, true);
    Assert.assertEquals(4, before.length + after.length);
    before = getRecordIndexesInFile("unenclosed-json-comma.json", 0, 58, true);
    after = getRecordIndexesInFile("unenclosed-json-comma.json", 58, 222, true);
    Assert.assertEquals(4, before.length + after.length);
  }

  @Test
  public void TestAttrNamedAttributes() throws Exception {
    //int [] recordBreaks = new int[] { 0, 57, 111, ,  };
    int[] rslt = getRecordIndexesInFile("unenclosed-json-attrs.json", 0, 225, true);
    Assert.assertEquals(5, rslt.length);
    int[] before = null, after = null;
    before = getRecordIndexesInFile("unenclosed-json-attrs.json", 0, 59, true);
    after = getRecordIndexesInFile("unenclosed-json-attrs.json", 59, 225, true);
    Assert.assertEquals(5, before.length + after.length);
    before = getRecordIndexesInFile("unenclosed-json-attrs.json", 0, 88, true);
    after = getRecordIndexesInFile("unenclosed-json-attrs.json", 88, 222, true);
    Assert.assertEquals(5, before.length + after.length);
    before = getRecordIndexesInFile("unenclosed-json-attrs.json", 0, 102, true);
    after = getRecordIndexesInFile("unenclosed-json-attrs.json", 102, 222, true);
    Assert.assertEquals(5, before.length + after.length);
  }

  @Test
  public void TestEscape() throws Exception {  // Issue #68
    //int [] recordBreaks = new int[] { 0, 44, 88, 137, 181, 229, 270, 311, 354 };  //length 395
    Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile("unenclosed-json-escape.json", 0, 44));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-escape.json", 0, 45));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-escape.json", 0, 46));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-escape.json", 43, 140));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-escape.json", 19, 140));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-escape.json", 44, 140));
    Assert.assertArrayEquals(new int[] { 2, 3 }, getRecordIndexesInFile("unenclosed-json-escape.json", 45, 140));
    Assert.assertArrayEquals(new int[] { 4, 5, 6 }, getRecordIndexesInFile("unenclosed-json-escape.json", 181, 289));
    Assert
        .assertArrayEquals(new int[] { 8 }, getRecordIndexesInFile("unenclosed-json-escape.json", 336, 400));  // 7|{}"
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-json-escape.json", 14, 45));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-json-escape.json", 22, 45));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-json-escape.json", 23, 45));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-json-escape.json", 24, 45));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-json-escape.json", 25, 45));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-json-escape.json", 44, 68));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-json-escape.json", 44, 69));
  }

  @Test
  public void TestEscQuoteLast() throws Exception {
    //int [] recordBreaks = new int[] { 0, 75, 146, 218, 290, 362, , ,  };
    Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile("unenclosed-json-esc1.json", 0, 44));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-esc1.json", 0, 45));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-esc1.json", 0, 46));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc1.json", 19, 140));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc1.json", 25, 140));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc1.json", 44, 140));
  }

  @Test
  public void TestEscAposLast() throws Exception {
    //int [] recordBreaks = new int[] { 0, 75, 146, 218, 290, 362, , ,  };
    Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile("unenclosed-json-esc2.json", 0, 44));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-esc2.json", 0, 45));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-esc2.json", 0, 46));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc2.json", 19, 140));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc2.json", 26, 140));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc2.json", 43, 140));
  }

  @Test
  public void TestEscSlashLast() throws Exception {
    //int [] recordBreaks = new int[] { 0, 75, 146, 218, 290, 362, , ,  };
    Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile("unenclosed-json-esc3.json", 0, 44));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-esc3.json", 0, 45));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-esc3.json", 0, 46));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc3.json", 19, 140));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc3.json", 26, 140));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc3.json", 44, 140));
  }

  @Test
  public void TestEscCloseLast() throws Exception {
    //int [] recordBreaks = new int[] { 0, 75, 146, 218, 290, 362, , ,  };
    Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile("unenclosed-json-esc4.json", 0, 44));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-esc4.json", 0, 45));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-esc4.json", 0, 46));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc4.json", 19, 140));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc4.json", 25, 140));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc4.json", 44, 140));
  }

  @Test
  public void TestEscOpenLast() throws Exception {
    //int [] recordBreaks = new int[] { 0, 75, 146, 218, 290, 362, , ,  };
    Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile("unenclosed-json-esc5.json", 0, 44));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-esc5.json", 0, 45));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-json-esc5.json", 0, 46));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc5.json", 19, 140));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc5.json", 26, 140));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-json-esc5.json", 44, 140));
    Assert.assertArrayEquals(new int[] { 6 }, getRecordIndexesInFile("unenclosed-json-esc5.json", 268, 280));
  }

  @Test
  public void TestEscPoints() throws Exception {
    //int [] recordBreaks = new int[] { 0, 75, 146, 218, 290, 362, , ,  };
    Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile("unenclosed-json-esc-points.json", 0, 74, true));
    Assert
        .assertArrayEquals(new int[] { 0, 75 }, getRecordIndexesInFile("unenclosed-json-esc-points.json", 0, 76, true));
    Assert.assertArrayEquals(new int[] { 75, 146 },
        getRecordIndexesInFile("unenclosed-json-esc-points.json", 70, 148, true));
  }

  // This tests some multi-byte characters in UTF-8.
  // If implementing a byte-based approach instead of character-based,
  // the test itself would probably have to be updated to byte-based offsets
  // See issue #75
  @Ignore
  public void TestCharacters() throws Exception {
    //int[] recordBreaks = new int[] { 0, 42, 84, 126, 168, 210, ...};  // character-based offsets
    Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile("unenclosed-json-chars.json", 0, 42, true));
    Assert.assertArrayEquals(new int[] { 0, 42 }, getRecordIndexesInFile("unenclosed-json-chars.json", 0, 43, true));
    Assert.assertArrayEquals(new int[] { 42 }, getRecordIndexesInFile("unenclosed-json-chars.json", 38, 43, true));
    Assert.assertArrayEquals(new int[] { 42 }, getRecordIndexesInFile("unenclosed-json-chars.json", 39, 43, true));
    Assert.assertArrayEquals(new int[] { 84, 126, 168 },
        getRecordIndexesInFile("unenclosed-json-chars.json", 43, 200, true));
    Assert.assertArrayEquals(new int[] { 210, 252, 294, 336 },
        getRecordIndexesInFile("unenclosed-json-chars.json", 200, 400, true));
  }

  @Test
  public void TestGeomFirst() throws Exception {
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-json-geom-first.json", 32, 54));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-json-geom-first.json", 48, 54));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-json-geom-first.json", 49, 54));
    Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile("unenclosed-json-geom-first.json", 0, 52, true));
  }


	/* *
	 * @deprecated superseded by UnenclosedEsriJsonRecordReader
	@Deprecated in v1.2 -> Obsolete
	@Test
	public void TestLegacyName() throws Exception {
		UnenclosedEsriJsonRecordReader uejrr =  new UnenclosedJsonRecordReader();
		Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile(uejrr, "unenclosed-json-simple.json", 0, 63));
		Assert.assertArrayEquals(new int[] { 2, 3 }, getRecordIndexesInFile(uejrr, "unenclosed-json-simple.json", 63, 121));
		Assert.assertArrayEquals(new int[] { 4 }, getRecordIndexesInFile(uejrr, "unenclosed-json-simple.json", 121, 187));
		Assert.assertArrayEquals(new int[] { 5, 6 }, getRecordIndexesInFile(uejrr, "unenclosed-json-simple.json", 187, 264));
		Assert.assertArrayEquals(new int[] { 7, 8 }, getRecordIndexesInFile(uejrr, "unenclosed-json-simple.json", 264, 352));
		Assert.assertArrayEquals(new int[] { 9 }, getRecordIndexesInFile(uejrr, "unenclosed-json-simple.json", 352, 412));

		Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile(uejrr, "unenclosed-json-simple.json", 0, 23));
		Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile(uejrr, "unenclosed-json-simple.json", 23, 41));
	}
    * */
}
