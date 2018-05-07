/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.serde;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class TestParquetHiveArrayInspector extends TestCase {

  private ParquetHiveArrayInspector inspector;

  @Override
  public void setUp() {
    inspector = new ParquetHiveArrayInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
  }

  @Test
  public void testNullArray() {
    assertEquals("Wrong size", -1, inspector.getListLength(null));
    assertNull("Should be null", inspector.getList(null));
    assertNull("Should be null", inspector.getListElement(null, 0));
  }

  @Test
  public void testNullContainer() {
    final ArrayWritable list = new ArrayWritable(ArrayWritable.class, null);
    assertEquals("Wrong size", -1, inspector.getListLength(list));
    assertNull("Should be null", inspector.getList(list));
    assertNull("Should be null", inspector.getListElement(list, 0));
  }

  @Test
  public void testEmptyContainer() {
    final ArrayWritable list = new ArrayWritable(ArrayWritable.class, new ArrayWritable[0]);
    assertEquals("Wrong size", 0, inspector.getListLength(list));
    assertNotNull("Should not be null", inspector.getList(list));
    assertNull("Should be null", inspector.getListElement(list, 0));
  }

  @Test
  public void testRegularList() {
    final ArrayWritable list = new ArrayWritable(Writable.class,
            new Writable[]{new IntWritable(3), new IntWritable(5), new IntWritable(1)});

    final List<Writable> expected = new ArrayList<Writable>();
    expected.add(new IntWritable(3));
    expected.add(new IntWritable(5));
    expected.add(new IntWritable(1));

    assertEquals("Wrong size", 3, inspector.getListLength(list));
    assertEquals("Wrong result of inspection", expected, inspector.getList(list));

    for (int i = 0; i < expected.size(); ++i) {
      assertEquals("Wrong result of inspection", expected.get(i), inspector.getListElement(list, i));

    }

    assertNull("Should be null", inspector.getListElement(list, 3));
  }
}
