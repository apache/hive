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

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class TestAbstractParquetMapInspector extends TestCase {

  class TestableAbstractParquetMapInspector extends AbstractParquetMapInspector {

    public TestableAbstractParquetMapInspector(ObjectInspector keyInspector, ObjectInspector valueInspector) {
      super(keyInspector, valueInspector);
    }

    @Override
    public Object getMapValueElement(Object o, Object o1) {
      throw new UnsupportedOperationException("Should not be called");
    }
  }
  private TestableAbstractParquetMapInspector inspector;

  @Override
  public void setUp() {
    inspector = new TestableAbstractParquetMapInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            PrimitiveObjectInspectorFactory.javaIntObjectInspector);
  }

  @Test
  public void testNullMap() {
    assertEquals("Wrong size", -1, inspector.getMapSize(null));
    assertNull("Should be null", inspector.getMap(null));
  }

  @Test
  public void testNullContainer() {
    final ArrayWritable map = new ArrayWritable(ArrayWritable.class, null);
    assertEquals("Wrong size", -1, inspector.getMapSize(map));
    assertNull("Should be null", inspector.getMap(map));
  }

  @Test
  public void testEmptyContainer() {
    final ArrayWritable map = new ArrayWritable(ArrayWritable.class, new ArrayWritable[0]);
    assertEquals("Wrong size", 0, inspector.getMapSize(map));
    assertNotNull("Should not be null", inspector.getMap(map));
  }

  @Test
  public void testRegularMap() {
    final Writable[] entry1 = new Writable[]{new IntWritable(0), new IntWritable(1)};
    final Writable[] entry2 = new Writable[]{new IntWritable(2), new IntWritable(3)};

    final ArrayWritable map = new ArrayWritable(ArrayWritable.class, new Writable[]{
      new ArrayWritable(Writable.class, entry1), new ArrayWritable(Writable.class, entry2)});

    final Map<Writable, Writable> expected = new HashMap<Writable, Writable>();
    expected.put(new IntWritable(0), new IntWritable(1));
    expected.put(new IntWritable(2), new IntWritable(3));

    assertEquals("Wrong size", 2, inspector.getMapSize(map));
    assertEquals("Wrong result of inspection", expected, inspector.getMap(map));
  }

  @Test
  public void testHashMap() {
    final Map<Writable, Writable> map = new HashMap<Writable, Writable>();
    map.put(new IntWritable(0), new IntWritable(1));
    map.put(new IntWritable(2), new IntWritable(3));
    map.put(new IntWritable(4), new IntWritable(5));
    map.put(new IntWritable(6), new IntWritable(7));

    assertEquals("Wrong size", 4, inspector.getMapSize(map));
    assertEquals("Wrong result of inspection", map, inspector.getMap(map));
  }
}
