/**
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
package org.apache.hadoop.hive.ql.io.parquet.serde.primitive;

import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.io.IntWritable;

/**
 * The ParquetShortInspector can inspect both ShortWritables and IntWritables into shorts.
 *
 */
public class ParquetShortInspector extends AbstractPrimitiveJavaObjectInspector implements SettableShortObjectInspector {

  ParquetShortInspector() {
    super(TypeInfoFactory.shortTypeInfo);
  }

  @Override
  public Object getPrimitiveWritableObject(final Object o) {
    return o == null ? null : new ShortWritable(get(o));
  }

  @Override
  public Object getPrimitiveJavaObject(final Object o) {
    return o == null ? null : get(o);
  }

  @Override
  public Object create(final short val) {
    return new ShortWritable(val);
  }

  @Override
  public Object set(final Object o, final short val) {
    ((ShortWritable) o).set(val);
    return o;
  }

  @Override
  public short get(Object o) {
    // Accept int writables and convert them.
    if (o instanceof IntWritable) {
      return (short) ((IntWritable) o).get();
    }

    return ((ShortWritable) o).get();
  }
}
