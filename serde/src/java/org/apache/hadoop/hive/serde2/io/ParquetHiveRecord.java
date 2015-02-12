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
package org.apache.hadoop.hive.serde2.io;

import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class wraps the object and object inspector that will be used later
 * in DataWritableWriter class to get the object values.
 */
public class ParquetHiveRecord implements Writable {
  public Object value;
  public StructObjectInspector inspector;

  public ParquetHiveRecord() {
    this(null, null);
  }

  public ParquetHiveRecord(final Object o, final StructObjectInspector oi) {
    value = o;
    inspector = oi;
  }

  public StructObjectInspector getObjectInspector() {
    return inspector;
  }

  public Object getObject() {
    return value;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    throw new UnsupportedOperationException("Unsupported method call.");
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException("Unsupported method call.");
  }
}
