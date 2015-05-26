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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class is a container of an array of objects of any type. It implements
 * the Writable interface so that it can bypass generic objects read from InputFormat
 * implementations up to Hive object inspectors. This class helps storage formats
 * to avoid Writable objects allocation unnecessary as the only Writable class needed
 * for map/reduce functions is this array of objects.
 *
 * This is the replacement for ArrayWritable class that contains only Writable objects.
 */
public class ObjectArrayWritable implements Writable {
  private Object[] values;

  public ObjectArrayWritable(final Object[] values) {
    this.values = values;
  }

  public Object[] get() {
    return values;
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
