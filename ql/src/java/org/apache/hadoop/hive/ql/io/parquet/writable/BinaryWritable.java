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
package org.apache.hadoop.hive.ql.io.parquet.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import parquet.io.api.Binary;

/**
 *
 * A Wrapper to support constructor with Binary and String
 *
 * TODO : remove it, and call BytesWritable with the getBytes() in HIVE-6366
 *
 */
public class BinaryWritable implements Writable {

  private Binary binary;

  public BinaryWritable(final Binary binary) {
    this.binary = binary;
  }

  public Binary getBinary() {
    return binary;
  }

  public byte[] getBytes() {
    return binary.getBytes();
  }

  public String getString() {
    return binary.toStringUsingUTF8();
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    byte[] bytes = new byte[input.readInt()];
    input.readFully(bytes);
    binary = Binary.fromByteArray(bytes);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(binary.length());
    binary.writeTo(output);
  }

  @Override
  public int hashCode() {
    return binary == null ? 0 : binary.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BinaryWritable) {
      final BinaryWritable other = (BinaryWritable)obj;
      return binary.equals(other.binary);
    }
    return false;
  }

  public static class DicBinaryWritable extends BinaryWritable {

    private final String string;

    public DicBinaryWritable(Binary binary, String string) {
      super(binary);
      this.string = string;
    }

    @Override
    public String getString() {
      return string;
    }
  }

}
