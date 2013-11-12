package org.apache.hadoop.hive.serde2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.HiveStringUtils;

public abstract class HiveBaseCharWritable {
  protected Text value = new Text();

  public HiveBaseCharWritable() {
  }

  public int getCharacterLength() {
    return HiveStringUtils.getTextUtfLength(value);
  }

  /**
   * Access to the internal Text member. Use with care.
   * @return
   */
  public Text getTextValue() {
    return value;
  }

  public void readFields(DataInput in) throws IOException {
    value.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    value.write(out);
  }

  public boolean equals(Object obj) {
    if (obj == null || (obj.getClass() != this.getClass())) {
      return false;
    }
    return value.equals(((HiveBaseCharWritable)obj).value);
  }

  public int hashCode() {
    return value.hashCode();
  }
}
