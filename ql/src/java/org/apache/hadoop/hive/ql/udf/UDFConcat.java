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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFConcat.
 *
 */
@Description(name = "concat",
    value = "_FUNC_(str1, str2, ... strN) - returns the concatenation of str1, str2, ... strN or "+
            "_FUNC_(bin1, bin2, ... binN) - returns the concatenation of bytes in binary data " +
            " bin1, bin2, ... binN",
    extended = "Returns NULL if any argument is NULL.\n"
    + "Example:\n"
    + "  > SELECT _FUNC_('abc', 'def') FROM src LIMIT 1;\n"
    + "  'abcdef'")
public class UDFConcat extends UDF {

  public UDFConcat() {
  }

  private final Text text = new Text();

  public Text evaluate(Text... args) {
    text.clear();
    for (Text arg : args) {
      if (arg == null) {
        return null;
      }
      text.append(arg.getBytes(), 0, arg.getLength());
    }
    return text;
  }

  public BytesWritable evaluate(BytesWritable... bw){

    int len = 0;
    for(BytesWritable bytes : bw){
      if (bytes == null){
        return null;
}
      len += bytes.getLength();
    }

    byte[] out = new byte[len];
    int curLen = 0;
    // Need to iterate twice since BytesWritable doesn't support append.
    for (BytesWritable bytes : bw){
      System.arraycopy(bytes.getBytes(), 0, out, curLen, bytes.getLength());
      curLen += bytes.getLength();
    }
    return new BytesWritable(out);
  }
}
