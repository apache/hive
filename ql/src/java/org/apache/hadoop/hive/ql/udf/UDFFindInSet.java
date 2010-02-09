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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFFindInSet.
 *
 */
@Description(name = "find_in_set", value = "_FUNC_(str,str_array) - Returns the first occurrence "
    + " of str in str_array where str_array is a comma-delimited string."
    + " Returns null if either argument is null."
    + " Returns 0 if the first argument has any commas.", extended = "Example:\n"
    + "  > SELECT _FUNC_('ab','abc,b,ab,c,def') FROM src LIMIT 1;\n"
    + "  3\n"
    + "  > SELECT * FROM src1 WHERE NOT _FUNC_(key,'311,128,345,956')=0;\n"
    + "  311  val_311\n" + "  128"

)
public class UDFFindInSet extends UDF {
  private final IntWritable result = new IntWritable();

  public IntWritable evaluate(Text s, Text txtarray) {
    if (s == null || txtarray == null) {
      return null;
    }

    byte[] search_bytes = s.getBytes();

    for (int i = 0; i < s.getLength(); i++) {
      if (search_bytes[i] == ',') {
        result.set(0);
        return result;
      }

    }

    byte[] data = txtarray.getBytes();
    int search_length = s.getLength();

    int cur_pos_in_array = 0;
    int cur_length = 0;
    boolean matching = true;

    for (int i = 0; i < txtarray.getLength(); i++) {
      if (data[i] == ',') {
        cur_pos_in_array++;
        if (matching && cur_length == search_length) {
          result.set(cur_pos_in_array);
          return result;
        } else {
          matching = true;
          cur_length = 0;
        }
      } else {
        if (cur_length + 1 <= search_length) {
          if (!matching || search_bytes[cur_length] != data[i]) {
            matching = false;
          }
        } else {
          matching = false;
        }
        cur_length++;
      }

    }

    if (matching && cur_length == search_length) {
      cur_pos_in_array++;
      result.set(cur_pos_in_array);
      return result;
    } else {
      result.set(0);
      return result;
    }
  }

}
