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

import org.apache.hadoop.hive.ql.exec.UDF;


public class UDFSubstr extends UDF {

  public UDFSubstr() {
  }

  public String evaluate(String s, Integer pos, Integer len)  {
    int start, end;

    if ((s == null) || (pos == null) || (len == null))
      return null;
    if ((len <= 0) || (Math.abs(pos) > s.length()))
      return "";

    if (pos > 0)
      start = pos - 1;
    else if (pos < 0)
      start = s.length() + pos;
    else
      start = 0;

    if ((s.length() - start) < len)
      end = s.length();
    else
      end = start + len;

    return s.substring(start, end);
  }

  public String evaluate(String s, Integer pos)  {
    return evaluate(s, pos, Integer.MAX_VALUE);
  }

}
