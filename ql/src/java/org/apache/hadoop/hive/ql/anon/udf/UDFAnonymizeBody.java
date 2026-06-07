/*
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

package org.apache.hadoop.hive.ql.anon.udf;

import org.apache.hadoop.hive.ql.anon.convert.JsonBodyConverter;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

@Description(name = "anonymize_body",
    value = "_FUNC_(body, subjectId) - return the body with erasure rules "
          + "applied when its userId matches subjectId, otherwise unchanged",
    extended = "Mirrors the per-row work that an ERASE FROM TABLE would do "
             + "via the swap path, so a Hive UPDATE can be compared like-for-like.")
public class UDFAnonymizeBody extends UDF {

  private static final List<String> EMPTY_IP_LIST = new ArrayList<>();

  private final Text result = new Text();

  public Text evaluate(final Text body, final IntWritable subjectId) {
    if (body == null || subjectId == null) {
      return body;
    }
    final Msg3 msg = JsonBodyConverter.convert(body.toString(), Msg3.class);
    if (msg.getUserId() != subjectId.get()) {
      return body;
    }
    msg.setCountry("");
    msg.setCity("");
    msg.setTelephone("");
    msg.setIpList(EMPTY_IP_LIST);
    result.set(JsonBodyConverter.serializeMsg(msg));
    return result;
  }
}
