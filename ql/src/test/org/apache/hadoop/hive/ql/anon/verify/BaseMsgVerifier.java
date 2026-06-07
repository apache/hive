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

package org.apache.hadoop.hive.ql.anon.verify;

import org.apache.hadoop.hive.ql.anon.model.BaseMsg;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Set;

public class BaseMsgVerifier implements Verifier {
  @Override
  public boolean verify(final Object obj, final Set<WritableComparable> keySet) {
    if (!(obj instanceof BaseMsg)) {
      return false;
    }

    if (obj instanceof Msg3) {
      final Msg3 msg3 = (Msg3) obj;
      final WritableComparable userId = new IntWritable(msg3.getUserId());
      if (!keySet.contains(userId)) {
        return true;
      }
      return msg3.getCity().isEmpty()
        && msg3.getCountry().isEmpty()
        && msg3.getTelephone().isEmpty();
    }

    return true;
  }
}
