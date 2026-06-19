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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Test;

public class TestUDFSign {

  @Test
  public void testDecimalSign() throws HiveException {
    UDFSign udf = new UDFSign();

    HiveDecimalWritable input = new HiveDecimalWritable(HiveDecimal.create("32300.004747"));
    IntWritable res = udf.evaluate(input);
    Assert.assertEquals(1, res.get());

    input = new HiveDecimalWritable(HiveDecimal.create("-30.047"));
    res = udf.evaluate(input);
    Assert.assertEquals(-1, res.get());

    input = new HiveDecimalWritable(HiveDecimal.ZERO);
    res = udf.evaluate(input);
    Assert.assertEquals(0, res.get());
  }

}
