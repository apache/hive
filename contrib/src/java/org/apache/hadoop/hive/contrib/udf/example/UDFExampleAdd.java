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
package org.apache.hadoop.hive.contrib.udf.example;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UDFExampleAdd.
 *
 */
@Description(name = "example_add", value = "_FUNC_(expr) - Example UDAF that returns the sum")
public class UDFExampleAdd extends UDF {

  public Integer evaluate(Integer... a) {
    int total = 0;
    for (Integer element : a) {
      if (element != null) {
        total += element;
      }
    }
    return total;
  }

  public Double evaluate(Double... a) {
    double total = 0;
    for (Double element : a) {
      if (element != null) {
        total += element;
      }
    }
    return total;
  }

}
