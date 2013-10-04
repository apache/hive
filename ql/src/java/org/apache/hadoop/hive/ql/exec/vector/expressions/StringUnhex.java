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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.expressions.StringUnaryUDF;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringUnaryUDF.IUDFUnaryString;
import org.apache.hadoop.hive.ql.udf.UDFUnhex;
import org.apache.hadoop.io.Text;

public class StringUnhex extends StringUnaryUDF {
  private static final long serialVersionUID = 1L;

  StringUnhex(int colNum, int outputColumn) {
    super(colNum, outputColumn, new IUDFUnaryString() {

      // Wrap the evaluate method of UDFUnhex to make it return the expected type, Text.
      @Override
      public Text evaluate(Text s) {
        final UDFUnhex unhex = new UDFUnhex();
        byte[] b = unhex.evaluate(s);
        if (b == null) {
          return null;
        }
        return new Text(b);
      }

    });
  }

  StringUnhex() {
    super();
  }
}
