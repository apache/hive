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
package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods to handle integer overflow/underflows in a ColumnVector.
 */
public class OverflowUtils {

  private OverflowUtils() {
    //prevent instantiation
  }

  public static void accountForOverflowLong(TypeInfo outputTypeInfo, LongColumnVector v,
      boolean selectedInUse, int[] sel, int n) {
    if (outputTypeInfo == null) {
      //can't do much if outputTypeInfo is not set
      return;
    }
    switch (outputTypeInfo.getTypeName()) {
    case serdeConstants.TINYINT_TYPE_NAME:
      //byte
      if (v.isRepeating) {
        v.vector[0] = (byte) v.vector[0];
      } else if (selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          v.vector[i] = (byte) v.vector[i];
        }
      } else {
        for (int i = 0; i != n; i++) {
          v.vector[i] = (byte) v.vector[i];
        }
      }
      break;
    case serdeConstants.SMALLINT_TYPE_NAME:
      //short
      if (v.isRepeating) {
        v.vector[0] = (short) v.vector[0];
      } else if (selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          v.vector[i] = (short) v.vector[i];
        }
      } else {
        for (int i = 0; i != n; i++) {
          v.vector[i] = (short) v.vector[i];
        }
      }
      break;
    case serdeConstants.INT_TYPE_NAME:
      //int
      if (v.isRepeating) {
        v.vector[0] = (int) v.vector[0];
      } else if (selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          v.vector[i] = (int) v.vector[i];
        }
      } else {
        for (int i = 0; i != n; i++) {
          v.vector[i] = (int) v.vector[i];
        }
      }
      break;
    default:
      //nothing to be done
    }
  }

  public static void accountForOverflowDouble(TypeInfo outputTypeInfo, DoubleColumnVector v,
      boolean selectedInUse, int[] sel, int n) {
    if (outputTypeInfo == null) {
      //can't do much if outputTypeInfo is not set
      return;
    }
    switch (outputTypeInfo.getTypeName()) {
    case serdeConstants.FLOAT_TYPE_NAME:
      //float
      if (v.isRepeating) {
        v.vector[0] = (float) v.vector[0];
      } else if (selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          v.vector[i] = (float) v.vector[i];
        }
      } else {
        for (int i = 0; i != n; i++) {
          v.vector[i] = (float) v.vector[i];
        }
      }
      break;
    default:
      //nothing to be done
    }
  }
}
