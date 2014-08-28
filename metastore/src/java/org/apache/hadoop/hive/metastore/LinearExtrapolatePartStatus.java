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

package org.apache.hadoop.hive.metastore;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Decimal;

public class LinearExtrapolatePartStatus implements IExtrapolatePartStatus {

  @Override
  public Object extrapolate(Object[] min, Object[] max, int colStatIndex,
      Map<String, Integer> indexMap) {
    int rightBorderInd = indexMap.size() - 1;
    int minInd = indexMap.get((String) min[1]);
    int maxInd = indexMap.get((String) max[1]);
    if (minInd == maxInd) {
      return min[0];
    }
    if (aggrTypes[colStatIndex] == AggrType.Max) {
      if (minInd < maxInd) {
        // right border is the max
        if (colStatTypes[colStatIndex] == ColStatType.Long) {
          return (Long) ((Long) min[0] + (((Long) max[0] - (Long) min[0])
              * (rightBorderInd - minInd) / (maxInd - minInd)));
        } else if (colStatTypes[colStatIndex] == ColStatType.Double) {
          return (Double) ((Double) min[0] + (((Double) max[0] - (Double) min[0])
              * (rightBorderInd - minInd) / (maxInd - minInd)));
        } else {
          Decimal dmax = (Decimal) max[0];
          BigDecimal bdmax = new BigDecimal(dmax.toString());
          double doublemax = bdmax.doubleValue();
          Decimal dmin = (Decimal) min[0];
          BigDecimal bdmin = new BigDecimal(dmin.toString());
          double doublemin = bdmin.doubleValue();
          double ret = doublemin + (doublemax - doublemin)
              * (rightBorderInd - minInd) / (maxInd - minInd);
          return createThriftDecimal(String.valueOf(ret));
        }
      } else {
        // left border is the max
        if (colStatTypes[colStatIndex] == ColStatType.Long) {
          return (Long) ((Long) min[0] + ((Long) max[0] - (Long) min[0])
              * minInd / (minInd - maxInd));
        } else if (colStatTypes[colStatIndex] == ColStatType.Double) {
          return (Double) ((Double) min[0] + ((Double) max[0] - (Double) min[0])
              * minInd / (maxInd - minInd));
        } else {
          Decimal dmax = (Decimal) max[0];
          BigDecimal bdmax = new BigDecimal(dmax.toString());
          double doublemax = bdmax.doubleValue();
          Decimal dmin = (Decimal) min[0];
          BigDecimal bdmin = new BigDecimal(dmin.toString());
          double doublemin = bdmin.doubleValue();
          double ret = doublemin + (doublemax - doublemin) * minInd
              / (maxInd - minInd);
          return createThriftDecimal(String.valueOf(ret));
        }
      }
    } else {
      if (minInd < maxInd) {
        // left border is the min
        if (colStatTypes[colStatIndex] == ColStatType.Long) {
          Long ret = (Long) max[0] - ((Long) max[0] - (Long) min[0]) * maxInd
              / (maxInd - minInd);
          return ret;
        } else if (colStatTypes[colStatIndex] == ColStatType.Double) {
          Double ret = (Double) max[0] - ((Double) max[0] - (Double) min[0])
              * maxInd / (maxInd - minInd);
          return ret;
        } else {
          Decimal dmax = (Decimal) max[0];
          BigDecimal bdmax = new BigDecimal(dmax.toString());
          double doublemax = bdmax.doubleValue();
          Decimal dmin = (Decimal) min[0];
          BigDecimal bdmin = new BigDecimal(dmin.toString());
          double doublemin = bdmin.doubleValue();
          double ret = doublemax - (doublemax - doublemin) * maxInd
              / (maxInd - minInd);
          return createThriftDecimal(String.valueOf(ret));

        }
      } else {
        // right border is the min
        if (colStatTypes[colStatIndex] == ColStatType.Long) {
          Long ret = (Long) max[0] - ((Long) max[0] - (Long) min[0])
              * (rightBorderInd - maxInd) / (minInd - maxInd);
          return ret;
        } else if (colStatTypes[colStatIndex] == ColStatType.Double) {
          Double ret = (Double) max[0] - ((Double) max[0] - (Double) min[0])
              * (rightBorderInd - maxInd) / (minInd - maxInd);
          return ret;
        } else {
          Decimal dmax = (Decimal) max[0];
          BigDecimal bdmax = new BigDecimal(dmax.toString());
          double doublemax = bdmax.doubleValue();
          Decimal dmin = (Decimal) min[0];
          BigDecimal bdmin = new BigDecimal(dmin.toString());
          double doublemin = bdmin.doubleValue();
          double ret = doublemax - (doublemax - doublemin)
              * (rightBorderInd - maxInd) / (minInd - maxInd);
          return createThriftDecimal(String.valueOf(ret));
        }
      }
    }
  }

  private static Decimal createThriftDecimal(String s) {
    BigDecimal d = new BigDecimal(s);
    return new Decimal(ByteBuffer.wrap(d.unscaledValue().toByteArray()),
        (short) d.scale());
  }

}
