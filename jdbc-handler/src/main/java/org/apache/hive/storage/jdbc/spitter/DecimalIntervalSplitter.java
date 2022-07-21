/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc.spitter;

import org.apache.commons.lang3.tuple.MutablePair;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

public class DecimalIntervalSplitter implements IntervalSplitter {
  @Override
  public List<MutablePair<String, String>> getIntervals(String lowerBound, String upperBound, int numPartitions) {
    List<MutablePair<String, String>> intervals = new ArrayList<>();
    BigDecimal decimalLower = new BigDecimal(lowerBound);
    BigDecimal decimalUpper = new BigDecimal(upperBound);
    // The bounds are either fetched automatically by querying the database or specified by the
    // user writing the table DDL statement. In the first case the scale for both bounds is the
    // same. In the second case the scale may differ since we have no control over the users choice.
    //
    // There is no reason to let a user specify a different scale between the bounds and maybe the
    // best option would be to throw an exception when this happens. However, throwing an exception
    // at this stage (query runtime) is not very nice, so we must find an alternative.
    // 
    // Using max or min both have some disadvantages and both choices can lead to incorrect results
    // if additional rounding happens in the database side since the intevals may not be distinct
    // anymore.
    int scale = Math.max(decimalLower.scale(), decimalUpper.scale());
    BigDecimal decimalInterval = (decimalUpper.subtract(decimalLower)).divide(new BigDecimal(numPartitions),
            MathContext.DECIMAL64);
    BigDecimal splitDecimalLower, splitDecimalUpper;
    for (int i=0;i<numPartitions;i++) {
      splitDecimalLower = decimalLower.add(decimalInterval.multiply(new BigDecimal(i))).setScale(scale,
          RoundingMode.HALF_EVEN);
      splitDecimalUpper = decimalLower.add(decimalInterval.multiply(new BigDecimal(i+1))).setScale(scale,
          RoundingMode.HALF_EVEN);
      if (splitDecimalLower.compareTo(splitDecimalUpper) < 0) {
        intervals.add(new MutablePair<String, String>(splitDecimalLower.toPlainString(), splitDecimalUpper.toPlainString()));
      }
    }
    return intervals;
  }
}
