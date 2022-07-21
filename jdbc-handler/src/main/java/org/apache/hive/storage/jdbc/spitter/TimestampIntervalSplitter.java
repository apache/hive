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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class TimestampIntervalSplitter implements IntervalSplitter {
  @Override
  public List<MutablePair<String, String>> getIntervals(String lowerBound, String upperBound, int numPartitions, TypeInfo
          typeInfo) {
    List<MutablePair<String, String>> intervals = new ArrayList<>();
    Timestamp timestampLower = Timestamp.valueOf(lowerBound);
    Timestamp timestampUpper = Timestamp.valueOf(upperBound);
    // Note nano is not fully represented as the precision limit
    double timestampInterval = (timestampUpper.getTime() - timestampLower.getTime())/(double)numPartitions;
    Timestamp splitTimestampLower, splitTimestampUpper;
    for (int i=0;i<numPartitions;i++) {
      splitTimestampLower = new Timestamp(Math.round(timestampLower.getTime() + timestampInterval*i));
      splitTimestampUpper = new Timestamp(Math.round(timestampLower.getTime() + timestampInterval*(i+1)));
      if (splitTimestampLower.compareTo(splitTimestampUpper) < 0) {
        intervals.add(new MutablePair<String, String>(splitTimestampLower.toString(), splitTimestampUpper.toString()));
      }
    }
    return intervals;
  }
}
