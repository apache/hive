/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.split.SplitLocationProvider;
import org.apache.hive.common.util.Murmur3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This maps a split (path + offset) to an index based on the number of locations provided.
 *
 * If locations do not change across jobs, the intention is to map the same split to the same node.
 *
 * A big problem is when nodes change (added, removed, temporarily removed and re-added) etc. That changes
 * the number of locations / position of locations - and will cause the cache to be almost completely invalidated.
 *
 * TODO: Support for consistent hashing when combining the split location generator and the ServiceRegistry.
 *
 */
public class HostAffinitySplitLocationProvider implements SplitLocationProvider {

  private final Logger LOG = LoggerFactory.getLogger(HostAffinitySplitLocationProvider.class);
  private final boolean isDebugEnabled = LOG.isDebugEnabled();

  private final String[] knownLocations;

  public HostAffinitySplitLocationProvider(String[] knownLocations) {
    Preconditions.checkState(knownLocations != null && knownLocations.length != 0,
        HostAffinitySplitLocationProvider.class.getName() +
            "needs at least 1 location to function");
    this.knownLocations = knownLocations;
  }

  @Override
  public String[] getLocations(InputSplit split) throws IOException {
    if (split instanceof FileSplit) {
      FileSplit fsplit = (FileSplit) split;
      long hash = generateHash(fsplit.getPath().toString(), fsplit.getStart());
      int indexRaw = (int) (hash % knownLocations.length);
      int index = Math.abs(indexRaw);
      if (isDebugEnabled) {
        LOG.debug(
            "Split at " + fsplit.getPath() + " with offset= " + fsplit.getStart() + ", length=" +
                fsplit.getLength() + " mapped to index=" + index + ", location=" +
                knownLocations[index]);
      }
      return new String[]{knownLocations[index]};
    } else {
      if (isDebugEnabled) {
        LOG.debug("Split: " + split + " is not a FileSplit. Using default locations");
      }
      return split.getLocations();
    }
  }

  private long generateHash(String path, long startOffset) throws IOException {
    // Explicitly using only the start offset of a split, and not the length.
    // Splits generated on block boundaries and stripe boundaries can vary slightly. Try hashing both to the same node.
    // There is the drawback of potentially hashing the same data on multiple nodes though, when a large split
    // is sent to 1 node, and a second invocation uses smaller chunks of the previous large split and send them
    // to different nodes.
    DataOutputBuffer dob = new DataOutputBuffer();
    dob.writeLong(startOffset);
    dob.writeUTF(path);
    return Murmur3.hash64(dob.getData(), 0, dob.getLength());
  }
}
