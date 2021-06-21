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

package org.apache.hadoop.hive.llap.tezplugins.scheduler;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class StatsPerDag {

  private AtomicInteger numRequestedAllocations = new AtomicInteger(0);
  private AtomicInteger numRequestsWithLocation = new AtomicInteger(0);
  private AtomicInteger numRequestsWithoutLocation = new AtomicInteger(0);
  // Below fields are accessed within WriteLock so no need for Atomic
  private int numTotalAllocations = 0;
  private int numLocalAllocations = 0;
  private int numNonLocalAllocations = 0;
  private int numAllocationsNoLocalityRequest = 0;
  private int numRejectedTasks = 0;
  private int numCommFailures = 0;
  private int numDelayedAllocations = 0;
  private int numPreemptedTasks = 0;
  private Map<String, AtomicInteger> localityBasedNumAllocationsPerHost = new HashMap<>();
  private Map<String, AtomicInteger> numAllocationsPerHost = new HashMap<>();

  public int getNumTotalAllocations() {
    return numTotalAllocations;
  }

  public int getNumLocalAllocations() {
    return numLocalAllocations;
  }

  public int getNumNonLocalAllocations() {
    return numNonLocalAllocations;
  }

  public int getNumAllocationsNoLocalityRequest() {
    return numAllocationsNoLocalityRequest;
  }

  public int getNumPreemptedTasks() {
    return numPreemptedTasks;
  }

  public int getNumRejectedTasks() {
    return numRejectedTasks;
  }

  public int getNumDelayedAllocations() {
    return numDelayedAllocations;
  }

  public Map<String, AtomicInteger> getNumAllocationsPerHost() {
    return numAllocationsPerHost;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("NumPreemptedTasks=").append(numPreemptedTasks).append(", ");
    sb.append("NumRequestedAllocations=").append(numRequestedAllocations).append(", ");
    sb.append("NumRequestsWithlocation=").append(numRequestsWithLocation).append(", ");
    sb.append("NumLocalAllocations=").append(numLocalAllocations).append(",");
    sb.append("NumNonLocalAllocations=").append(numNonLocalAllocations).append(",");
    sb.append("NumTotalAllocations=").append(numTotalAllocations).append(",");
    sb.append("NumRequestsWithoutLocation=").append(numRequestsWithoutLocation).append(", ");
    sb.append("NumRejectedTasks=").append(numRejectedTasks).append(", ");
    sb.append("NumCommFailures=").append(numCommFailures).append(", ");
    sb.append("NumDelayedAllocations=").append(numDelayedAllocations).append(", ");
    sb.append("LocalityBasedAllocationsPerHost=").append(localityBasedNumAllocationsPerHost).append(", ");
    sb.append("NumAllocationsPerHost=").append(numAllocationsPerHost);
    return sb.toString();
  }

  /**
   * Update DAG metrics caused by an incoming allocate Task call.
   * Updated metrics should use Atomics here to avoid using a lock on the caller.
   * @param requestedHosts
   * @param requestedRacks
   */
  public void registerTaskRequest(String[] requestedHosts, String[] requestedRacks) {
    numRequestedAllocations.incrementAndGet();
    // TODO Change after HIVE-9987. For now, there's no rack matching.
    if (requestedHosts != null && requestedHosts.length != 0) {
      numRequestsWithLocation.incrementAndGet();
    } else {
      numRequestsWithoutLocation.incrementAndGet();
    }
  }

  public void registerTaskAllocated(String[] requestedHosts, String[] requestedRacks, String allocatedHost) {
    // TODO Change after HIVE-9987. For now, there's no rack matching.
    if (requestedHosts != null && requestedHosts.length != 0) {
      Set<String> requestedHostSet = new HashSet<>(Arrays.asList(requestedHosts));
      if (requestedHostSet.contains(allocatedHost)) {
        numLocalAllocations++;
        _registerAllocationInHostMap(allocatedHost, localityBasedNumAllocationsPerHost);
      } else {
        numNonLocalAllocations++;
      }
    } else {
      numAllocationsNoLocalityRequest++;
    }
    numTotalAllocations++;
    _registerAllocationInHostMap(allocatedHost, numAllocationsPerHost);
  }

  // TODO Track stats of rejections etc per host
  public void registerTaskPreempted(String host) {
    numPreemptedTasks++;
  }

  public void registerCommFailure(String host) {
    numCommFailures++;
  }

  public void registerTaskRejected(String host) {
    numRejectedTasks++;
  }

  public void registerDelayedAllocation() {
    numDelayedAllocations++;
  }

  private void _registerAllocationInHostMap(String host, Map<String, AtomicInteger> hostMap) {
    AtomicInteger val = hostMap.get(host);
    if (val == null) {
      val = new AtomicInteger(0);
      hostMap.put(host, val);
    }
    val.incrementAndGet();
  }
}
