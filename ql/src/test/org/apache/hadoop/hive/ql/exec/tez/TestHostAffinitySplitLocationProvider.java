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

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHostAffinitySplitLocationProvider {
  private final Logger LOG = LoggerFactory.getLogger(TestHostAffinitySplitLocationProvider.class);


  private static final List<String> locations = new ArrayList<>();
  private static final Set<String> locationsSet = new HashSet<>();
  private static final List<String> executorLocations = new ArrayList<>();
  private static final Set<String> executorLocationsSet = new HashSet<>();

  static {
    for (int i = 0 ; i < 5 ; i++) {
      locations.add("location" + i);
      locationsSet.add(locations.get(i));
    }

    for (int i = 0 ; i < 9 ; i++) {
      executorLocations.add("execLocation" + i);
      executorLocationsSet.add(executorLocations.get(i));
    }

  }

  @Test (timeout = 5000)
  public void testNonFileSplits() throws IOException {

    HostAffinitySplitLocationProvider locationProvider = new HostAffinitySplitLocationProvider(executorLocations);

    InputSplit inputSplit1 = createMockInputSplit(new String[] {locations.get(0), locations.get(1)});
    InputSplit inputSplit2 = createMockInputSplit(new String[] {locations.get(2), locations.get(3)});

    assertArrayEquals(new String[] {locations.get(0), locations.get(1)}, locationProvider.getLocations(inputSplit1));
    assertArrayEquals(new String[] {locations.get(2), locations.get(3)}, locationProvider.getLocations(inputSplit2));
  }

  @Test (timeout = 5000)
  public void testOrcSplitsBasic() throws IOException {
    HostAffinitySplitLocationProvider locationProvider = new HostAffinitySplitLocationProvider(executorLocations);

    InputSplit os1 = createMockFileSplit(true, "path1", 0, 1000, new String[] {locations.get(0), locations.get(1)});
    InputSplit os2 = createMockFileSplit(true, "path2", 0, 2000, new String[] {locations.get(2), locations.get(3)});
    InputSplit os3 = createMockFileSplit(true, "path3", 1000, 2000, new String[] {locations.get(0), locations.get(3)});

    String[] retLoc1 = locationProvider.getLocations(os1);
    String[] retLoc2 = locationProvider.getLocations(os2);
    String[] retLoc3 = locationProvider.getLocations(os3);

    assertEquals(1, retLoc1.length);
    assertFalse(locationsSet.contains(retLoc1[0]));
    assertTrue(executorLocationsSet.contains(retLoc1[0]));

    assertEquals(1, retLoc2.length);
    assertFalse(locationsSet.contains(retLoc2[0]));
    assertTrue(executorLocationsSet.contains(retLoc2[0]));

    assertEquals(1, retLoc3.length);
    assertFalse(locationsSet.contains(retLoc3[0]));
    assertTrue(executorLocationsSet.contains(retLoc3[0]));
  }


  @Test (timeout = 10000)
  public void testConsistentHashing() throws IOException {
    final int LOC_COUNT = 20, MIN_LOC_COUNT = 4, SPLIT_COUNT = 100;
    List<String> locations = createLocations(LOC_COUNT);
    InputSplit[] splits = createSplits(SPLIT_COUNT);

    StringBuilder failBuilder = new StringBuilder("\n");
    String[] lastLocations = new String[splits.length];
    double movedRatioSum = 0, newRatioSum = 0,
        movedRatioWorst = 0, newRatioWorst = Double.MAX_VALUE;
    for (int locs = MIN_LOC_COUNT; locs <= locations.size(); ++locs) {
      List<String> partLoc = locations.subList(0, locs);
      HostAffinitySplitLocationProvider lp = new HostAffinitySplitLocationProvider(partLoc);
      int moved = 0, newLoc = 0;
      String newNode = partLoc.get(locs - 1);
      for (int splitIx = 0; splitIx < splits.length; ++splitIx) {
        String[] splitLocations = lp.getLocations(splits[splitIx]);
        assertEquals(1, splitLocations.length);
        String splitLocation = splitLocations[0];
        if (locs > MIN_LOC_COUNT && !splitLocation.equals(lastLocations[splitIx])) {
          ++moved;
        }
        if (newNode.equals(splitLocation)) {
          ++newLoc;
        }
        lastLocations[splitIx] = splitLocation;
      }
      if (locs == MIN_LOC_COUNT) {
        continue;
      }
      String msgTail = " when going to " + locs + " locations";
      String movedMsg = moved + " splits moved",
          newMsg = newLoc + " splits went to the new node";
      LOG.info(movedMsg + " and " + newMsg + msgTail);
      double maxMoved = 1.0f * splits.length / locs, minNew = 1.0f * splits.length / locs;
      movedRatioSum += moved / maxMoved;
      movedRatioWorst = Math.max(moved / maxMoved, movedRatioWorst);
      newRatioSum += newLoc / minNew;
      newRatioWorst = Math.min(newLoc / minNew, newRatioWorst);
      logBadRatios(failBuilder, moved, newLoc, msgTail, movedMsg, newMsg, maxMoved, minNew);
    }
    int count = locations.size() - MIN_LOC_COUNT;
    double moveRatioAvg = movedRatioSum / count, newRatioAvg = newRatioSum / count;
    String errorMsg = "Move counts: average " + moveRatioAvg + ", worst " + movedRatioWorst
        + "; assigned to new node: average " + newRatioAvg + ", worst " + newRatioWorst;
    LOG.info(errorMsg);
    // Give it a LOT of slack, since on low numbers consistent hashing is very imprecise.
    if (moveRatioAvg > 1.2f || newRatioAvg < 0.8f
        || movedRatioWorst > 1.67f || newRatioWorst < 0.5f) {
      fail(errorMsg + "; example failures: " + failBuilder.toString());
    }
  }

  public FileSplit[] createSplits(final int splitCount) throws IOException {
    FileSplit[] splits = new FileSplit[splitCount];
    for (int i = 0; i < splits.length; ++i) {
      splits[i] = createMockFileSplit(true, "path" + i, 0, 1000, new String[] {});
    }
    return splits;
  }

  public List<String> createLocations(final int locCount) {
    List<String> locations = new ArrayList<>(locCount);
    for (int i = 0; i < locCount; ++i) {
      locations.add(String.valueOf(i));
    }
    return locations;
  }

  @org.junit.Ignore("HIVE-26308")
  @Test (timeout = 20000)
  public void testConsistentHashingFallback() throws IOException {
    final int LOC_COUNT_TO = 20, SPLIT_COUNT = 500, MAX_MISS_COUNT = 4,
        LOC_COUNT_FROM = MAX_MISS_COUNT + 1;
    FileSplit[] splits = createSplits(SPLIT_COUNT);
    AtomicInteger errorCount = new AtomicInteger(0);
    int cvErrorCount = 0;
    for (int locs = LOC_COUNT_FROM; locs <= LOC_COUNT_TO; ++locs) {
      int aboveAvgCount = 0;
      double sum = 0;
      double[] cvs = new double[MAX_MISS_COUNT + 1];
      for (int missCount = 0; missCount <= MAX_MISS_COUNT; ++missCount) {
        double cv = cvs[missCount] = testHashDistribution(locs, missCount, splits, errorCount);
        sum += cv;
        if (missCount > 0 && cv > sum / (missCount + 1)) {
          ++aboveAvgCount;
        }
      }
      if (aboveAvgCount > 2) {
        LOG.info("CVs for " + locs + " locations aren't to our liking: " + Arrays.toString(cvs));
        ++cvErrorCount;
      }
    }
    assertTrue("Found " + errorCount.get() + " abnormalities", errorCount.get() < 3);
    // TODO: the way we add hash fns does exhibit some irregularities.
    //       Seems like the 3rd iter has a better distribution in many cases, even better
    //       that the original hash. That trips the "above MA" criteria, even if the rest is flat.
    assertTrue("Found " + cvErrorCount + " abnormalities", cvErrorCount< 7);
  }

  private double testHashDistribution(int locs, final int missCount, FileSplit[] splits,
      AtomicInteger errorCount) {
    // This relies heavily on what method determineSplits ... calls and doesn't.
    // We could do a wrapper with only size() and get() methods instead of List, to be sure.
    @SuppressWarnings("unchecked")
    List<String> partLocs = (List<String>)Mockito.mock(List.class);
    Mockito.when(partLocs.size()).thenReturn(locs);
    final AtomicInteger state = new AtomicInteger(0);
    Mockito.when(partLocs.get(Mockito.anyInt())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        return (state.getAndIncrement() == missCount) ? "not-null" : null;
      }
    });
    int[] hitCounts = new int[locs];
    for (int splitIx = 0; splitIx < splits.length; ++splitIx) {
      state.set(0);
      int index = HostAffinitySplitLocationProvider.determineLocation(partLocs, splits[splitIx]);
      ++hitCounts[index];
    }
    SummaryStatistics ss = new SummaryStatistics();
    for (int hitCount : hitCounts) {
      ss.addValue(hitCount);
    }
    // All of this is completely bogus and mostly captures the following function:
    // f(output) = I-eyeballed-the(output) == they-look-ok.
    // It's pretty much a golden file...
    // The fact that stdev doesn't increase with increasing missCount is captured outside.
    double avg = ss.getSum()/ss.getN(), stdev = ss.getStandardDeviation(), cv = stdev/avg;
    double allowedMin = avg - 2.5 * stdev, allowedMax = avg + 2.5 * stdev;
    if (allowedMin > ss.getMin() || allowedMax < ss.getMax() || cv > 0.22) {
      LOG.info("The distribution for " + locs + " locations, " + missCount + " misses isn't to "
          + "our liking: avg " + avg + ", stdev " + stdev  + ", cv " + cv + ", min " + ss.getMin()
          + ", max " + ss.getMax());
      errorCount.incrementAndGet();
    }
    return cv;
  }


  private void logBadRatios(StringBuilder failBuilder, int moved, int newLoc, String msgTail,
      String movedMsg, String newMsg, double maxMoved, double minNew) {
    boolean logged = false;
    if (moved > maxMoved * 1.33f) {
      failBuilder.append(movedMsg).append(" (threshold ").append(maxMoved).append(") ");
      logged = true;
    }
    if (newLoc < minNew * 0.75f) {
      failBuilder.append(newMsg).append(" (threshold ").append(minNew).append(") ");
      logged = true;
    }
    if (logged) {
      failBuilder.append(msgTail).append(";\n");
    }
  }

  @Test (timeout = 5000)
  public void testOrcSplitsLocationAffinity() throws IOException {
    HostAffinitySplitLocationProvider locationProvider = new HostAffinitySplitLocationProvider(executorLocations);

    // Same file, offset, different lengths
    InputSplit os11 = createMockFileSplit(true, "path1", 0, 15000, new String[] {locations.get(0), locations.get(1)});
    InputSplit os12 = createMockFileSplit(true, "path1", 0, 30000, new String[] {locations.get(0), locations.get(1)});
    // Same file, different offset
    InputSplit os13 = createMockFileSplit(true, "path1", 15000, 30000, new String[] {locations.get(0), locations.get(1)});

    String[] retLoc11 = locationProvider.getLocations(os11);
    String[] retLoc12 = locationProvider.getLocations(os12);
    String[] retLoc13 = locationProvider.getLocations(os13);

    assertEquals(1, retLoc11.length);
    assertFalse(locationsSet.contains(retLoc11[0]));
    assertTrue(executorLocationsSet.contains(retLoc11[0]));

    assertEquals(1, retLoc12.length);
    assertFalse(locationsSet.contains(retLoc12[0]));
    assertTrue(executorLocationsSet.contains(retLoc12[0]));

    assertEquals(1, retLoc13.length);
    assertFalse(locationsSet.contains(retLoc13[0]));
    assertTrue(executorLocationsSet.contains(retLoc13[0]));

    // Verify the actual locations being correct.
    // os13 should be on a different location. Splits are supposed to be consistent across JVMs,
    // the test is setup to verify a different host (make sure not to hash to the same host as os11,os12).
    // If the test were to fail because the host is the same - the assumption about consistent across JVM
    // instances is likely incorrect.
    assertEquals(retLoc11[0], retLoc12[0]);
    assertNotEquals(retLoc11[0], retLoc13[0]);


    // Get locations again, and make sure they're the same.
    String[] retLoc112 = locationProvider.getLocations(os11);
    String[] retLoc122 = locationProvider.getLocations(os12);
    String[] retLoc132 = locationProvider.getLocations(os13);
    assertArrayEquals(retLoc11, retLoc112);
    assertArrayEquals(retLoc12, retLoc122);
    assertArrayEquals(retLoc13, retLoc132);
  }


  private InputSplit createMockInputSplit(String[] locations) throws IOException {
    InputSplit inputSplit = mock(InputSplit.class);
    doReturn(locations).when(inputSplit).getLocations();
    return inputSplit;
  }

  static FileSplit createMockFileSplit(boolean createOrcSplit, String fakePathString, long start,
                                       long length, String[] locations) throws IOException {
    FileSplit fileSplit;
    if (createOrcSplit) {
      fileSplit = mock(OrcSplit.class);
    } else {
      fileSplit = mock(FileSplit.class);
    }

    doReturn(start).when(fileSplit).getStart();
    doReturn(length).when(fileSplit).getLength();
    doReturn(new Path(fakePathString)).when(fileSplit).getPath();
    doReturn(locations).when(fileSplit).getLocations();

    return new HiveInputFormat.HiveInputSplit(fileSplit, "unused");
  }


}
