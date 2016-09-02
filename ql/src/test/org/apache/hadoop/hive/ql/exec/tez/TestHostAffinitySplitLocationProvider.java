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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHostAffinitySplitLocationProvider {
  private final Logger LOG = LoggerFactory.getLogger(TestHostAffinitySplitLocationProvider.class);


  private static final String[] locations = new String[5];
  private static final Set<String> locationsSet = new HashSet<>();
  private static final String[] executorLocations = new String[9];
  private static final Set<String> executorLocationsSet = new HashSet<>();

  static {
    for (int i = 0 ; i < 5 ; i++) {
      locations[i] = "location" + i;
      locationsSet.add(locations[i]);
    }

    for (int i = 0 ; i < 9 ; i++) {
      executorLocations[i] = "execLocation" + i;
      executorLocationsSet.add(executorLocations[i]);
    }

  }

  @Test (timeout = 5000)
  public void testNonFileSplits() throws IOException {

    HostAffinitySplitLocationProvider locationProvider = new HostAffinitySplitLocationProvider(executorLocations);

    InputSplit inputSplit1 = createMockInputSplit(new String[] {locations[0], locations[1]});
    InputSplit inputSplit2 = createMockInputSplit(new String[] {locations[2], locations[3]});

    assertArrayEquals(new String[] {locations[0], locations[1]}, locationProvider.getLocations(inputSplit1));
    assertArrayEquals(new String[] {locations[2], locations[3]}, locationProvider.getLocations(inputSplit2));
  }

  @Test (timeout = 5000)
  public void testOrcSplitsBasic() throws IOException {
    HostAffinitySplitLocationProvider locationProvider = new HostAffinitySplitLocationProvider(executorLocations);

    InputSplit os1 = createMockFileSplit(true, "path1", 0, 1000, new String[] {locations[0], locations[1]});
    InputSplit os2 = createMockFileSplit(true, "path2", 0, 2000, new String[] {locations[2], locations[3]});
    InputSplit os3 = createMockFileSplit(true, "path3", 1000, 2000, new String[] {locations[0], locations[3]});

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

    String[] locations = new String[LOC_COUNT];
    for (int i = 0; i < locations.length; ++i) {
      locations[i] = String.valueOf(i);
    }
    InputSplit[] splits = new InputSplit[SPLIT_COUNT];
    for (int i = 0; i < splits.length; ++i) {
      splits[i] = createMockFileSplit(true, "path" + i, 0, 1000, new String[] {});
    }

    StringBuilder failBuilder = new StringBuilder("\n");
    String[] lastLocations = new String[splits.length];
    double movedRatioSum = 0, newRatioSum = 0,
        movedRatioWorst = 0, newRatioWorst = Double.MAX_VALUE;
    for (int locs = MIN_LOC_COUNT; locs <= locations.length; ++locs) {
      String[] partLoc = Arrays.copyOf(locations, locs);
      HostAffinitySplitLocationProvider lp = new HostAffinitySplitLocationProvider(partLoc);
      int moved = 0, newLoc = 0;
      String newNode = partLoc[locs - 1];
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
      if (locs == MIN_LOC_COUNT) continue;
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
    int count = locations.length - MIN_LOC_COUNT;
    double moveRatioAvg = movedRatioSum / count, newRatioAvg = newRatioSum / count;
    String errorMsg = "Move counts: average " + moveRatioAvg + ", worst " + movedRatioWorst
        + "; assigned to new node: average " + newRatioAvg + ", worst " + newRatioWorst;
    LOG.info(errorMsg);
    // Give it a LOT of slack, since on low numbers consistent hashing is very imprecise.
    if (moveRatioAvg > 1.2f || newRatioAvg < 0.8f
        || movedRatioWorst > 1.5f || newRatioWorst < 0.5f) {
      fail(errorMsg + "; example failures: " + failBuilder.toString());
    }
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
    InputSplit os11 = createMockFileSplit(true, "path1", 0, 15000, new String[] {locations[0], locations[1]});
    InputSplit os12 = createMockFileSplit(true, "path1", 0, 30000, new String[] {locations[0], locations[1]});
    // Same file, different offset
    InputSplit os13 = createMockFileSplit(true, "path1", 15000, 30000, new String[] {locations[0], locations[1]});

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

  private InputSplit createMockFileSplit(boolean createOrcSplit, String fakePathString, long start,
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

    doReturn(locations).when(fileSplit).getLocations();
    return fileSplit;
  }


}
