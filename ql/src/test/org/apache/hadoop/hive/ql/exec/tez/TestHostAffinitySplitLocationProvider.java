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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.junit.Test;

public class TestHostAffinitySplitLocationProvider {


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
