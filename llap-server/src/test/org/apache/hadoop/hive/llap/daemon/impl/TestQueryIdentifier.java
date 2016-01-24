package org.apache.hadoop.hive.llap.daemon.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

public class TestQueryIdentifier {

  @Test (timeout = 5000)
  public void testEquality() {

    String appIdString1 = "app1";
    String appIdString2 = "app2";

    int dagId1 = 1;
    int dagId2 = 2;

    QueryIdentifier[] queryIdentifiers = new QueryIdentifier[4];

    queryIdentifiers[0] = new QueryIdentifier(appIdString1, dagId1);
    queryIdentifiers[1] = new QueryIdentifier(appIdString1, dagId2);
    queryIdentifiers[2] = new QueryIdentifier(appIdString2, dagId1);
    queryIdentifiers[3] = new QueryIdentifier(appIdString2, dagId2);

    for (int i = 0 ; i < 4 ; i++) {
      for (int j = 0 ; j < 4 ; j++) {
        if (i == j) {
          assertEquals(queryIdentifiers[i], queryIdentifiers[j]);
        } else {
          assertNotEquals(queryIdentifiers[i], queryIdentifiers[j]);
        }
      }
    }

    QueryIdentifier q11 = new QueryIdentifier(appIdString1, dagId1);
    QueryIdentifier q12 = new QueryIdentifier(appIdString1, dagId2);
    QueryIdentifier q21 = new QueryIdentifier(appIdString2, dagId1);
    QueryIdentifier q22 = new QueryIdentifier(appIdString2, dagId2);

    assertEquals(queryIdentifiers[0], q11);
    assertEquals(queryIdentifiers[1], q12);
    assertEquals(queryIdentifiers[2], q21);
    assertEquals(queryIdentifiers[3], q22);


  }
}
