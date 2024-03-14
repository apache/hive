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

package org.apache.hadoop.hive.metastore.tools.metatool;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MetaToolTaskUpdateLocation extends MetaToolTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetaToolTaskUpdateLocation.class.getName());

  @Override
  void execute() {
    String[] loc = getCl().getUpddateLocationParams();

    Path newPath = new Path(loc[0]);
    Path oldPath = new Path(loc[1]);

    URI oldURI = oldPath.toUri();
    URI newURI = newPath.toUri();

    /*
     * validate input - Both new and old URI should contain valid host names and valid schemes.
     * port is optional in both the URIs since HDFS HA NN URI doesn't have a port.
     */
    if (oldURI.getHost() == null || newURI.getHost() == null) {
      throw new IllegalStateException("HiveMetaTool:A valid host is required in both old-loc and new-loc");
    } else if (oldURI.getScheme() == null || newURI.getScheme() == null) {
      throw new IllegalStateException("HiveMetaTool:A valid scheme is required in both old-loc and new-loc");
    }

    updateFSRootLocation(oldURI, newURI, getCl().getSerdePropKey(), getCl().getTablePropKey(), getCl().isDryRun());
  }

  private void updateFSRootLocation(URI oldURI, URI newURI, String serdePropKey, String tablePropKey,
      boolean isDryRun) {
    updateMDatabaseURI(oldURI, newURI, isDryRun);
    updateMStorageDescriptorTblURI(oldURI, newURI, isDryRun);
    updateTablePropURI(oldURI, newURI, tablePropKey, isDryRun);
    upateSerdeURI(oldURI, newURI, serdePropKey, isDryRun);
  }

  private void updateMDatabaseURI(URI oldURI, URI newURI, boolean isDryRun) {
    System.out.println("Looking for LOCATION_URI field in DBS table to update..");
    ObjectStore.UpdateMDatabaseURIRetVal retVal = getObjectStore().updateMDatabaseURI(oldURI, newURI, isDryRun);
    if (retVal == null) {
      System.err.println("Encountered error while executing updateMDatabaseURI - commit of JDO transaction failed. " +
          "Failed to update FSRoot locations in LOCATION_URI field in DBS table.");
    } else {
      printUpdateLocations(retVal.getUpdateLocations(), isDryRun, "DBS");
      printBadRecords(retVal.getBadRecords(), "DBS", "LOCATION_URI");
    }
  }

  private void updateMStorageDescriptorTblURI(URI oldURI, URI newURI, boolean isDryRun) {
    System.out.println("Looking for LOCATION field in SDS table to update..");
    ObjectStore.UpdateMStorageDescriptorTblURIRetVal retVal =
        getObjectStore().updateMStorageDescriptorTblURI(oldURI, newURI, isDryRun);
    if (retVal == null) {
      System.err.println("Encountered error while executing updateMStorageDescriptorTblURI - commit of JDO " +
          "transaction failed. Failed to update FSRoot locations in LOCATION field in SDS table.");
    } else {
      printUpdateLocations(retVal.getUpdateLocations(), isDryRun, "SDS");
      printBadRecords(retVal.getBadRecords(), "SDS", "LOCATION");

      int numNullRecords = retVal.getNumNullRecords();
      if (numNullRecords != 0) {
        LOGGER.debug("Number of NULL location URI: " + numNullRecords + ". This can happen for View or Index.");
      }
    }
  }

  private void updateTablePropURI(URI oldURI, URI newURI, String tablePropKey, boolean isDryRun) {
    if (tablePropKey != null) {
      System.out.println("Looking for value of " + tablePropKey + " key in TABLE_PARAMS table to update..");
      ObjectStore.UpdatePropURIRetVal updateTblPropURIRetVal =
          getObjectStore().updateTblPropURI(oldURI, newURI, tablePropKey, isDryRun);
      printPropURIUpdateSummary(updateTblPropURIRetVal, tablePropKey, isDryRun, "TABLE_PARAMS", "updateTblPropURI");

      System.out.println("Looking for value of " + tablePropKey + " key in SD_PARAMS table to update..");
      ObjectStore.UpdatePropURIRetVal updatePropURIRetVal =
          getObjectStore().updateMStorageDescriptorTblPropURI(oldURI, newURI, tablePropKey, isDryRun);
      printPropURIUpdateSummary(updatePropURIRetVal, tablePropKey, isDryRun, "SD_PARAMS",
          "updateMStorageDescriptorTblPropURI");
    }
  }

  private void printPropURIUpdateSummary(ObjectStore.UpdatePropURIRetVal retVal, String tablePropKey, boolean isDryRun,
      String tblName, String methodName) {
    if (retVal == null) {
      System.err.println("Encountered error while executing " + methodName + " - commit of JDO transaction failed. " +
          "Failed to update FSRoot locations in value field corresponding to" + tablePropKey + " in " + tblName +
          " table.");
    } else {
      printUpdateLocations(retVal.getUpdateLocations(), isDryRun, tblName);
      printBadRecords(retVal.getBadRecords(), tblName, tablePropKey + " key");
    }
  }

  private void upateSerdeURI(URI oldURI, URI newURI, String serdePropKey, boolean isDryRun) {
    if (serdePropKey != null) {
      System.out.println("Looking for value of " + serdePropKey + " key in SERDE_PARAMS table to update..");
      ObjectStore.UpdateSerdeURIRetVal retVal =
          getObjectStore().updateSerdeURI(oldURI, newURI, serdePropKey, isDryRun);
      if (retVal == null) {
        System.err.println("Encountered error while executing updateSerdeURI - commit of JDO transaction failed. " +
            "Failed to update FSRoot locations in value field corresponding to " + serdePropKey + " in " +
            "SERDE_PARAMS table.");
      } else {
        printUpdateLocations(retVal.getUpdateLocations(), isDryRun, "SERDE_PARAMS");
        printBadRecords(retVal.getBadRecords(), "SERDE_PARAMS", serdePropKey + " key");
      }
    }
  }

  private void printUpdateLocations(Map<String, String> updateLocations, boolean isDryRun, String tableName) {
    System.out.println(isDryRun ?
        "Dry Run of updateLocation on table " + tableName + ".." :
        "Successfully updated the following locations..");

    for (Map.Entry<String, String> e : updateLocations.entrySet()) {
      System.out.println("old location: " + e.getKey() + " new location: " + e.getValue());
    }

    System.out.println(isDryRun ?
        "Found " + updateLocations.size() + " records in " + tableName + " table to update" :
        "Updated " + updateLocations.size() + " records in " + tableName + " table");
  }

  private void printBadRecords(List<String> badRecords, String tableName, String field) {
    if (!badRecords.isEmpty()) {
      System.err.println("Warning: Found records with bad " + field + " in " + tableName + " table.. ");
      for (String badRecord : badRecords) {
        System.err.println("bad location URI: " + badRecord);
      }
    }
  }
}
