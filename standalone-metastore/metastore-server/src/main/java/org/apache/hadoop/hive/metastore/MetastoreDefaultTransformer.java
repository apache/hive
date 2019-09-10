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
package org.apache.hadoop.hive.metastore;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_NONE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_READONLY;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_READWRITE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_TRANSACTIONAL;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.EXTERNAL_TABLE_PURGE;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetastoreDefaultTransformer implements IMetaStoreMetadataTransformer {
  public static final Logger LOG = LoggerFactory.getLogger(MetastoreDefaultTransformer.class);
  private IHMSHandler hmsHandler = null;

  private static final String CONNECTORREAD = "CONNECTORREAD".intern();
  private static final String CONNECTORWRITE = "CONNECTORWRITE".intern();
  private static final String EXTWRITE = "EXTWRITE".intern();
  private static final String EXTREAD = "EXTREAD".intern();
  private static final String HIVEBUCKET2 = "HIVEBUCKET2".intern();
  private static final String HIVECACHEINVALIDATE = "HIVECACHEINVALIDATE".intern();
  private static final String HIVEFULLACIDREAD = "HIVEFULLACIDREAD".intern();
  private static final String HIVEFULLACIDWRITE = "HIVEFULLACIDWRITE".intern();
  private static final String HIVEMANAGEDINSERTREAD = "HIVEMANAGEDINSERTREAD".intern();
  private static final String HIVEMANAGEDINSERTWRITE = "HIVEMANAGEDINSERTWRITE".intern();
  private static final String HIVEMANAGESTATS = "HIVEMANAGESTATS".intern();
  private static final String HIVEMQT = "HIVEMQT".intern();
  private static final String HIVEONLYMQTWRITE = "HIVEONLYMQTWRITE".intern();
  private static final String HIVESQL = "HIVESQL".intern();
  private static final String OBJCAPABILITIES = "OBJCAPABILITIES".intern();
  private static final String MANAGERAWMETADATA = "MANAGE_RAW_METADATA".intern();

  private static final List<String> ACIDCOMMONWRITELIST = new ArrayList(Arrays.asList(
      HIVEMANAGESTATS,
      HIVECACHEINVALIDATE,
      CONNECTORWRITE));

  private List<String> acidWriteList = new ArrayList<>(Arrays.asList(HIVEFULLACIDWRITE));
  private List<String> insertOnlyWriteList = new ArrayList<>(Arrays.asList(HIVEMANAGEDINSERTWRITE));
  private static List<String> MQTLIST = new ArrayList<>(Arrays.asList(HIVEFULLACIDREAD, HIVEONLYMQTWRITE,
      HIVEMANAGESTATS, HIVEMQT, CONNECTORREAD));

  private List<String> acidList = new ArrayList<>();
  private List<String> insertOnlyList = new ArrayList<>();
  public MetastoreDefaultTransformer(IHMSHandler handler) throws HiveMetaException {
    this.hmsHandler = handler;

    acidWriteList.addAll(ACIDCOMMONWRITELIST);
    acidList.addAll(acidWriteList);
    acidList.add(HIVEFULLACIDREAD);
    acidList.add(CONNECTORREAD);

    insertOnlyWriteList.addAll(ACIDCOMMONWRITELIST);
    insertOnlyList.addAll(insertOnlyWriteList);
    insertOnlyList.add(HIVEMANAGEDINSERTREAD);
    insertOnlyList.add(CONNECTORREAD);
  }

  @Override
  public Map<Table, List<String>> transform(List<Table> objects, List<String> processorCapabilities, String processorId) throws MetaException {
    LOG.info("Starting translation for processor " + processorId + " on list " + objects.size());
    Map<Table, List<String>> ret = new HashMap<Table, List<String>>();

    for (Table table : objects) {
      Map<String, String> params = table.getParameters();
      String tableType = table.getTableType();
      String tCapabilities = params.get(OBJCAPABILITIES);
      int numBuckets = table.getSd().getNumBuckets();
      boolean isBucketed = (numBuckets > 0) ? true : false;
      List<String> generated = new ArrayList<String>();
      List<String> requiredReads = new ArrayList<>();
      List<String> requiredWrites = new ArrayList<>();

      LOG.info("Table " + table.getTableName() + ",#bucket=" + numBuckets + ",isBucketed:" + isBucketed + ",tableType=" + tableType + ",tableCapabilities=" + tCapabilities);

      // if the table has no tCapabilities, then generate default ones
      if (tCapabilities == null) {
        LOG.debug("Table has no specific required capabilities");

        switch (tableType) {
          case "EXTERNAL_TABLE":
            Table newTable = new Table(table);
            generated.add(EXTREAD);
            generated.add(EXTWRITE);

            if (numBuckets > 0) {
              generated.add(HIVEBUCKET2);
              if (processorCapabilities.contains(HIVEBUCKET2)) {
                LOG.debug("External bucketed table with HB2 capability:RW");
                newTable.setAccessType(ACCESSTYPE_READWRITE);
              } else {
                LOG.debug("External bucketed table without HB2 capability:RO");
                newTable.setAccessType(ACCESSTYPE_READONLY);
                requiredWrites.add(HIVEBUCKET2);
                StorageDescriptor newSd = new StorageDescriptor(table.getSd());
                newSd.setNumBuckets(-1); // remove bucketing info
                newTable.setSd(newSd);
                newTable.setRequiredWriteCapabilities(requiredWrites);
                LOG.info("Bucketed table without HIVEBUCKET2 capability, removed bucketing info from table");
              }
            } else { // Unbucketed
              if (processorCapabilities.contains(EXTWRITE) && processorCapabilities.contains(EXTREAD)) {
                LOG.debug("External unbucketed table with EXTREAD/WRITE capability:RW");
                newTable.setAccessType(ACCESSTYPE_READWRITE);
              } else if (processorCapabilities.contains(EXTREAD)) {
                LOG.debug("External unbucketed table with EXTREAD capability:RO");
                newTable.setAccessType(ACCESSTYPE_READONLY);
                requiredWrites.add(EXTWRITE);
                newTable.setRequiredWriteCapabilities(requiredWrites);
              } else {
                LOG.debug("External unbucketed table without EXTREAD/WRITE capability:NONE");
                newTable.setAccessType(ACCESSTYPE_NONE);
                requiredReads.add(EXTREAD);
                requiredWrites.add(EXTWRITE);
                newTable.setRequiredWriteCapabilities(requiredWrites);
                newTable.setRequiredReadCapabilities(requiredReads);
              }
            }

            ret.put(newTable, generated);
            break;
          case "MANAGED_TABLE":
            String txnal = params.get(TABLE_IS_TRANSACTIONAL);
            if (txnal == null || txnal.equalsIgnoreCase("FALSE")) { // non-ACID MANAGED table
              LOG.debug("Managed non-acid table:RW");
              table.setAccessType(ACCESSTYPE_READWRITE);
              generated.addAll(acidWriteList);
            }

            if (txnal != null && txnal.equalsIgnoreCase("TRUE")) { // ACID table
              String txntype = params.get(TABLE_TRANSACTIONAL_PROPERTIES);
              if (txntype != null && txntype.equalsIgnoreCase("insert_only")) { // MICRO_MANAGED Tables
                // MGD table is insert only, not full ACID
                if (processorCapabilities.contains(HIVEMANAGEDINSERTWRITE) || processorCapabilities.contains(CONNECTORWRITE)) {
                  LOG.debug("Managed acid table with INSERTWRITE or CONNECTORWRITE capability:RW");
                  table.setAccessType(ACCESSTYPE_READWRITE); // clients have RW access to INSERT-ONLY ACID tables
                  processorCapabilities.retainAll(insertOnlyWriteList);
                  generated.addAll(processorCapabilities);
                  LOG.info("Processor has one of the write capabilities on insert-only, granting RW");
                } else if (processorCapabilities.contains(HIVEMANAGEDINSERTREAD) || processorCapabilities.contains(CONNECTORREAD)) {
                  LOG.debug("Managed acid table with INSERTREAD or CONNECTORREAD capability:RO");
                  table.setAccessType(ACCESSTYPE_READONLY); // clients have RO access to INSERT-ONLY ACID tables
                  generated.addAll(insertOnlyWriteList);
                  table.setRequiredWriteCapabilities(insertOnlyWriteList);
                  processorCapabilities.retainAll(getReads(insertOnlyList));
                  generated.addAll(processorCapabilities);
                  LOG.info("Processor has one of the read capabilities on insert-only, granting RO");
                } else {
                  table.setAccessType(ACCESSTYPE_NONE); // clients have NO access to INSERT-ONLY ACID tables
                  generated.addAll(acidList);
                  generated.addAll(insertOnlyList);
                  table.setRequiredWriteCapabilities(insertOnlyWriteList);
                  table.setRequiredReadCapabilities(Arrays.asList(CONNECTORREAD, HIVEMANAGEDINSERTREAD));
                  LOG.info("Processor has no read or write capabilities on insert-only, NO access");
                }
              } else { // FULL ACID MANAGED TABLE
                if (processorCapabilities.contains(HIVEFULLACIDWRITE) || processorCapabilities.contains(CONNECTORWRITE)) {
                  LOG.debug("Full acid table with ACIDWRITE or CONNECTORWRITE capability:RW");
                  table.setAccessType(ACCESSTYPE_READWRITE); // clients have RW access to IUD ACID tables
                  processorCapabilities.retainAll(acidWriteList);
                  generated.addAll(processorCapabilities);
                } else if (processorCapabilities.contains(HIVEFULLACIDREAD) || processorCapabilities.contains(CONNECTORREAD)) {
                  LOG.debug("Full acid table with ACIDREAD or CONNECTORREAD capability:RO");
                  table.setAccessType(ACCESSTYPE_READONLY); // clients have RO access to IUD ACID tables
                  generated.addAll(acidWriteList);
                  table.setRequiredWriteCapabilities(acidWriteList);
                  processorCapabilities.retainAll(getReads(acidList));
                  generated.addAll(processorCapabilities);
                } else {
                  LOG.debug("Full acid table without ACIDREAD/WRITE or CONNECTORREAD/WRITE capability:NONE");
                  table.setAccessType(ACCESSTYPE_NONE); // clients have NO access to IUD ACID tables
                  table.setRequiredWriteCapabilities(acidWriteList);
                  table.setRequiredReadCapabilities(Arrays.asList(CONNECTORREAD, HIVEFULLACIDREAD));
                  generated.addAll(acidList);
                }
              }
            }
            ret.put(table, generated);
            break;
          case "VIRTUAL_VIEW":
            if (processorCapabilities.contains(HIVESQL) ||
                processorCapabilities.contains(CONNECTORREAD)) {
              table.setAccessType(ACCESSTYPE_READONLY);
            } else {
              table.setAccessType(ACCESSTYPE_NONE);
              table.setRequiredReadCapabilities(generated);
              generated.add(HIVESQL);
              generated.add(CONNECTORREAD);
            }
            ret.put(table, generated);
            break;
          case "MATERIALIZED_VIEW":
            if ((processorCapabilities.contains(CONNECTORREAD) ||
                processorCapabilities.contains(HIVEFULLACIDREAD)) && processorCapabilities.contains(HIVEMQT)) {
              LOG.info("Processor has one of the READ abilities and HIVEMQT, AccessType=RO");
              table.setAccessType(ACCESSTYPE_READONLY);
              generated.addAll(diff(MQTLIST, processorCapabilities));
            } else {
              LOG.info("Processor has no READ abilities or HIVEMQT, AccessType=None");
              table.setAccessType(ACCESSTYPE_NONE);
              table.setRequiredReadCapabilities(Arrays.asList(CONNECTORREAD, HIVEMQT));
              generated.addAll(MQTLIST);
            }
            ret.put(table, generated);
            break;
          default:
            table.setAccessType(ACCESSTYPE_NONE);
            ret.put(table,generated);
            break;
        }
        continue;
      }

      // WITH CAPABLITIES ON TABLE
      tCapabilities = tCapabilities.replaceAll("\\s","").toUpperCase(); // remove spaces between tCapabilities + toUppercase
      List<String> requiredCapabilities = Arrays.asList(tCapabilities.split(","));
      switch (tableType) {
        case "EXTERNAL_TABLE":
          if (processorCapabilities.containsAll(requiredCapabilities)) {
            // AccessType is RW
            LOG.info("Abilities for match: Table type=" + tableType + ",accesstype is RW");
            table.setAccessType(ACCESSTYPE_READWRITE);
            ret.put(table, requiredCapabilities);
            break;
          }

          Table newTable = new Table(table);
          boolean removedBucketing = false;

          if (requiredCapabilities.contains(HIVEBUCKET2) && !processorCapabilities.contains(HIVEBUCKET2)) {
            StorageDescriptor newSd = new StorageDescriptor(table.getSd());
            newSd.setNumBuckets(-1); // removing bucketing if HIVEBUCKET2 isnt specified
            newTable.setSd(newSd);
            removedBucketing = true;
            newTable.setAccessType(ACCESSTYPE_READONLY);
            LOG.debug("Adding HIVEBUCKET2 to requiredWrites");
            requiredWrites.add(HIVEBUCKET2);
            LOG.info("Removed bucketing information from table");
          }

          if (requiredCapabilities.contains(EXTWRITE) && processorCapabilities.contains(EXTWRITE)) {
            if (!removedBucketing) {
              LOG.info("EXTWRITE Matches, accessType=" + ACCESSTYPE_READWRITE);
              newTable.setAccessType(ACCESSTYPE_READWRITE);
              ret.put(newTable, requiredCapabilities);
              continue;
            }
          }

          if (requiredCapabilities.contains(EXTREAD) && processorCapabilities.contains(EXTREAD)) {
            LOG.info("EXTREAD Matches, accessType=" + ACCESSTYPE_READONLY);
            newTable.setAccessType(ACCESSTYPE_READONLY);
            requiredWrites.add(EXTWRITE);
            newTable.setRequiredWriteCapabilities(requiredWrites);
          } else {
            LOG.debug("No matches, accessType=" + ACCESSTYPE_NONE);
            newTable.setAccessType(ACCESSTYPE_NONE);
            requiredReads.add(EXTREAD);
            requiredWrites.addAll(getWrites(requiredCapabilities));
            newTable.setRequiredReadCapabilities(requiredReads);
            newTable.setRequiredWriteCapabilities(requiredWrites);
          }

          LOG.info("setting required to " + requiredCapabilities);
          ret.put(newTable, requiredCapabilities);
          break;
        case "MANAGED_TABLE":
          if (processorCapabilities.size() == 0) { // processor has no capabilities
            LOG.info("Client has no capabilities for type " + tableType + ",accesstype is NONE");
            table.setAccessType(ACCESSTYPE_NONE);
            table.setRequiredReadCapabilities(getReads(requiredCapabilities));
            table.setRequiredWriteCapabilities(getWrites(requiredCapabilities));
            ret.put(table, requiredCapabilities);
            continue;
          }

          if (processorCapabilities.containsAll(requiredCapabilities)) {
            // AccessType is RW
            LOG.info("Abilities for match: Table type=" + tableType + ",accesstype is RW");
            table.setAccessType(ACCESSTYPE_READWRITE);
            ret.put(table, requiredCapabilities);
            continue;
          }

          String txnal = params.get(TABLE_IS_TRANSACTIONAL);
          if (txnal == null || txnal.equalsIgnoreCase("FALSE")) { // non-ACID MANAGED table
            LOG.info("Table is non ACID, accesstype is RO");
            table.setAccessType(ACCESSTYPE_READONLY);
            List<String> missing = diff(requiredCapabilities, processorCapabilities);
            table.setRequiredWriteCapabilities(getWrites(missing));
            ret.put(table, missing);
            continue;
          }

          if (txnal != null && txnal.equalsIgnoreCase("TRUE")) { // ACID table
            String txntype = params.get(TABLE_TRANSACTIONAL_PROPERTIES);
            List<String> hintList = new ArrayList<>();
            if (txntype != null && txntype.equalsIgnoreCase("insert_only")) { // MICRO_MANAGED Tables
              LOG.info("Table is INSERTONLY ACID");
              // MGD table is insert only, not full ACID
              if (processorCapabilities.containsAll(getWrites(requiredCapabilities)) // contains all writes on table
                   || processorCapabilities.contains(HIVEFULLACIDWRITE)) {
                LOG.info("Processor has all writes or full acid write, access is RW");
                table.setAccessType(ACCESSTYPE_READWRITE); // clients have RW access to INSERT-ONLY ACID tables
                ret.put(table, hintList);
                continue;
              }

              if (processorCapabilities.contains(CONNECTORWRITE)) {
                LOG.debug("Managed acid table with CONNECTORWRITE capability:RW");
                table.setAccessType(ACCESSTYPE_READWRITE); // clients have RW access to INSERT-ONLY ACID tables with CONNWRITE
                hintList.add(CONNECTORWRITE);
                hintList.addAll(getReads(requiredCapabilities));
                ret.put(table, hintList);
                continue;
              } else if (processorCapabilities.containsAll(getReads(requiredCapabilities))
                      || processorCapabilities.contains(HIVEMANAGEDINSERTREAD)) {
                LOG.debug("Managed acid table with MANAGEDREAD capability:RO");
                table.setAccessType(ACCESSTYPE_READONLY);
                table.setRequiredWriteCapabilities(diff(getWrites(requiredCapabilities), getWrites(processorCapabilities)));
                hintList.add(HIVEMANAGEDINSERTWRITE);
                hintList.addAll(diff(getWrites(requiredCapabilities), processorCapabilities));
                ret.put(table, hintList);
                continue;
              } else if (processorCapabilities.contains(CONNECTORREAD)) {
                LOG.debug("Managed acid table with CONNECTORREAD capability:RO");
                table.setAccessType(ACCESSTYPE_READONLY);
                table.setRequiredWriteCapabilities(diff(getWrites(requiredCapabilities), getWrites(processorCapabilities)));
                hintList.add(CONNECTORREAD);
                hintList.addAll(diff(getWrites(requiredCapabilities), processorCapabilities));
                ret.put(table, hintList);
                continue;
              } else {
                LOG.debug("Managed acid table without any READ capability:NONE");
                table.setAccessType(ACCESSTYPE_NONE);
                ret.put(table, requiredCapabilities);
                table.setRequiredWriteCapabilities(diff(getWrites(requiredCapabilities), getWrites(processorCapabilities)));
                table.setRequiredReadCapabilities(diff(getReads(requiredCapabilities), getReads(processorCapabilities)));
                continue;
              }
            } else { // MANAGED FULL ACID TABLES
              LOG.info("Table is FULLACID");
              if (processorCapabilities.containsAll(getWrites(requiredCapabilities)) // contains all writes on table
                  || processorCapabilities.contains(HIVEFULLACIDWRITE)) {
                LOG.info("Processor has all writes or atleast " + HIVEFULLACIDWRITE + ", access is RW");
                table.setAccessType(ACCESSTYPE_READWRITE); // clients have RW access to ACID tables
                hintList.add(HIVEFULLACIDWRITE);
                ret.put(table, hintList);
                continue;
              }

              if(processorCapabilities.contains(CONNECTORWRITE)) {
                LOG.debug("Full acid table with CONNECTORWRITE capability:RW");
                table.setAccessType(ACCESSTYPE_READWRITE); // clients have RW access to IUD ACID tables
                hintList.add(CONNECTORWRITE);
                hintList.addAll(getReads(requiredCapabilities));
                ret.put(table, hintList);
                continue;
              } else if (processorCapabilities.contains(HIVEFULLACIDREAD)
                     || (processorCapabilities.contains(CONNECTORREAD) )) {
                LOG.debug("Full acid table with CONNECTORREAD/ACIDREAD capability:RO");
                table.setAccessType(ACCESSTYPE_READONLY); // clients have RO access to IUD ACID tables
                table.setRequiredWriteCapabilities(diff(getWrites(requiredCapabilities), getWrites(processorCapabilities)));
                hintList.add(CONNECTORREAD);
                hintList.addAll(diff(getWrites(requiredCapabilities), processorCapabilities));
                ret.put(table, hintList);
                continue;
              } else {
                LOG.debug("Full acid table without READ capability:RO");
                table.setAccessType(ACCESSTYPE_NONE); // clients have NO access to IUD ACID tables
                table.setRequiredWriteCapabilities(diff(getWrites(requiredCapabilities), getWrites(processorCapabilities)));
                table.setRequiredReadCapabilities(diff(getReads(requiredCapabilities), getReads(processorCapabilities)));
                ret.put(table, requiredCapabilities);
                continue;
              }
            }
          }
          LOG.info("setting required to " + ret.get(table) + ",MANAGED:Access=" + table.getAccessType());
          break;
        case "VIRTUAL_VIEW":
        case "MATERIALIZED_VIEW":
          if (processorCapabilities.containsAll(requiredCapabilities)) {
            table.setAccessType(ACCESSTYPE_READONLY);
            ret.put(table, new ArrayList<String>());
          } else {
            table.setAccessType(ACCESSTYPE_NONE);
            table.setRequiredReadCapabilities(diff(getReads(requiredCapabilities), getReads(processorCapabilities)));
            ret.put(table, requiredCapabilities);
          }
          break;
        default:
          table.setAccessType(ACCESSTYPE_NONE);
          table.setRequiredReadCapabilities(getReads(requiredCapabilities));
          table.setRequiredWriteCapabilities(getWrites(requiredCapabilities));
          ret.put(table, requiredCapabilities);
          break;
      }
    }

    LOG.info("Transformer return list of " + ret.size());
    return ret;
  }

  @Override
  public List<Partition> transformPartitions(List<Partition> objects, Table table, List<String> processorCapabilities, String processorId) throws MetaException {
    if (processorCapabilities != null && processorCapabilities.contains(MANAGERAWMETADATA)) {
      return objects;
    }

    LOG.info("Starting translation for partition for processor " + processorId + " on list " + objects.size());
    List<Partition> ret = new ArrayList<>();
    int partBuckets = 0;

    for (Partition partition : objects) {
      String tableName = partition.getTableName();
      String dbName = partition.getDbName();

      Map<String, String> params = table.getParameters();
      if (params == null)
        params = new HashMap<>();
      String tableType = table.getTableType();
      String tCapabilities = params.get(OBJCAPABILITIES);
      if (partition.getSd() != null) {
        partBuckets = partition.getSd().getNumBuckets();
        LOG.info("Number of original part buckets=" + partBuckets);
      }

      if (tCapabilities == null) {
        LOG.debug("Table " + table.getTableName() + " has no specific required capabilities");

        switch (tableType) {
          case "EXTERNAL_TABLE":
            if (partBuckets > 0 && !processorCapabilities.contains(HIVEBUCKET2)) {
              Partition newPartition = new Partition(partition);
              StorageDescriptor newSd = new StorageDescriptor(partition.getSd());
              newSd.setNumBuckets(-1); // remove bucketing info
              newPartition.setSd(newSd);
              ret.add(newPartition);
            } else {
              ret.add(partition);
            }
            break;
	  case "MANAGED_TABLE":
            String txnal = params.get(TABLE_IS_TRANSACTIONAL);
            if (txnal == null || txnal.equalsIgnoreCase("FALSE")) { // non-ACID MANAGED table
              if (partBuckets > 0 && !processorCapabilities.contains(HIVEBUCKET2)) {
                Partition newPartition = new Partition(partition);
                StorageDescriptor newSd = new StorageDescriptor(partition.getSd());
                newSd.setNumBuckets(-1); // remove bucketing info
                newPartition.setSd(newSd);
                ret.add(newPartition);
                break;
              }
            }
            // INSERT or FULL ACID table, bucketing info to be retained
            ret.add(partition);
            break;
          default:
            ret.add(partition);
            break;
        }
      } else { // table has capabilities
        tCapabilities = tCapabilities.replaceAll("\\s","").toUpperCase(); // remove spaces between tCapabilities + toUppercase
        List<String> requiredCapabilities = Arrays.asList(tCapabilities.split(","));
        if (partBuckets <= 0 || processorCapabilities.containsAll(requiredCapabilities)) {
          ret.add(partition);
          continue;
        }

        switch (tableType) {
          case "EXTERNAL_TABLE":
            if (requiredCapabilities.contains(HIVEBUCKET2) && !processorCapabilities.contains(HIVEBUCKET2)) {
              Partition newPartition = new Partition(partition);
              StorageDescriptor newSd = new StorageDescriptor(partition.getSd());
              newSd.setNumBuckets(-1); // removing bucketing if HIVEBUCKET2 isnt specified
              newPartition.setSd(newSd);
              LOG.info("Removed bucketing information from partition");
              ret.add(newPartition);
              break;
            }
          case "MANAGED_TABLE":
            String txnal = params.get(TABLE_IS_TRANSACTIONAL);
            if (txnal == null || txnal.equalsIgnoreCase("FALSE")) { // non-ACID MANAGED table
              if (!processorCapabilities.contains(HIVEBUCKET2)) {
                Partition newPartition = new Partition(partition);
                StorageDescriptor newSd = new StorageDescriptor(partition.getSd());
                newSd.setNumBuckets(-1); // remove bucketing info
                newPartition.setSd(newSd);
                ret.add(newPartition);
                break;
              }
            }
            ret.add(partition);
            break;
          default:
            ret.add(partition);
            break;
        }
      }
    }
    LOG.info("Returning partition set of size " + ret.size());
    return ret;
  }

  @Override
  public Table transformCreateTable(Table table, List<String> processorCapabilities, String processorId) throws MetaException {
    Table newTable = new Table(table);
    LOG.info("Starting translation for CreateTable for processor " + processorId + " with " + processorCapabilities
        + " on table " + newTable.getTableName());
    Map<String, String> params = table.getParameters();
    if (params == null)
      params = new HashMap<>();
    String tableType = newTable.getTableType();
    String txnal = null;

    if (TableType.MANAGED_TABLE.name().equals(tableType)) {
      LOG.info("Table is a MANAGED_TABLE");
      txnal = params.get(TABLE_IS_TRANSACTIONAL);
      if (txnal == null || txnal.equalsIgnoreCase("FALSE")) { // non-ACID MANAGED TABLE
        if (processorCapabilities == null || (!processorCapabilities.contains(HIVEMANAGEDINSERTWRITE) &&
            !processorCapabilities.contains(HIVEFULLACIDWRITE))) {
          LOG.info("Converting " + newTable.getTableName() + " to EXTERNAL tableType for " + processorId);
          newTable.setTableType(TableType.EXTERNAL_TABLE.toString());
          params.remove(TABLE_IS_TRANSACTIONAL);
          params.remove(TABLE_TRANSACTIONAL_PROPERTIES);
          params.put("EXTERNAL", "TRUE");
          params.put(EXTERNAL_TABLE_PURGE, "TRUE");
          params.put("TRANSLATED_TO_EXTERNAL", "TRUE");
          newTable.setParameters(params);
          LOG.info("Modified table params are:" + params.toString());
          if (table.getSd().getLocation() == null) {
            try {
              Path newPath = hmsHandler.getWh().getDefaultTablePath(table.getDbName(), table.getTableName(), true);
              newTable.getSd().setLocation(newPath.toString());
              LOG.info("Modified location from to " + newPath);
            } catch (Exception e) {
              LOG.warn("Exception determining external table location:" + e.getMessage());
            }
          }
        }
      } else { // ACID table
        if (processorCapabilities == null || processorCapabilities.isEmpty()) {
          throw new MetaException("Processor has no capabilities, cannot create an ACID table.");
        }
        String txntype = params.get(TABLE_TRANSACTIONAL_PROPERTIES);
        if (txntype != null && txntype.equalsIgnoreCase("insert_only")) { // MICRO_MANAGED Tables
          if (processorCapabilities.contains(HIVEMANAGEDINSERTWRITE)) {
            LOG.info("Processor has required capabilities to be able to create INSERT-only tables");
            return newTable;
          } else {
            throw new MetaException("Processor does not have capabilities to create a INSERT ACID table:" +
                diff(insertOnlyWriteList, processorCapabilities));
          }
        } else { // FULL-ACID table
          if (processorCapabilities.contains(HIVEFULLACIDWRITE)) {
            LOG.info("Processor has required capabilities to be able to create FULLACID tables.");
            return newTable;
          } else {
            throw new MetaException("Processor does not have capabilities to create a FULL ACID table:" +
                diff(acidWriteList, processorCapabilities));
          }
        }
      }
    } else {
      LOG.info("Table to be created is of type " + tableType + " but not " + TableType.MANAGED_TABLE.toString());
    }
    LOG.info("Transformer returning table:" + newTable.toString());
    return newTable;
  }

  /**
   * Alter location of the database depending on whether or not the processor has ACID capabilities.
   */
  @Override
  public Database transformDatabase(Database db, List<String> processorCapabilities, String processorId) throws MetaException {
    if (processorCapabilities != null && processorCapabilities.contains(MANAGERAWMETADATA)) {
      return db;
    }

    LOG.info("Starting translation for transformDatabase for processor " + processorId + " with " + processorCapabilities
        + " on database " + db.getName());

    if (processorCapabilities == null || (!processorCapabilities.contains(HIVEMANAGEDINSERTWRITE) &&
            !processorCapabilities.contains(HIVEFULLACIDWRITE))) {
      LOG.info("Processor does not have any of ACID write capabilities, changing current location from " +
              db.getLocationUri() + " to external warehouse location");
      Path extWhLocation = hmsHandler.getWh().getDefaultExternalDatabasePath(db.getName());
      LOG.debug("Setting DBLocation to " + extWhLocation.toString());
      db.setLocationUri(extWhLocation.toString());
    }
    LOG.info("Transformer returning database:" + db.toString());
    return db;
  }

  // returns the elements contained in list1 but missing in list2
  private List<String> diff(final List<String> list1, final List<String> list2) {
    List<String> diffList = new ArrayList<>();

    if (list2 == null || list2.size() == 0)
      return list1;

    if (list1 == null || list1.size() == 0)
      return new ArrayList<String>();

    if (list2.containsAll(list1))
      return new ArrayList<String>();

    diffList.addAll(list2);
    LOG.debug("diffList=" + Arrays.toString(diffList.toArray()) + ",master list=" + Arrays.toString(list1.toArray()));
    if (diffList.retainAll(list1)) {
      LOG.debug("diffList=" + Arrays.toString(diffList.toArray()));
      if (diffList.size() == list1.size()) { // lists match
        return new ArrayList<String>(); // return empty list indicating no missing elements
      } else {
        list1.removeAll(diffList);
        LOG.debug("list1.size():" + list1.size());
        return list1;
      }
    } else {
      list1.removeAll(diffList);
      LOG.debug("diff returning " + Arrays.toString(diffList.toArray()) + ",full list=" + Arrays.toString(list1.toArray()));
      return list1;
    }
  }

  private List<String> getWrites(List<String> capabilities) {
    List<String> writes = new ArrayList<>();
    for (String capability : capabilities) {
      if (capability.toUpperCase().endsWith("WRITE") ||
          capability.toUpperCase().endsWith("STATS") ||
          capability.toUpperCase().endsWith("INVALIDATE")) {
        writes.add(capability);
      }
    }
    return writes;
  }

  private List<String> getReads(List<String> capabilities) {
    List<String> reads = new ArrayList<>();
    for (String capability : capabilities) {
      if (capability.toUpperCase().endsWith("READ") ||
          capability.toUpperCase().endsWith("SQL")) {
        reads.add(capability);
      }
    }
    return reads;
  }
}
