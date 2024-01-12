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
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.CTAS_LEGACY_CONFIG;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_TRANSACTIONAL;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.EXTERNAL_TABLE_PURGE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetastoreDefaultTransformer implements IMetaStoreMetadataTransformer {
  public static final Logger LOG = LoggerFactory.getLogger(MetastoreDefaultTransformer.class);
  private IHMSHandler hmsHandler = null;
  private String defaultCatalog = null;
  private boolean isTenantBasedStorage = false;

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
  private static final String ACCEPTSUNMODIFIEDMETADATA = "ACCEPTS_UNMODIFIED_METADATA".intern();
  private static final String EXTERNALTABLESONLY = "EXTERNAL_TABLES_ONLY".intern();

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
    this.defaultCatalog = MetaStoreUtils.getDefaultCatalog(handler.getConf());
    this.isTenantBasedStorage = hmsHandler.getConf().getBoolean(MetastoreConf.ConfVars.ALLOW_TENANT_BASED_STORAGE.getVarname(), false);

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
      List<String> generated = new ArrayList<String>();
      List<String> requiredReads = new ArrayList<>();
      List<String> requiredWrites = new ArrayList<>();

      if (!defaultCatalog.equalsIgnoreCase(table.getCatName())) {
        ret.put(table, generated);
        continue;
      }

      Map<String, String> params = table.getParameters();
      String tableType = table.getTableType();
      String tCapabilities = params.get(OBJCAPABILITIES);
      int numBuckets = table.isSetSd()? table.getSd().getNumBuckets() : 0;
      boolean isBucketed = (numBuckets > 0) ? true : false;

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
                if (!processorCapabilities.contains(ACCEPTSUNMODIFIEDMETADATA)) {
                  LOG.debug("Bucketed table without HIVEBUCKET2 capability, removed bucketing info from table");
                  newSd.setNumBuckets(-1); // remove bucketing info
                }
                newTable.setSd(newSd);
                newTable.setRequiredWriteCapabilities(requiredWrites);
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
          if (requiredCapabilities.contains(HIVEBUCKET2) && !processorCapabilities.contains(HIVEBUCKET2)) {
            StorageDescriptor newSd = new StorageDescriptor(table.getSd());
            if (!processorCapabilities.contains(ACCEPTSUNMODIFIEDMETADATA)) {
              newSd.setNumBuckets(-1); // removing bucketing if HIVEBUCKET2 isnt specified
              LOG.debug("Bucketed table without HIVEBUCKET2 capability, removed bucketing info from table");
            }
            newTable.setSd(newSd);
            newTable.setAccessType(ACCESSTYPE_READONLY);
            LOG.debug("Adding HIVEBUCKET2 to requiredWrites");
            requiredWrites.add(HIVEBUCKET2);
          }

          if (requiredCapabilities.contains(EXTWRITE) && processorCapabilities.contains(EXTWRITE)) {
            if (!isBucketed) {
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
    if ((processorCapabilities != null && processorCapabilities.contains(MANAGERAWMETADATA)) ||
        !defaultCatalog.equalsIgnoreCase(table.getCatName())) {
      LOG.debug("Table belongs to non-default catalog, skipping translation");
      return objects;
    }

    LOG.info("Starting translation for partition for processor " + processorId + " on list " + objects.size());
    List<Partition> ret = new ArrayList<>();
    int partBuckets = 0;

    for (Partition partition : objects) {
      String tableName = partition.getTableName();
      String dbName = partition.getDbName();

      Map<String, String> params = table.getParameters();
      if (params == null) {
        params = new HashMap<>();
      }
      String tableType = table.getTableType();
      String tCapabilities = params.get(OBJCAPABILITIES);
      if (partition.getSd() != null) {
        partBuckets = partition.getSd().getNumBuckets();
        LOG.debug("Number of original part buckets=" + partBuckets);
      } else {
        partBuckets = 0;
      }

      if (tCapabilities == null) {
        LOG.debug("Table " + table.getTableName() + " has no specific required capabilities");

        switch (tableType) {
          case "EXTERNAL_TABLE":
          if (partBuckets > 0 && !processorCapabilities.contains(HIVEBUCKET2)) {
            Partition newPartition = new Partition(partition);
            StorageDescriptor newSd = new StorageDescriptor(partition.getSd());
            if (!processorCapabilities.contains(ACCEPTSUNMODIFIEDMETADATA)) {
              newSd.setNumBuckets(-1); // remove bucketing info
            }
            newPartition.setSd(newSd);
            ret.add(newPartition);
          } else {
            ret.add(partition);
          }
          break;
          case "MANAGED_TABLE":
          String txnal = params.get(TABLE_IS_TRANSACTIONAL);
          if (txnal == null || "FALSE".equalsIgnoreCase(txnal)) { // non-ACID MANAGED table
            if (partBuckets > 0 && !processorCapabilities.contains(HIVEBUCKET2)) {
              Partition newPartition = new Partition(partition);
              StorageDescriptor newSd = new StorageDescriptor(partition.getSd());
              if (!processorCapabilities.contains(ACCEPTSUNMODIFIEDMETADATA)) {
                newSd.setNumBuckets(-1); // remove bucketing info
              }
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
              if (!processorCapabilities.contains(ACCEPTSUNMODIFIEDMETADATA)) {
                newSd.setNumBuckets(-1); // removing bucketing if HIVEBUCKET2 isnt specified
              }
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
                if (!processorCapabilities.contains(ACCEPTSUNMODIFIEDMETADATA)) {
                  newSd.setNumBuckets(-1); // remove bucketing info
                }
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

  static enum TableLocationStrategy {
    seqprefix {
      @Override
      Path getLocation(IHMSHandler hmsHandler, Database db, Table table, int idx) throws MetaException {
        if (idx == 0) {
          return getDefaultPath(hmsHandler, db, table.getTableName());
        }
        return getDefaultPath(hmsHandler, db, idx + "_" + table.getTableName());
      }
    },
    seqsuffix {
      @Override
      Path getLocation(IHMSHandler hmsHandler, Database db, Table table, int idx) throws MetaException {
        if (idx == 0) {
          return getDefaultPath(hmsHandler, db, table.getTableName());
        }
        return getDefaultPath(hmsHandler, db, table.getTableName() + "_" + idx);
      }
    },
    prohibit {
      @Override
      Path getLocation(IHMSHandler hmsHandler, Database db, Table table, int idx) throws MetaException {
        Path p = getDefaultPath(hmsHandler, db, table.getTableName());

        if (idx == 0) {
          return p;
        }
        throw new MetaException("Default location is not available for table: " + p);
      }
    },
    force {
      @Override
      Path getLocation(IHMSHandler hmsHandler, Database db, Table table, int idx) throws MetaException {
        Path p = getDefaultPath(hmsHandler, db, table.getTableName());
        return p;
      }

    };

    private static final Path getDefaultPath(IHMSHandler hmsHandler, Database db, String tableName)
        throws MetaException {
      return hmsHandler.getWh().getDefaultTablePath(db, tableName, true);
    }

    abstract Path getLocation(IHMSHandler hmsHandler, Database db, Table table, int idx) throws MetaException;

  }

  @Override
  public Table transformCreateTable(Table table, List<String> processorCapabilities, String processorId) throws MetaException {
    if (!defaultCatalog.equalsIgnoreCase(table.getCatName())) {
      LOG.debug("Table belongs to non-default catalog, skipping");
      return table;
    }

    Table newTable = new Table(table);
    LOG.info("Starting translation for CreateTable for processor " + processorId + " with " + processorCapabilities
        + " on table " + newTable.getTableName());
    Map<String, String> params = table.getParameters();
    if (params == null) {
      params = new HashMap<>();
    }
    String tableType = newTable.getTableType();
    String txnal = null;
    String txn_properties = null;
    boolean isInsertAcid = false;

    String dbName = table.getDbName();
    Database db = null;
    try {
      db = hmsHandler.get_database_core(table.getCatName(), table.getDbName());
    } catch (NoSuchObjectException e) {
      throw new MetaException("Database " + dbName + " for table " + table.getTableName() + " could not be found");
    }

      if (TableType.MANAGED_TABLE.name().equals(tableType)) {
      LOG.debug("Table is a MANAGED_TABLE");
      txnal = params.get(TABLE_IS_TRANSACTIONAL);
      txn_properties = params.get(TABLE_TRANSACTIONAL_PROPERTIES);
      isInsertAcid = (txn_properties != null && txn_properties.equalsIgnoreCase("insert_only"));
      boolean ctas_legacy_config = params.containsKey(CTAS_LEGACY_CONFIG) && params.get(CTAS_LEGACY_CONFIG).equalsIgnoreCase("true") ? true : false;
      if (((txnal == null || txnal.equalsIgnoreCase("FALSE")) && !isInsertAcid) || (ctas_legacy_config && (txnal == null || txnal.equalsIgnoreCase("FALSE")))) { // non-ACID MANAGED TABLE
        LOG.info("Converting " + newTable.getTableName() + " to EXTERNAL tableType for " + processorId);
        newTable.setTableType(TableType.EXTERNAL_TABLE.toString());
        params.remove(TABLE_IS_TRANSACTIONAL);
        params.remove(TABLE_TRANSACTIONAL_PROPERTIES);
        params.put(HiveMetaHook.EXTERNAL, "TRUE");
        params.put(EXTERNAL_TABLE_PURGE, "TRUE");
        params.put(HiveMetaHook.TRANSLATED_TO_EXTERNAL, "TRUE");
        newTable.setParameters(params);
        LOG.info("Modified table params are:" + params.toString());

        if (getLocation(table) == null) {
          try {
            Path location = getTranslatedToExternalTableDefaultLocation(db, newTable);
            newTable.getSd().setLocation(location.toString());
          } catch (Exception e) {
            throw new MetaException("Exception determining external table location:" + e.getMessage());
          }
        } else {
          // table with explicitly set location
          // has "translated" properties and will be removed on drop
          // should we check tbl directory existence?
        }
      } else { // ACID table
        // if the property 'EXTERNAL_TABLES_ONLY'='true' is set on the database, then creating managed/ACID tables are prohibited. See HIVE-25724 for more details.
        if (db.getParameters().containsKey(EXTERNALTABLESONLY) &&
                db.getParameters().get(EXTERNALTABLESONLY).equalsIgnoreCase("true")) {
          throw new MetaException("Creation of ACID table is not allowed when the property 'EXTERNAL_TABLES_ONLY'='TRUE' is set on the database.");
        }

        if (processorCapabilities == null || processorCapabilities.isEmpty()) {
          throw new MetaException("Processor has no capabilities, cannot create an ACID table.");
        }


        newTable = validateTablePaths(table);
        if (isInsertAcid) { // MICRO_MANAGED Tables
          if (processorCapabilities.contains(HIVEMANAGEDINSERTWRITE)) {
            LOG.debug("Processor has required capabilities to be able to create INSERT-only tables");
            return newTable;
          } else {
            throw new MetaException("Processor does not have capabilities to create a INSERT ACID table:" +
                diff(insertOnlyWriteList, processorCapabilities));
          }
        } else { // FULL-ACID table
          if (processorCapabilities.contains(HIVEFULLACIDWRITE)) {
            LOG.debug("Processor has required capabilities to be able to create FULLACID tables.");
            return newTable;
          } else {
            throw new MetaException("Processor does not have capabilities to create a FULL ACID table:" +
                diff(acidWriteList, processorCapabilities));
          }
        }
      }
    } else if (TableType.EXTERNAL_TABLE.name().equals(tableType)) {
      LOG.debug("Table to be created is of type " + tableType);
      newTable = validateTablePaths(table);
    }
    LOG.info("Transformer returning table:" + newTable.toString());
    return newTable;
  }

  private Path getTranslatedToExternalTableDefaultLocation(Database db, Table table) throws MetaException {
    String strategyVar =
        MetastoreConf.getVar(hmsHandler.getConf(), ConfVars.METASTORE_METADATA_TRANSFORMER_LOCATION_MODE);
    TableLocationStrategy strategy = TableLocationStrategy.valueOf(strategyVar);
    int idx = 0;
    Path location = null;
    while (true) {
      location = strategy.getLocation(hmsHandler, db, table, idx++);
      if (strategy == TableLocationStrategy.force || !hmsHandler.getWh().isDir(location)) {
        break;
      }
    }
    LOG.info("Using location {} for table {}", location, table.getTableName());
    return location;
  }

  private Path getLocation(Table table) {
    if (table.isSetSd() && StringUtils.isNotBlank(table.getSd().getLocation())) {
      return new Path(table.getSd().getLocation());
    }
    return null;
  }

  @Override
  public Table transformAlterTable(Table oldTable, Table newTable, List<String> processorCapabilities,
      String processorId) throws MetaException {
    if (!defaultCatalog.equalsIgnoreCase(newTable.getCatName())) {
      LOG.debug("Table belongs to non-default catalog, skipping translation");
      return newTable;
    }

    LOG.info("Starting translation for Alter table for processor " + processorId + " with " + processorCapabilities
        + " on table " + newTable.getTableName());


    if (tableLocationChanged(oldTable, newTable)) {
      validateTablePaths(newTable);
    }

    Database oldDb = getDbForTable(oldTable);
    boolean isTranslatedToExternalFollowsRenames = MetastoreConf.getBoolVar(hmsHandler.getConf(),
        ConfVars.METASTORE_METADATA_TRANSFORMER_TRANSLATED_TO_EXTERNAL_FOLLOWS_RENAMES);

    if (isTranslatedToExternalFollowsRenames && isTableRename(oldTable, newTable)
        && isTranslatedToExternalTable(oldTable)
        && isTranslatedToExternalTable(newTable)) {
      Database newDb = getDbForTable(newTable);
      Path oldPath = TableLocationStrategy.getDefaultPath(hmsHandler, oldDb, oldTable.getTableName());
      if (oldTable.getSd().getLocation().equals(oldPath.toString())) {
        Path newPath = getTranslatedToExternalTableDefaultLocation(newDb, newTable);
        newTable.getSd().setLocation(newPath.toString());
      }
    }

    LOG.debug("Transformer returning table:" + newTable.toString());
    return newTable;
  }

  private Database getDbForTable(Table oldTable) throws MetaException {
    try {
      return hmsHandler.get_database_core(oldTable.getCatName(), oldTable.getDbName());
    } catch (NoSuchObjectException e) {
      throw new MetaException(
          "Database " + oldTable.getDbName() + " for table " + oldTable.getTableName() + " could not be found");
    }
  }

  private boolean isTableRename(Table oldTable, Table newTable) {
    return !MetaStoreUtils.getTableNameFor(oldTable).equals(MetaStoreUtils.getTableNameFor(newTable));
  }

  private boolean isTranslatedToExternalTable(Table table) {
    Map<String, String> p = table.getParameters();
    return p != null && MetaStoreUtils.isPropertyTrue(p, HiveMetaHook.EXTERNAL)
        && MetaStoreUtils.isPropertyTrue(p, HiveMetaHook.TRANSLATED_TO_EXTERNAL) && table.getSd() != null
        && table.getSd().isSetLocation();
  }

  private boolean tableLocationChanged(Table oldTable, Table newTable) throws MetaException {
    if (!newTable.isSetSd() || newTable.getSd().getLocation() == null) {
      return false;
    }
    if (!oldTable.isSetSd() || oldTable.getSd().getLocation() == null) {
      return false;
    }
    return !oldTable.getSd().getLocation().equals(newTable.getSd().getLocation());
  }

  /**
   * Alter location of the database depending on whether or not the processor has ACID capabilities.
   */
  @Override
  public Database transformDatabase(Database db, List<String> processorCapabilities, String processorId) throws MetaException {
    if ((processorCapabilities != null && processorCapabilities.contains(MANAGERAWMETADATA)) ||
        !defaultCatalog.equalsIgnoreCase(db.getCatalogName())) {
      LOG.debug("Database belongs to non-default catalog, skipping translation");
      return db;
    }

    LOG.info("Starting translation for transformDatabase for processor " + processorId + " with " + processorCapabilities
        + " on database {} locationUri={} managedLocationUri={}", db.getName(), db.getLocationUri(), db.getManagedLocationUri());

    if (!isTenantBasedStorage) {
      // for legacy DBs, location could have been managed or external. So if it is pointing to managed location, set it as
      // managed location and return a new default external path for location
      Path locationPath = Path.getPathWithoutSchemeAndAuthority(new Path(db.getLocationUri()));
      Path whRootPath = Path.getPathWithoutSchemeAndAuthority(hmsHandler.getWh().getWhRoot());
      LOG.debug("Comparing DB and warehouse paths warehouse={} db.getLocationUri={}", whRootPath.toString(), locationPath.toString());
      if (FileUtils.isSubdirectory(whRootPath.toString(), locationPath.toString()) || locationPath.equals(whRootPath)) { // legacy path
        if (processorCapabilities != null && (processorCapabilities.contains(HIVEMANAGEDINSERTWRITE) ||
            processorCapabilities.contains(HIVEFULLACIDWRITE))) {
          LOG.debug("Processor has atleast one of ACID write capabilities, setting current locationUri " + db.getLocationUri() + " as managedLocationUri");
          db.setManagedLocationUri(new Path(db.getLocationUri()).toString());
        }
        Path extWhLocation = hmsHandler.getWh().getDefaultExternalDatabasePath(db.getName());
        LOG.info("Database's location is a managed location, setting to a new default path based on external warehouse path:" + extWhLocation.toString());
        db.setLocationUri(extWhLocation.toString());
      }
    }
    LOG.info("Transformer returning database:" + db.toString());
    return db;
  }

  // returns the elements contained in list1 but missing in list2
  private List<String> diff(final List<String> list1, final List<String> list2) {
    List<String> diffList = new ArrayList<>();

    if (list2 == null || list2.size() == 0) {
      return list1;
    }

    if (list1 == null || list1.size() == 0) {
      return Collections.emptyList();
    }

    if (list2.containsAll(list1)) {
      return Collections.emptyList();
    }

    diffList.addAll(list2);
    LOG.debug("diffList=" + Arrays.toString(diffList.toArray()) + ",master list=" + Arrays.toString(list1.toArray()));
    if (diffList.retainAll(list1)) {
      LOG.debug("diffList=" + Arrays.toString(diffList.toArray()));
      if (diffList.size() == list1.size()) { // lists match
        return Collections.emptyList(); // return empty list indicating no missing elements
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

  private Table validateTablePaths(Table table) throws MetaException {
    Database db = null;
    String tableLocation = table.isSetSd()? table.getSd().getLocation() : null;
    try {
      db = hmsHandler.get_database_core(table.getCatName(), table.getDbName());
    } catch (NoSuchObjectException e) {
      throw new MetaException("Database " + table.getDbName() + " for table " + table.getTableName() + " could not be found");
    }

    if (TableType.MANAGED_TABLE.name().equals(table.getTableType())) {
      if (db.getManagedLocationUri() != null) {
        if (tableLocation != null) {
          if (!FileUtils.isSubdirectory(db.getManagedLocationUri(), tableLocation)) {
            throw new MetaException(
                "Illegal location for managed table, it has to be within database's managed location");
          }
        } else {
          Path path = hmsHandler.getWh().getDefaultTablePath(db, table.getTableName(), false);
          table.getSd().setLocation(path.toString());
          return table;
        }
      } else {
        if (tableLocation != null) {
          Path tablePath = Path.getPathWithoutSchemeAndAuthority(new Path(tableLocation));
          if (!FileUtils.isSubdirectory(hmsHandler.getWh().getWhRoot().toString(), tableLocation)) {
            throw new MetaException(
                "A managed table's location should be located within managed warehouse root directory or within its database's "
                    + "managedLocationUri. Table " + table.getTableName() + "'s location is not valid:" + tableLocation
                    + ", managed warehouse:" + hmsHandler.getWh().getWhRoot());
          }
        } else {
          Path path = hmsHandler.getWh().getDefaultManagedTablePath(db, table.getTableName());
          table.getSd().setLocation(path.toString());
        }
      }
    } else { // EXTERNAL TABLE
      Path whRootPath = Path.getPathWithoutSchemeAndAuthority(hmsHandler.getWh().getWhRoot());
      Path dbLocation = Path.getPathWithoutSchemeAndAuthority(new Path(db.getLocationUri()));
      LOG.debug("ValidateTablePaths: whRoot={} dbLocation={} tableLocation={} ", whRootPath.toString(), dbLocation.toString(), tableLocation);
      if (tableLocation != null) {
        Path tablePath = Path.getPathWithoutSchemeAndAuthority(new Path(tableLocation));
        if (isTenantBasedStorage) {
          if (!FileUtils.isSubdirectory(dbLocation.toString(), tablePath.toString())) { // location outside dblocation
            throw new MetaException(
                "An external table's location should not be located outside the location specified on its database, table:"
                    + table.getTableName() + ",location:" + tablePath + ",Database location for external tables:" + dbLocation);
          }

          dbLocation = Path.getPathWithoutSchemeAndAuthority(hmsHandler.getWh().getDatabaseManagedPath(db));
          if (dbLocation != null && FileUtils.isSubdirectory(dbLocation.toString(), tablePath.toString())) {
            throw new MetaException(
                "An external table's location should not be located within managed warehouse root directory of its database, table:"
                    + table.getTableName() + ",location:" + tablePath + ",Database's managed warehouse:" + dbLocation);
          }
        } else {
          if (isExternalWarehouseSet() && FileUtils.isSubdirectory(whRootPath.toString(), tablePath.toString())) {
            throw new MetaException(
                "An external table's location should not be located within managed warehouse root directory, table:"
                    + table.getTableName() + ",location:" + tablePath + ",managed warehouse:" + whRootPath);
          }
          return table;
        }
      } else {
        Path tablePath = hmsHandler.getWh().getDefaultTablePath(db, table.getTableName(), true);
        table.getSd().setLocation(tablePath.toString());
      }
    }
    return table;
  }

  private boolean isExternalWarehouseSet() {
    return hmsHandler.getConf().get(MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname()) != null;
  }
}
