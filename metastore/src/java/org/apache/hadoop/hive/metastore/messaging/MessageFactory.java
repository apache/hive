/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.messaging;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.Iterator;
import java.util.Map;

/**
 * Abstract Factory for the construction of HCatalog message instances.
 */
public abstract class MessageFactory {

  // Common name constants for event messages
  public static final String ADD_PARTITION_EVENT = "ADD_PARTITION";
  public static final String ALTER_PARTITION_EVENT = "ALTER_PARTITION";
  public static final String DROP_PARTITION_EVENT = "DROP_PARTITION";
  public static final String CREATE_TABLE_EVENT = "CREATE_TABLE";
  public static final String ALTER_TABLE_EVENT = "ALTER_TABLE";
  public static final String DROP_TABLE_EVENT = "DROP_TABLE";
  public static final String CREATE_DATABASE_EVENT = "CREATE_DATABASE";
  public static final String DROP_DATABASE_EVENT = "DROP_DATABASE";
  public static final String INSERT_EVENT = "INSERT";
  public static final String CREATE_FUNCTION_EVENT = "CREATE_FUNCTION";
  public static final String DROP_FUNCTION_EVENT = "DROP_FUNCTION";
  public static final String CREATE_INDEX_EVENT = "CREATE_INDEX";
  public static final String DROP_INDEX_EVENT = "DROP_INDEX";
  public static final String ALTER_INDEX_EVENT = "ALTER_INDEX";


  private static MessageFactory instance = null;

  protected static final HiveConf hiveConf = new HiveConf();
  static {
    hiveConf.addResource("hive-site.xml");
  }

  // This parameter is retained for legacy reasons, in case someone implemented custom
  // factories. This, however, should not be the case, since this api was intended to
  // be internal-only, and we should manage the jms and json implementations without
  // needing this parameter. Marking as deprecated, for removal by 2.4 - see corresponding
  // note on the getDeserializer(String,String) method
  @Deprecated
  private static final String CONF_LABEL_HCAT_MESSAGE_FACTORY_IMPL_PREFIX = "hcatalog.message.factory.impl.";

  protected static final String MS_SERVER_URL = hiveConf.get(HiveConf.ConfVars.METASTOREURIS.name(), "");
  protected static final String MS_SERVICE_PRINCIPAL = hiveConf.get(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.name(), "");

  /**
   * Getter for MessageFactory instance.
   */
  public static MessageFactory getInstance() {
    if (instance == null) {
      instance =
          getInstance(hiveConf.get(HiveConf.ConfVars.METASTORE_EVENT_MESSAGE_FACTORY.varname));
    }
    return instance;
  }

  private static MessageFactory getInstance(String className) {
    try {
      return (MessageFactory)ReflectionUtils.newInstance(JavaUtils.loadClass(className), hiveConf);
    }
    catch (ClassNotFoundException classNotFound) {
      throw new IllegalStateException("Could not construct MessageFactory implementation: ", classNotFound);
    }
  }

  /**
   * Getter for MessageDeserializer, corresponding to the specified format and version.
   * @param format Serialization format for notifications.
   * @param version Version of serialization format (currently ignored.)
   * @return MessageDeserializer.
   */
  public static MessageDeserializer getDeserializer(String format,
                            String version) {
    return getInstance(hiveConf.get(CONF_LABEL_HCAT_MESSAGE_FACTORY_IMPL_PREFIX + format,
        HiveConf.ConfVars.METASTORE_EVENT_MESSAGE_FACTORY.varname)).getDeserializer();
    // Note : The reason this method exists outside the no-arg getDeserializer method is in
    // case there is a user-implemented MessageFactory that's used, and some the messages
    // are in an older format and the rest in another. Then, what MessageFactory is default
    // is irrelevant, we should always use the one that was used to create it to deserialize.
    //
    // There exist only 2 implementations of this - json and jms
    //
    // Additional note : rather than as a config parameter, does it make sense to have
    // this use jdbc-like semantics that each MessageFactory made available register
    // itself for discoverability? Might be worth pursuing.
  }

  public abstract MessageDeserializer getDeserializer();

  /**
   * Getter for message-format.
   */
  public abstract String getMessageFormat();

  /**
   * Factory method for CreateDatabaseMessage.
   * @param db The Database being added.
   * @return CreateDatabaseMessage instance.
   */
  public abstract CreateDatabaseMessage buildCreateDatabaseMessage(Database db);

  /**
   * Factory method for DropDatabaseMessage.
   * @param db The Database being dropped.
   * @return DropDatabaseMessage instance.
   */
  public abstract DropDatabaseMessage buildDropDatabaseMessage(Database db);

  /**
   * Factory method for CreateTableMessage.
   * @param table The Table being created.
   * @param files Iterator of files
   * @return CreateTableMessage instance.
   */
  public abstract CreateTableMessage buildCreateTableMessage(Table table, Iterator<String> files);

  /**
   * Factory method for AlterTableMessage.  Unlike most of these calls, this one can return null,
   * which means no message should be sent.  This is because there are many flavors of alter
   * table (add column, add partition, etc.).  Some are covered elsewhere (like add partition)
   * and some are not yet supported.
   * @param before The table before the alter
   * @param after The table after the alter
   * @param isTruncateOp Flag to denote truncate table
   * @return
   */
  public abstract AlterTableMessage buildAlterTableMessage(Table before, Table after, boolean isTruncateOp);

  /**
   * Factory method for DropTableMessage.
   * @param table The Table being dropped.
   * @return DropTableMessage instance.
   */
  public abstract DropTableMessage buildDropTableMessage(Table table);

    /**
     * Factory method for AddPartitionMessage.
     * @param table The Table to which the partitions are added.
     * @param partitions The iterator to set of Partitions being added.
     * @param partitionFiles The iterator of partition files
     * @return AddPartitionMessage instance.
     */
  public abstract AddPartitionMessage buildAddPartitionMessage(Table table, Iterator<Partition> partitions,
      Iterator<PartitionFiles> partitionFiles);

  /**
   * Factory method for building AlterPartitionMessage
   * @param table The table in which the partition is being altered
   * @param before The partition before it was altered
   * @param after The partition after it was altered
   * @param isTruncateOp Flag to denote truncate partition
   * @return a new AlterPartitionMessage
   */
  public abstract AlterPartitionMessage buildAlterPartitionMessage(Table table, Partition before,
                                                                   Partition after, boolean isTruncateOp);

  /**
   * Factory method for DropPartitionMessage.
   * @param table The Table from which the partition is dropped.
   * @param partitions The set of partitions being dropped.
   * @return DropPartitionMessage instance.
   */
  public abstract DropPartitionMessage buildDropPartitionMessage(Table table, Iterator<Partition> partitions);

  /**
   * Factory method for CreateFunctionMessage.
   * @param fn The Function being added.
   * @return CreateFunctionMessage instance.
   */
  public abstract CreateFunctionMessage buildCreateFunctionMessage(Function fn);

  /**
   * Factory method for DropFunctionMessage.
   * @param fn The Function being dropped.
   * @return DropFunctionMessage instance.
   */
  public abstract DropFunctionMessage buildDropFunctionMessage(Function fn);

  /**
   * Factory method for CreateIndexMessage.
   * @param idx The Index being added.
   * @return CreateIndexMessage instance.
   */
  public abstract CreateIndexMessage buildCreateIndexMessage(Index idx);

  /**
   * Factory method for DropIndexMessage.
   * @param idx The Index being dropped.
   * @return DropIndexMessage instance.
   */
  public abstract DropIndexMessage buildDropIndexMessage(Index idx);

  /**
   * Factory method for AlterIndexMessage.
   * @param before The index before the alter
   * @param after The index after the alter
   * @return AlterIndexMessage
   */
  public abstract AlterIndexMessage buildAlterIndexMessage(Index before, Index after);

  /**
   * Factory method for building insert message
   *
   * @param db Name of the database the insert occurred in
   * @param table Name of the table the insert occurred in
   * @param partVals Partition values for the partition that the insert occurred in, may be null if
   *          the insert was done into a non-partitioned table
   * @param replace Flag to represent if INSERT OVERWRITE or INSERT INTO
   * @param files Iterator of file created
   * @return instance of InsertMessage
   */
  public abstract InsertMessage buildInsertMessage(String db, String table,
      Map<String, String> partVals, boolean replace, Iterator<String> files);
}
