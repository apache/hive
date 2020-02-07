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

package org.apache.hadoop.hive.metastore.tools;

import com.google.common.base.Joiner;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Helper utilities. The Util class is just a placeholder for static methods,
 * it should be never instantiated.
 */
public final class Util {
  private static final String DEFAULT_TYPE = "string";
  private static final String TYPE_SEPARATOR = ":";
  private static final String THRIFT_SCHEMA = "thrift";
  static final String DEFAULT_HOST = "localhost";
  private static final String ENV_SERVER = "HMS_HOST";
  private static final String ENV_PORT = "HMS_PORT";
  private static final String PROP_HOST = "hms.host";
  private static final String PROP_PORT = "hms.port";

  private static final String HIVE_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.HiveInputFormat";
  private static final String HIVE_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.HiveOutputFormat";
  private static final String LAZY_SIMPLE_SERDE = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

  private static final Pattern[] EMPTY_PATTERN = new Pattern[]{};
  private static final Pattern[] MATCH_ALL_PATTERN = new Pattern[]{Pattern.compile(".*")};

  private static final Logger LOG = LoggerFactory.getLogger(Util.class);

  // Disable public constructor
  private Util() {
  }

  /**
   * Wrapper that moves all checked exceptions to RuntimeException.
   *
   * @param throwingSupplier Supplier that throws Exception
   * @param <T>              Supplier return type
   * @return Supplier that throws unchecked exception
   */
  public static <T> T throwingSupplierWrapper(ThrowingSupplier<T, Exception> throwingSupplier) {
    try {
      return throwingSupplier.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Version of the Supplier that can throw exceptions.
   *
   * @param <T> Supplier return type
   * @param <E> Exception type
   */
  @FunctionalInterface
  public interface ThrowingSupplier<T, E extends Exception> {
    T get() throws E;
  }

  /**
   * A builder for Database.  The name of the new database is required.  Everything else
   * selects reasonable defaults.
   * This is a modified version of Hive 3.0 DatabaseBuilder.
   */
  public static class DatabaseBuilder {
    private String name;
    private String description;
    private String location;
    private String ownerName;
    private PrincipalType ownerType;
    private Map<String, String> params = null;

    // Disable default constructor
    private DatabaseBuilder() {
    }

    /**
     * Constructor from database name.
     *
     * @param name Database name
     */
    public DatabaseBuilder(@NotNull String name) {
      this.name = name;
      ownerType = PrincipalType.USER;
    }

    /**
     * Add database description.
     *
     * @param description Database description string.
     * @return this
     */
    public DatabaseBuilder withDescription(@NotNull String description) {
      this.description = description;
      return this;
    }

    /**
     * Add database location
     *
     * @param location Database location string
     * @return this
     */
    public DatabaseBuilder withLocation(@NotNull String location) {
      this.location = location;
      return this;
    }

    /**
     * Add Database parameters
     *
     * @param params database parameters
     * @return this
     */
    public DatabaseBuilder withParams(@NotNull Map<String, String> params) {
      this.params = params;
      return this;
    }

    /**
     * Add a single database parameter.
     *
     * @param key parameter key
     * @param val parameter value
     * @return this
     */
    public DatabaseBuilder withParam(@NotNull String key, @NotNull String val) {
      if (this.params == null) {
        this.params = new HashMap<>();
      }
      this.params.put(key, val);
      return this;
    }

    /**
     * Add database owner name
     *
     * @param ownerName new owner name
     * @return this
     */
    public DatabaseBuilder withOwnerName(@NotNull String ownerName) {
      this.ownerName = ownerName;
      return this;
    }

    /**
     * Add owner tyoe
     *
     * @param ownerType database owner type (USER or GROUP)
     * @return this
     */
    public DatabaseBuilder withOwnerType(PrincipalType ownerType) {
      this.ownerType = ownerType;
      return this;
    }

    /**
     * Build database object
     *
     * @return database
     */
    public Database build() {
      Database db = new Database(name, description, location, params);
      if (ownerName != null) {
        db.setOwnerName(ownerName);
      }
      if (ownerType != null) {
        db.setOwnerType(ownerType);
      }
      return db;
    }
  }

  /**
   * Builder for Table.
   */
  public static class TableBuilder {
    private final String dbName;
    private final String tableName;
    private TableType tableType = TableType.MANAGED_TABLE;
    private String location;
    private String serde = LAZY_SIMPLE_SERDE;
    private String owner;
    private List<FieldSchema> columns;
    private List<FieldSchema> partitionKeys;
    private String inputFormat = HIVE_INPUT_FORMAT;
    private String outputFormat = HIVE_OUTPUT_FORMAT;
    private Map<String, String> parameters = new HashMap<>();

    private TableBuilder() {
      dbName = null;
      tableName = null;
    }

    TableBuilder(String dbName, String tableName) {
      this.dbName = dbName;
      this.tableName = tableName;
    }

    static Table buildDefaultTable(String dbName, String tableName) {
      return new TableBuilder(dbName, tableName).build();
    }

    TableBuilder withType(TableType tabeType) {
      this.tableType = tabeType;
      return this;
    }

    TableBuilder withOwner(String owner) {
      this.owner = owner;
      return this;
    }

    TableBuilder withColumns(List<FieldSchema> columns) {
      this.columns = columns;
      return this;
    }

    TableBuilder withPartitionKeys(List<FieldSchema> partitionKeys) {
      this.partitionKeys = partitionKeys;
      return this;
    }

    TableBuilder withSerde(String serde) {
      this.serde = serde;
      return this;
    }

    TableBuilder withInputFormat(String inputFormat) {
      this.inputFormat = inputFormat;
      return this;
    }

    TableBuilder withOutputFormat(String outputFormat) {
      this.outputFormat = outputFormat;
      return this;
    }

    TableBuilder withParameter(String name, String value) {
      parameters.put(name, value);
      return this;
    }

    TableBuilder withLocation(String location) {
      this.location = location;
      return this;
    }

    Table build() {
      StorageDescriptor sd = new StorageDescriptor();
      if (columns == null) {
        sd.setCols(Collections.emptyList());
      } else {
        sd.setCols(columns);
      }
      SerDeInfo serdeInfo = new SerDeInfo();
      serdeInfo.setSerializationLib(serde);
      serdeInfo.setName(tableName);
      sd.setSerdeInfo(serdeInfo);
      sd.setInputFormat(inputFormat);
      sd.setOutputFormat(outputFormat);
      if (location != null) {
        sd.setLocation(location);
      }

      Table table = new Table();
      table.setDbName(dbName);
      table.setTableName(tableName);
      table.setSd(sd);
      table.setParameters(parameters);
      table.setOwner(owner);
      if (partitionKeys != null) {
        table.setPartitionKeys(partitionKeys);
      }
      table.setTableType(tableType.toString());
      return table;
    }
  }

  /**
   * Builder of partitions.
   */
  public static class PartitionBuilder {
    private final Table table;
    private List<String> values;
    private String location;
    private Map<String, String> parameters = new HashMap<>();

    private PartitionBuilder() {
      table = null;
    }

    PartitionBuilder(Table table) {
      this.table = table;
    }

    PartitionBuilder withValues(List<String> values) {
      this.values = new ArrayList<>(values);
      return this;
    }

    PartitionBuilder withLocation(String location) {
      this.location = location;
      return this;
    }

    PartitionBuilder withParameter(String name, String value) {
      parameters.put(name, value);
      return this;
    }

    PartitionBuilder withParameters(Map<String, String> params) {
      parameters = params;
      return this;
    }

    Partition build() {
      Partition partition = new Partition();
      List<String> partitionNames = table.getPartitionKeys()
              .stream()
              .map(FieldSchema::getName)
              .collect(Collectors.toList());
      if (partitionNames.size() != values.size()) {
        throw new RuntimeException("Partition values do not match table schema");
      }
      List<String> spec = IntStream.range(0, values.size())
              .mapToObj(i -> partitionNames.get(i) + "=" + values.get(i))
              .collect(Collectors.toList());

      partition.setDbName(table.getDbName());
      partition.setTableName(table.getTableName());
      partition.setParameters(parameters);
      partition.setValues(values);
      partition.setSd(table.getSd().deepCopy());
      if (this.location == null) {
        partition.getSd().setLocation(table.getSd().getLocation() + "/" + Joiner.on("/").join(spec));
      } else {
        partition.getSd().setLocation(location);
      }
      return partition;
    }
  }

  /**
   * Builder of lock requests
   */
  public static class LockRequestBuilder {
    private LockRequest req;
    private LockTrie trie;
    private boolean userSet;

    LockRequestBuilder(String agentInfo) {
      req = new LockRequest();
      trie = new LockTrie();
      userSet = false;

      if(agentInfo != null) {
        req.setAgentInfo(agentInfo);
      }
    }

    public LockRequest build() {
      if (!userSet) {
        throw new RuntimeException("Cannot build a lock without giving a user");
      }
      trie.addLocksToRequest(req);
      try {
        req.setHostname(InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException e) {
        throw new RuntimeException("Unable to determine our local host!");
      }
      return req;
    }

    /**
     * Set the transaction id.
     * @param txnid transaction id
     * @return reference to this builder
     */
    public LockRequestBuilder setTransactionId(long txnid) {
      req.setTxnid(txnid);
      return this;
    }

    public LockRequestBuilder setUser(String user) {
      if (user == null) user = "unknown";
      req.setUser(user);
      userSet = true;
      return this;
    }

    /**
     * Add a lock component to the lock request
     * @param component to add
     * @return reference to this builder
     */
    public LockRequestBuilder addLockComponent(LockComponent component) {
      trie.add(component);
      return this;
    }

    /**
     * Add a collection with lock components to the lock request
     * @param components to add
     * @return reference to this builder
     */
    public LockRequestBuilder addLockComponents(Collection<LockComponent> components) {
      trie.addAll(components);
      return this;
    }

    private static class LockTrie {
      Map<String, TableTrie> trie;

      LockTrie() {
        trie = new LinkedHashMap<>();
      }

      public void add(LockComponent comp) {
        TableTrie tabs = trie.get(comp.getDbname());
        if (tabs == null) {
          tabs = new TableTrie();
          trie.put(comp.getDbname(), tabs);
        }
        setTable(comp, tabs);
      }

      public void addAll(Collection<LockComponent> components) {
        for(LockComponent component: components) {
          add(component);
        }
      }

      public void addLocksToRequest(LockRequest request) {
        for (TableTrie tab : trie.values()) {
          for (PartTrie part : tab.values()) {
            for (LockComponent lock :  part.values()) {
              request.addToComponent(lock);
            }
          }
        }
      }

      private void setTable(LockComponent comp, TableTrie tabs) {
        PartTrie parts = tabs.get(comp.getTablename());
        if (parts == null) {
          parts = new PartTrie();
          tabs.put(comp.getTablename(), parts);
        }
        setPart(comp, parts);
      }

      private void setPart(LockComponent comp, PartTrie parts) {
        LockComponent existing = parts.get(comp.getPartitionname());
        if (existing == null) {
          // No existing lock for this partition.
          parts.put(comp.getPartitionname(), comp);
        } else if (existing.getType() != LockType.EXCLUSIVE && (comp.getType() == LockType.EXCLUSIVE
            || comp.getType() == LockType.SHARED_WRITE)) {
          // We only need to promote if comp.type is > existing.type.  For
          // efficiency we check if existing is exclusive (in which case we
          // need never promote) or if comp is exclusive or shared_write (in
          // which case we can promote even though they may both be shared
          // write).  If comp is shared_read there's never a need to promote.
          parts.put(comp.getPartitionname(), comp);
        }
      }
    }

    private static class TableTrie extends LinkedHashMap<String, PartTrie> {}
    private static class PartTrie extends LinkedHashMap<String, LockComponent> {}
  }

  public static class LockComponentBuilder {
    private LockComponent component;
    private boolean tableNameSet;
    private boolean partNameSet;

    public LockComponentBuilder() {
      component = new LockComponent();
      tableNameSet = partNameSet = false;
    }

    /**
     * Set the lock to be exclusive.
     * @return reference to this builder
     */
    public LockComponentBuilder setExclusive() {
      component.setType(LockType.EXCLUSIVE);
      return this;
    }

    /**
     * Set the lock to be semi-shared.
     * @return reference to this builder
     */
    public LockComponentBuilder setSemiShared() {
      component.setType(LockType.SHARED_WRITE);
      return this;
    }

    /**
     * Set the lock to be shared.
     * @return reference to this builder
     */
    public LockComponentBuilder setShared() {
      component.setType(LockType.SHARED_READ);
      return this;
    }

    /**
     * Set the database name.
     * @param dbName database name
     * @return reference to this builder
     */
    public LockComponentBuilder setDbName(String dbName) {
      component.setDbname(dbName);
      return this;
    }

    public LockComponentBuilder setIsTransactional(boolean t) {
      component.setIsTransactional(t);
      return this;
    }

    public LockComponentBuilder setOperationType(DataOperationType dop) {
      component.setOperationType(dop);
      return this;
    }

    /**
     * Set the table name.
     * @param tableName table name
     * @return reference to this builder
     */
    public LockComponentBuilder setTableName(String tableName) {
      component.setTablename(tableName);
      tableNameSet = true;
      return this;
    }

    /**
     * Set the partition name.
     * @param partitionName partition name
     * @return reference to this builder
     */
    public LockComponentBuilder setPartitionName(String partitionName) {
      component.setPartitionname(partitionName);
      partNameSet = true;
      return this;
    }

    public LockComponent build() {
      LockLevel level = LockLevel.DB;
      if (tableNameSet) level = LockLevel.TABLE;
      if (partNameSet) level = LockLevel.PARTITION;
      component.setLevel(level);
      return component;
    }

    public LockComponent setLock(LockType type) {
      component.setType(type);
      return component;
    }
  }

  /**
   * Create table schema from parameters.
   *
   * @param params list of parameters. Each parameter can be either a simple name or
   *               name:type for non-String types.
   * @return table schema description
   */
  public static List<FieldSchema> createSchema(@Nullable List<String> params) {
    if (params == null || params.isEmpty()) {
      return Collections.emptyList();
    }

    return params.stream()
            .map(Util::param2Schema)
            .collect(Collectors.toList());
  }

  /**
   * Get server URI.<p>
   * HMS host is obtained from
   * <ol>
   * <li>Argument</li>
   * <li>HMS_HOST environment parameter</li>
   * <li>hms.host Java property</li>
   * <li>use 'localhost' if above fails</li>
   * </ol>
   * HMS Port is obtained from
   * <ol>
   * <li>Argument</li>
   * <li>host:port string</li>
   * <li>HMS_PORT environment variable</li>
   * <li>hms.port Java property</li>
   * <li>default port value</li>
   * </ol>
   *
   * @param host       HMS host string.
   * @param portString HMS port
   * @return HMS URI
   * @throws URISyntaxException if URI is is invalid
   */
  public static @Nullable URI getServerUri(@Nullable String host, @Nullable String portString) throws
          URISyntaxException {
    if (host == null) {
      host = System.getenv(ENV_SERVER);
    }
    if (host == null) {
      host = System.getProperty(PROP_HOST);
    }
    if (host == null) {
      host = DEFAULT_HOST;
    }
    host = host.trim();

    if ((portString == null || portString.isEmpty() || portString.equals("0")) &&
            !host.contains(":")) {
      portString = System.getenv(ENV_PORT);
      if (portString == null) {
        portString = System.getProperty(PROP_PORT);
      }
    }
    Integer port = Constants.HMS_DEFAULT_PORT;
    if (portString != null) {
      port = Integer.parseInt(portString);
    }

    HostAndPort hp = HostAndPort.fromString(host)
            .withDefaultPort(port);

    LOG.info("Connecting to {}:{}", hp.getHostText(), hp.getPort());

    return new URI(THRIFT_SCHEMA, null, hp.getHostText(), hp.getPort(),
            null, null, null);
  }


  private static FieldSchema param2Schema(@NotNull String param) {
    String colType = DEFAULT_TYPE;
    String name = param;
    if (param.contains(TYPE_SEPARATOR)) {
      String[] parts = param.split(TYPE_SEPARATOR);
      name = parts[0];
      colType = parts[1].toLowerCase();
    }
    return new FieldSchema(name, colType, "");
  }

  /**
   * Create multiple partition objects.
   *
   * @param table
   * @param arguments   - list of partition names.
   * @param npartitions - Partition parameters
   * @return List of created partitions
   */
  static List<Partition> createManyPartitions(@NotNull Table table,
                                              @Nullable Map<String, String> parameters,
                                              @NotNull List<String> arguments,
                                              int npartitions) {
    return IntStream.range(0, npartitions)
            .mapToObj(i ->
                    new PartitionBuilder(table)
                            .withParameters(parameters)
                            .withValues(
                                    arguments.stream()
                                            .map(a -> a + i)
                                            .collect(Collectors.toList())).build())
            .collect(Collectors.toList());
  }

  /**
   * Add many partitions in one HMS call
   *
   * @param client      HMS Client
   * @param dbName      database name
   * @param tableName   table name
   * @param arguments   list of partition names
   * @param npartitions number of partitions to create
   * @throws TException if fails to create partitions
   */
  static Object addManyPartitions(@NotNull HMSClient client,
                                  @NotNull String dbName,
                                  @NotNull String tableName,
                                  @Nullable Map<String, String> parameters,
                                  @NotNull List<String> arguments,
                                  int npartitions) throws TException {
    Table table = client.getTable(dbName, tableName);
    client.addPartitions(createManyPartitions(table, parameters, arguments, npartitions));
    return null;
  }

  static List<String> generatePartitionNames(@NotNull String prefix, int npartitions) {
    return IntStream.range(0, npartitions).mapToObj(i -> prefix + i).collect(Collectors.toList());
  }

  static void addManyPartitionsNoException(@NotNull HMSClient client,
                                           @NotNull String dbName,
                                           @NotNull String tableName,
                                           @Nullable Map<String, String> parameters,
                                           List<String> arguments,
                                           int npartitions) {
    throwingSupplierWrapper(() ->
            addManyPartitions(client, dbName, tableName, parameters, arguments, npartitions));
  }

  /**
   * Filter candidates - find all that match positive matches and do not match
   * any negative matches.
   *
   * @param candidates       list of candidate strings. If null, return an empty list.
   * @param positivePatterns list of regexp that should all match. If null, everything matches.
   * @param negativePatterns list of regexp, none of these should match. If null, everything matches.
   * @return list of filtered results.
   */
  public static List<String> filterMatches(@Nullable List<String> candidates,
                                           @Nullable Pattern[] positivePatterns,
                                           @Nullable Pattern[] negativePatterns) {
    if (candidates == null || candidates.isEmpty()) {
      return Collections.emptyList();
    }
    final Pattern[] positive = (positivePatterns == null || positivePatterns.length == 0) ?
            MATCH_ALL_PATTERN : positivePatterns;
    final Pattern[] negative = negativePatterns == null ? EMPTY_PATTERN : negativePatterns;

    return candidates.stream()
            .filter(c -> Arrays.stream(positive).anyMatch(p -> p.matcher(c).matches()))
            .filter(c -> Arrays.stream(negative).noneMatch(p -> p.matcher(c).matches()))
            .collect(Collectors.toList());
  }
}
