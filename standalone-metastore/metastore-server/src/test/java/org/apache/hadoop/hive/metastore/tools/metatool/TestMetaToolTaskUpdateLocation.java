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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.tools.MetaToolObjectStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/** Unit tests for MetaToolTaskUpdateLocation. */
@Category(MetastoreUnitTest.class)
public class TestMetaToolTaskUpdateLocation {
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private OutputStream os;

  @Before
  public void setup() {
    os = new ByteArrayOutputStream();
    System.setOut(new PrintStream(os));
    System.setErr(new PrintStream(os));
  }

  @Test
  public void testNoHost() throws Exception {
    exception.expect(IllegalStateException.class);
    exception.expectMessage("HiveMetaTool:A valid host is required in both old-loc and new-loc");

    MetaToolTaskUpdateLocation t = new MetaToolTaskUpdateLocation();
    t.setCommandLine(new HiveMetaToolCommandLine(new String[] {"-updateLocation", "hdfs://", "hdfs://"}));
    t.execute();
  }

  @Test
  public void testNoScheme() throws Exception {
    exception.expect(IllegalStateException.class);
    exception.expectMessage("HiveMetaTool:A valid scheme is required in both old-loc and new-loc");

    MetaToolTaskUpdateLocation t = new MetaToolTaskUpdateLocation();
    t.setCommandLine(new HiveMetaToolCommandLine(new String[] {"-updateLocation", "//old.host", "//new.host"}));
    t.execute();
  }

  @Test
  public void testUpdateLocationNoUpdate() throws Exception {
    // testing only that the proper functions are called on ObjectStore - effect tested in TestHiveMetaTool in itests
    String oldUriString = "hdfs://old.host";
    String newUriString = "hdfs://new.host";
    String tablePropKey = "abc";
    String serdePropKey = "def";

    URI oldUri = new Path(oldUriString).toUri();
    URI newUri = new Path(newUriString).toUri();

    MetaToolObjectStore mockObjectStore = Mockito.mock(MetaToolObjectStore.class);
    when(mockObjectStore.updateMDatabaseURI(eq(oldUri), eq(newUri), eq(true))).thenReturn(null);
    when(mockObjectStore.updateMStorageDescriptorTblURI(eq(oldUri), eq(newUri), eq(true))).thenReturn(null);
    when(mockObjectStore.updateTblPropURI(eq(oldUri), eq(newUri), eq(tablePropKey), eq(true))).thenReturn(null);
    when(mockObjectStore.updateMStorageDescriptorTblPropURI(eq(oldUri), eq(newUri), eq(tablePropKey), eq(true)))
      .thenReturn(null);
    when(mockObjectStore.updateSerdeURI(eq(oldUri), eq(newUri), eq(serdePropKey), eq(true))).thenReturn(null);

    MetaToolTaskUpdateLocation t = new MetaToolTaskUpdateLocation();
    t.setCommandLine(new HiveMetaToolCommandLine(new String[] {"-updateLocation", newUriString, oldUriString, "-dryRun",
        "-tablePropKey", tablePropKey, "-serdePropKey", serdePropKey}));
    t.setObjectStore(mockObjectStore);
    t.execute();
  }

  @Test
  public void testUpdateLocation() throws Exception {
    MetaToolObjectStore objectStore = new MetaToolObjectStore();
    Configuration conf = MetastoreConf.newMetastoreConf();
    objectStore.setConf(conf);
    URI oldUri = new Path("hdfs://old.host").toUri();
    URI newUri = new Path("hdfs://new.host").toUri();
    String tablePropKey1 = "tbl_prop_key1";
    String tablePropKey2 = "tbl_prop_key2";
    String serdePropKey1 = "serde_prop_key1";
    String serdePropKey2 = "serde_prop_key2";
    try (HiveMetaStoreClient client = new HiveMetaStoreClient(conf)) {
      new DatabaseBuilder().setName("test_update_location").create(client, conf);
    }
    Table[] tables = new Table[] {
        createTable(conf, new TableName("hive", "test_update_location", "tab1"),
            List.of(
                Pair.of(tablePropKey1, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata"),
                Pair.of(tablePropKey2, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata")),
            List.of(
                Pair.of(serdePropKey1, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata"),
                Pair.of(serdePropKey2, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata")),
            List.of(
                Pair.of(tablePropKey1, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata"),
                Pair.of(tablePropKey2, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata"))),
        createTable(conf, new TableName("hive", "test_update_location", "tab2"),
            List.of(
                Pair.of(tablePropKey1, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata"),
                Pair.of(tablePropKey2, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata")),
            List.of(
                Pair.of(serdePropKey1, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata")),
            List.of(
                Pair.of(tablePropKey1, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata"))),
        createTable(conf, new TableName("hive", "test_update_location", "tab3"),
            List.of(
                Pair.of(tablePropKey1, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata")),
            List.of(
                Pair.of(serdePropKey1, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata")),
            List.of(
                Pair.of(tablePropKey2, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata"))),
        createTable(conf, new TableName("hive", "test_update_location", "tab4"),
            List.of(
                Pair.of(tablePropKey1, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata")),
            null,
            List.of(
                Pair.of(tablePropKey2, "hdfs://old.host/tab1/" + UUID.randomUUID().toString() + "/metadata")))};

    AtomicReference<Pair<String, AtomicInteger>> methodRef = new AtomicReference<>();
    Function<Pair<Boolean, String[]>, MetaToolObjectStore.UpdateURIRetVal> updatePropURI =
        createFunctionForTesting(methodRef, oldUri, newUri, objectStore);

    methodRef.set(Pair.of("updateTblPropURI", new AtomicInteger(0)));
    validate(newUri, new String[] {tablePropKey1, tablePropKey2}, () -> getTables(conf, tables),
        Table::getParameters, updatePropURI);
    validate(oldUri, new String[] {tablePropKey1}, () -> getTables(conf, tables),
        Table::getParameters, updatePropURI);

    methodRef.set(Pair.of("updateSerdeURI", new AtomicInteger(0)));
    validate(newUri, new String[] {serdePropKey1, serdePropKey2}, () -> getTables(conf, tables),
        t -> t.getSd().getSerdeInfo().getParameters(), updatePropURI);
    validate(oldUri, new String[] {serdePropKey2}, () -> getTables(conf, tables),
        t -> t.getSd().getSerdeInfo().getParameters(), updatePropURI);

    methodRef.set(Pair.of("updateMStorageDescriptorTblPropURI", new AtomicInteger(0)));
    validate(newUri, new String[] {tablePropKey1, tablePropKey2}, () -> getTables(conf, tables),
        t -> t.getSd().getParameters(), updatePropURI);
    validate(oldUri, new String[] {tablePropKey1}, () -> getTables(conf, tables),
        t -> t.getSd().getParameters(), updatePropURI);
  }


  private Function<Pair<Boolean, String[]>, MetaToolObjectStore.UpdateURIRetVal> createFunctionForTesting(
      AtomicReference<Pair<String, AtomicInteger>> methodRef, URI oldUri, URI newUri, MetaToolObjectStore objectStore) {
    return  f -> {
      try {
        MetaToolObjectStore.UpdateURIRetVal retVal = null;
        Pair<String, AtomicInteger> method = methodRef.get();
        String methodName = method.getLeft();
        int counter = f.getLeft() ? method.getRight().get() : method.getRight().getAndIncrement();
        URI newU = newUri;
        URI oldU = oldUri;
        if (counter % 2 != 0) {
          newU = oldUri;
          oldU = newUri;
        }
        if ("updateTblPropURI".equals(methodName)) {
          retVal = objectStore.updateTblPropURI(oldU, newU, String.join(",", f.getRight()), f.getLeft());
        } else if ("updateSerdeURI".equals(methodName)) {
          retVal = objectStore.updateSerdeURI(oldU, newU, String.join(",", f.getRight()), f.getLeft());
        } else if ("updateMStorageDescriptorTblPropURI".equals(methodName)) {
          retVal = objectStore.updateMStorageDescriptorTblPropURI(oldU, newU,
              String.join(",", f.getRight()), f.getLeft());
        }
        return retVal;
      } catch (MetaException e) {
        throw new RuntimeException(e);
      }
    };
  }

  private void validate(URI newUri, String[] keys, Callable<Table[]> getTables,
      Function<Table, Map<String, String>> params,
      Function<Pair<Boolean, String[]>, MetaToolObjectStore.UpdateURIRetVal> call)
      throws Exception {
    // This is a dry run
    MetaToolObjectStore.UpdateURIRetVal retVal = call.apply(Pair.of(true, keys));
    Assert.assertTrue(retVal.getBadRecords().isEmpty());
    Map<String, String> updateLocations = retVal.getUpdateLocations();
    Set<String> oldLocations = Arrays.stream(getTables.call())
        .map(t -> getParams(params.apply(t), keys)).flatMap(Collection::stream).collect(Collectors.toSet());
    Assert.assertEquals(oldLocations, updateLocations.keySet());

    Table[] refreshTables = getTables.call();
    Set<String> locations = Arrays.stream(refreshTables).map(t -> getParams(params.apply(t), keys))
        .flatMap(Collection::stream).collect(Collectors.toSet());
    Assert.assertEquals(oldLocations, locations);

    // Update the location
    call.apply(Pair.of(false, keys));
    refreshTables = getTables.call();
    locations = Arrays.stream(refreshTables).map(t -> getParams(params.apply(t), keys))
        .flatMap(Collection::stream).collect(Collectors.toSet());
    Assert.assertEquals(new HashSet<>(updateLocations.values()), locations);
    Assert.assertTrue(locations.stream().allMatch(l -> l.startsWith(newUri.toString())));
  }

  public Table[] getTables(Configuration configuration, Table... oldTables) throws Exception {
    Table[] results = new Table[oldTables.length];
    try (HiveMetaStoreClient msc = new HiveMetaStoreClient(configuration)) {
      for (int i = 0; i < oldTables.length; i++) {
        Table oldTable = oldTables[i];
        results[i] = msc.getTable(oldTable.getCatName(), oldTable.getDbName(), oldTable.getTableName());
      }
    }
    return results;
  }

  private Set<String> getParams(Map<String, String> params, String... keys) {
    Set<String> results = new HashSet<>();
    for (String key : keys) {
      if (params.containsKey(key)) {
        results.add(params.get(key));
      }
    }
    return results;
  }

  private Table createTable(Configuration configuration, TableName tableName, List<Pair<String, String>> tableParams,
      List<Pair<String, String>> serdeParams, List<Pair<String, String>> sdParams) throws Exception {
    try (HiveMetaStoreClient msc = new HiveMetaStoreClient(configuration)) {
      TableBuilder builder = new TableBuilder().setDbName(tableName.getDb()).setTableName(tableName.getTable())
          .addCol("a", "string").addPartCol("dt", "string")
          .addTableParam("EXTERNAL", "true")
          .setType("EXTERNAL_TABLE")
          .setSerdeLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
          .setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
          .setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
      if (tableParams != null) {
        tableParams.forEach(p -> builder.addTableParam(p.getLeft(), p.getRight()));
      }
      if (serdeParams != null) {
        serdeParams.forEach(p -> builder.addSerdeParam(p.getLeft(), p.getRight()));
      }
      if (sdParams != null) {
        sdParams.forEach(p -> builder.addStorageDescriptorParam(p.getLeft(), p.getRight()));
      }
      return builder.create(msc, configuration);
    }
  }
}
