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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metasummary.MetaSummaryHandler;
import org.apache.hadoop.hive.metastore.metasummary.MetaSummarySchema;
import org.apache.hadoop.hive.metastore.metasummary.MetadataTableSummary;
import org.apache.hadoop.hive.metastore.tools.MetaToolObjectStore;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaToolTaskMetadataSummary extends MetaToolTask {
  private static final Logger LOG = LoggerFactory.getLogger(MetaToolTaskMetadataSummary.class);
  private static final Map<String, String> NON_NATIVE_SUMMARY_HANDLER = new HashMap<>();

  static {
    NON_NATIVE_SUMMARY_HANDLER.put("iceberg", "org.apache.iceberg.metasummary.IcebergSummaryHandler");
  }

  private boolean formatJson;
  private boolean formatCsv;
  private boolean formatConsole;
  private long taskTimeout;

  @Override
  void execute() {
    String[] inputParams = validateInput();
    if (inputParams == null) {
      return;
    }
    ExecutorService service = Executors.newSingleThreadExecutor();
    Future<Pair<MetaSummarySchema, List<MetadataTableSummary>>> resFuture =
        service.submit(() -> obtainAndFilterSummary(inputParams, formatJson));
    try {
      service.shutdown();
      Pair<MetaSummarySchema, List<MetadataTableSummary>> result =
          resFuture.get(taskTimeout, TimeUnit.MILLISECONDS);
      if (result == null) {
        System.err.println("Oops, no summary is generated...");
        return;
      }
      // If we are here but the timeout is reached, let's perform the rest of the work
      MetaSummarySchema extraSchema = result.getLeft();
      List<MetadataTableSummary> summaries = result.getRight();
      String fileName = null;
      if (inputParams.length >= 2) {
        fileName = inputParams[1].toLowerCase().trim();
      }
      if (formatJson) {
        exportInJson(summaries, fileName == null ? "./MetastoreSummary.json" : fileName);
      } else if (formatConsole) {
        printToConsole(summaries, extraSchema);
      } else {
        exportInCsv(summaries, extraSchema, fileName == null ? "./MetastoreSummary.csv" : fileName);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } finally {
      service.shutdownNow();
    }
  }

  private String[] validateInput() {
    String[] inputParams = getCl().getMetadataSummaryParams();
    String formatOption = inputParams[0].toLowerCase().trim();
    this.formatJson = formatOption.equalsIgnoreCase("-json");
    this.formatCsv = formatOption.equalsIgnoreCase("-csv");
    this.formatConsole = formatOption.equalsIgnoreCase("-console");
    if (!formatJson && !formatCsv && !formatConsole) {
      System.err.println("Invalid format option: " + formatOption + 
          " to -metadataSummary, only -json, -csv and -console are allowed");
      return null;
    }
    return inputParams;
  }

  Pair<MetaSummarySchema, List<MetadataTableSummary>> obtainAndFilterSummary(String[] inputParams,
      boolean formatJson) throws MetaException {
    Deadline.registerIfNot(taskTimeout);
    boolean isTimerStarted = false;
    try {
      isTimerStarted = Deadline.startTimer("obtainAndFilterSummary");
      MetaToolObjectStore objectStore = getObjectStore();
      List<MetadataTableSummary> allSummaries = objectStore.getMetadataSummary(null, null, null);
      if (allSummaries == null || allSummaries.isEmpty()) {
        System.out.println("Return set of tables is empty or null");
        return null;
      }
      ArrayListMultimap<Class<? extends MetaSummaryHandler>, Pair<TableName, MetadataTableSummary>> nonNativeSummaries =
          findNonNativeSummaries(allSummaries);
      Integer lastUpdatedDays = inputParams.length >= 3 ? Integer.valueOf(inputParams[2]) : null;
      Integer tablesLimit = inputParams.length >= 4 ? Integer.valueOf(inputParams[3]) : null;
      Map<MetadataTableSummary, Void> filteredSummary = new IdentityHashMap<>();
      MetaSummarySchema extraSchema = new MetaSummarySchema();
      for (Class<? extends MetaSummaryHandler> handler : nonNativeSummaries.keys()) {
        Configuration conf = getObjectStore().getConf();
        try (MetaSummaryHandler summaryHandler = JavaUtils.newInstance(handler)) {
          summaryHandler.setConf(conf);
          summaryHandler.initialize(MetaStoreUtils.getDefaultCatalog(conf), formatJson, extraSchema);
          List<Pair<TableName, MetadataTableSummary>> tableSummaries = nonNativeSummaries.get(handler);
          // Filter those we don't want to collect
          Set<TableName> tableNames = getObjectStore().filterTablesForSummary(tableSummaries, lastUpdatedDays, tablesLimit);
          for (Pair<TableName, MetadataTableSummary> ts : tableSummaries) {
            MetadataTableSummary summary = ts.getRight();
            if (tableNames.contains(ts.getLeft())) {
              summaryHandler.appendSummary(ts.getLeft(), summary);
            } else {
              filteredSummary.put(summary, null);
            }
            // If there is an exception while collecting the summary, remove it
            if (summary.isDropped()) {
              filteredSummary.put(summary, null);
            }
          }
        } catch (Exception e) {
          System.err.println(ExceptionUtils.getStackTrace(e));
          LOG.warn("Error collecting the summary from handler: " + handler.getName(), e);
        }
      }
      // Filter the table summary from the output
      if (!filteredSummary.isEmpty()) {
        allSummaries = allSummaries.stream()
            .filter(s -> !filteredSummary.containsKey(s)).collect(Collectors.toList());
      }
      return Pair.of(extraSchema, allSummaries);
    } finally {
      if (isTimerStarted) {
        Deadline.stopTimer();
      }
    }
  }

  private ArrayListMultimap<Class<? extends MetaSummaryHandler>,
      Pair<TableName, MetadataTableSummary>> findNonNativeSummaries(List<MetadataTableSummary> summaries) {
    ArrayListMultimap<Class<? extends MetaSummaryHandler>,
        Pair<TableName, MetadataTableSummary>> summaryHandlers = ArrayListMultimap.create();
    Map<String, Class<? extends MetaSummaryHandler>> visitedClz = new HashMap<>();
    summaries.stream().filter(summary -> summary.getTableType() != null && NON_NATIVE_SUMMARY_HANDLER.containsKey(
        summary.getTableType().toLowerCase())).forEach(summary -> {
      Class<? extends MetaSummaryHandler> handler;
      String tableType = summary.getTableType().toLowerCase();
      String className = NON_NATIVE_SUMMARY_HANDLER.get(tableType);
      TableName tableName = new TableName(MetaStoreUtils.getDefaultCatalog(getObjectStore().getConf()),
          summary.getDbName(), summary.getTblName());
      try {
        handler = visitedClz.get(className);
        if (handler == null) {
          handler = JavaUtils.getClass(className, MetaSummaryHandler.class);
          visitedClz.put(className, handler);
        }
        summaryHandlers.put(handler, Pair.of(tableName, summary));
      } catch (Exception e) {
        LOG.error(
            "Unable to load the class: " + className + ", will ignore the non-native summary for the table: " + tableName,
            e);
      }
    });
    return summaryHandlers;
  }

  /**
   * Exporting the MetadataSummary in JSON format.
   *
   * @param tableSummaryList
   * @param filename         fully qualified path of the output file
   */
  public void exportInJson(List<MetadataTableSummary> tableSummaryList, String filename) throws IOException {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    JsonElement element = gson.toJsonTree(tableSummaryList);
    // flatten the extra summary
    if (element.isJsonArray()) {
      JsonArray elements = element.getAsJsonArray();
      for (JsonElement outer : elements) {
        if (!outer.isJsonObject()) {
          continue;
        }
        JsonObject object = outer.getAsJsonObject();
        JsonElement innerElement = object.get("summary");
        if (innerElement != null && innerElement.isJsonObject()) {
          JsonObject innerObject = innerElement.getAsJsonObject();
          for (String fieldName : innerObject.keySet().toArray(new String[0])) {
            object.add(fieldName, innerObject.remove(fieldName));
          }
          object.remove("summary");
        }
      }
    }
    writeJsonInFile(gson.toJson(element), filename);
  }

  /**
   * Exporting the MetadataSummary in CONSOLE.
   *
   * @param tableSummariesList
   */
  public void printToConsole(List<MetadataTableSummary> tableSummariesList,
      MetaSummarySchema extraSchema) {
    System.out.println(
        "----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----   LEGEND -----    ----    ----    ----    ---    -----     ----    ----    ----    -----");
    System.out.print("\033[0;1m#COLS\033[0m ");
    System.out.print("--> # of columns in the table ");
    System.out.print("\033[0;1m#PARTS\033[0m ");
    System.out.print("--> # of Partitions ");
    System.out.print("\033[0;1m#ROWS\033[0m ");
    System.out.print("--> # of rows in table ");
    System.out.print("\033[0;1m#FILES\033[0m ");
    System.out.print("--> No of files in table ");
    System.out.print("\033[0;1mSIZE\033[0m ");
    System.out.print("--> Size of table in bytes ");
    System.out.print("\033[0;1m#PCOLS\033[0m ");
    System.out.print("--> # of partition columns ");
    System.out.print("\033[0;1m#ARR\033[0m ");
    System.out.print("--> # of array columns ");
    System.out.print("\033[0;1m#STRT\033[0m ");
    System.out.print("--> # of struct columns ");
    System.out.print("\033[0;1m#MAP\033[0m ");
    System.out.print("--> # of map columns ");
    StringBuilder format = new StringBuilder("");
    List<String> extraFields = extraSchema.getFields();
    List<String> upperFields = new ArrayList<>(extraFields.size());
    List<String> columns = new ArrayList<>(Arrays.asList("DATABASE",
        "TABLE NAME", "OWNER", "#COLS", "#PARTS", "TYPE", "FORMAT", "COMPRESSION", "#ROWS", "#FILES", "SIZE(b)", "#PCOLS",
        "#ARR", "#STRT", "#MAP"));
    int colIndex = columns.size() + 1;
    for (String field : extraFields) {
      String upperField = field.toUpperCase();
      System.out.print("\033[0;1m#" + upperField + "\033[0m ");
      System.out.print("--> # extra summary field ");
      upperFields.add(upperField);
      format.append(" %").append(colIndex++).append("$15s ");
    }
    System.out.println("");
    System.out.println(
        "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
    System.out.println(
        "                                                                                                    Metadata Summary                                                                                                        ");
    System.out.println(
        "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
    System.out.print("\033[0;1m");

    columns.addAll(upperFields);
    System.out.printf("%1$20s %2$30s %3$10s %4$5s %5$5s %6$15s %7$15s %8$10s %9$10s %10$10s %11$10s %12$10s %13$5s %14$5s %15$5s" + format, columns.toArray());
    System.out.print("\033[0m");
    System.out.println();
    System.out.println(
        "------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
    String[] values = new String[extraSchema.size()];
    Arrays.fill(values, "-");
    for (MetadataTableSummary summary : tableSummariesList) {
      Map<String, Object> extraSummary = summary.getExtraSummary();
      if (extraSummary != null) {
        for (int i = 0; i < extraSchema.size(); i++) {
          Object val = extraSummary.get(extraFields.get(i));
          if (val != null) {
            values[i] = String.valueOf(val);
          }
        }
      }
      List<Object> vals =  new ArrayList<>(Arrays.asList(summary.getDbName(), summary.getTblName(), summary.getOwner(), summary.getColCount(), summary.getPartitionCount(),
          summary.getTableType(), summary.getFileFormat(), summary.getCompressionType(), summary.getNumRows(),
          summary.getNumFiles(), summary.getTotalSize(), summary.getPartitionColumnCount(),
          summary.getArrayColumnCount(), summary.getStructColumnCount(), summary.getMapColumnCount()));
      vals.addAll(Arrays.asList(values));
      System.out.format("%1$20s %2$30s %3$10s %4$5d %5$5d %6$15s %7$15s %8$10s %9$10s %10$10s %11$10s %12$10s %13$5d %14$5d %15$5d" + format, vals.toArray());
      System.out.println();
      Arrays.fill(values, "-");
    }
    System.out.println(
        "------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
  }

  /**
   * Exporting the MetadataSummary in JSON format.
   *
   * @param metadataTableSummaryList List of Summary Objects to be printed
   * @param filename                 Fully qualified name of the output file
   */
  public void exportInCsv(List<MetadataTableSummary> metadataTableSummaryList, MetaSummarySchema extraSchema,
      String filename) throws IOException {
    File csvOutputFile = new File(filename);
    try (PrintWriter pw = new PrintWriter(csvOutputFile)){
      // print the header
      StringBuilder header = new StringBuilder()
          .append("Database Name, Table Name, Owner, Column Count, Partition Count, Table Type, File Format,")
          .append("Compression Type, Number of Rows, Number of Files, Size in Bytes, Partition Column Count, Array Column Count, Struct Column Count, Map Column Count");
      List<String> fields = extraSchema.getFields();
      if (!fields.isEmpty()) {
        header.append(", ").append(String.join(", ", fields));
      }
      pw.println(header);
      metadataTableSummaryList.stream().map(summary -> summary.toCSV(fields)).forEach(pw::println);
      pw.flush();
    } catch (IOException e) {
      System.out.println("IOException occurred: " + e);
      throw e;
    }
  }

  /**
   * Helper method of exportInJson.
   *
   * @param jsonOutput A string, JSON formatted string about metadataSummary.
   * @param filename   Path of a file in String where the summary needs to be output to.
   */
  private void writeJsonInFile(String jsonOutput, String filename) throws IOException {
    File jsonOutputFile;
    try {
      jsonOutputFile = new File(filename);
      if (jsonOutputFile.exists()) {
        File oldFile = new File(jsonOutputFile.getAbsolutePath() + "_old");
        System.out.println("Output file already exists, renaming to " + oldFile);
        jsonOutputFile.renameTo(oldFile);
      }
      if (jsonOutputFile.createNewFile()) {
        System.out.println("File created: " + jsonOutputFile.getName());
      } else {
        System.out.println("File already exists.");
      }
    } catch (IOException e) {
      System.out.println("IOException occurred: " + e);
      throw e;
    }

    // Try block to check for exceptions
    try (PrintWriter pw = new PrintWriter(jsonOutputFile)) {
      pw.println(jsonOutput);
      pw.flush();
      System.out.println("Summary written to " + jsonOutputFile);
    } catch (IOException ex) {
      // Print message as exception occurred when invalid path of local machine is passed
      System.out.println("Failed to write output file:" + ex.getMessage());
      throw ex;
    }
  }

  @VisibleForTesting
  public static void addSummaryHandler(String tableType, String handlerName) throws ClassNotFoundException {
    if (tableType == null || handlerName == null) {
      throw new IllegalArgumentException("The input parameters shouldn't be null");
    }
    // Make sure the handler is in classpath
    Class.forName(handlerName);
    NON_NATIVE_SUMMARY_HANDLER.put(tableType, handlerName);
  }

  @Override
  void setObjectStore(MetaToolObjectStore objectStore) {
    super.setObjectStore(objectStore);
    this.taskTimeout = MetastoreConf.getTimeVar(objectStore.getConf(), MetastoreConf.ConfVars.METADATA_SUMMARY_TIMEOUT,
        TimeUnit.MILLISECONDS);
  }
}