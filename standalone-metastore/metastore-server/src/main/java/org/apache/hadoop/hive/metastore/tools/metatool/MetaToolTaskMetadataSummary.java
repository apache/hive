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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.hive.metastore.ObjectStore;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;

public class MetaToolTaskMetadataSummary extends MetaToolTask {
    @Override
    void execute() {
      try {
        String[] inputParams = getCl().getMetadataSummaryParams();
        ObjectStore objectStore = getObjectStore();
        List<MetadataTableSummary> tableSummariesList = objectStore.getMetadataSummary(null, null, null);
        if (tableSummariesList == null || tableSummariesList.size() == 0) {
          System.out.println("Return set of tables is empty or null");
          return;
        }
        String filename = null;
        if (inputParams.length == 2) {
          filename = inputParams[1].toLowerCase().trim();
        }
        String formatOption = inputParams[0].toLowerCase().trim();
        switch (formatOption) {
          case "-json":
            exportInJson(tableSummariesList, filename);
            break;
          case "-console":
            printToConsole(tableSummariesList);
            break;
          case "-csv":
            exportInCsv(tableSummariesList, filename);
            break;
          default:
            System.out.println("Invalid option to -metadataSummary");
            return;
        }
      } catch (SQLException e) {
        System.out.println("Generating HMS Summary failed: \n" + e.getMessage());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Exporting the MetadataSummary in JSON format.
     * @param tableSummaryList
     * @param filename fully qualified path of the output file
     */
    public void exportInJson(List<MetadataTableSummary> tableSummaryList, String filename) throws IOException {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String jsonOutput = gson.toJson(tableSummaryList);
        writeJsonInFile(jsonOutput, filename);
    }

    /**
     * Exporting the MetadataSummary in CONSOLE.
     * @param tableSummariesList
     */
    public void printToConsole(List<MetadataTableSummary> tableSummariesList) {
      System.out.println("----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----   LEGEND -----    ----    ----    ----    ---    -----     ----    ----    ----    -----");
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
      System.out.println("");
      System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
      System.out.println("                                                                                                    Metadata Summary                                                                                                        ");
      System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
      System.out.print("\033[0;1m");
      System.out.printf("%10s %20s %30s %5s %5s %15s %15s %10s %10s %10s %10s %10s %5s %5s %5s", "CATALOG", "DATABASE", "TABLE NAME", "#COLS",
                "#PARTS", "TYPE", "FORMAT", "COMPRESSION", "#ROWS", "#FILES", "SIZE(b)",  "#PCOLS", "#ARR", "#STRT", "#MAP");
      System.out.print("\033[0m");
      System.out.println();
      System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
      for(MetadataTableSummary summary: tableSummariesList) {
        System.out.format("%10s %20s %30s %5d %5d %15s %15s %10s %10s %10s %10s %10s %5d %5d %5d", summary.getCtlgName(), summary.getDbName(),
            summary.getTblName(), summary.getColCount(), summary.getPartitionCount(), summary.getTableType(), summary.getFileFormat(),
            summary.getCompressionType(), summary.getSizeNumRows(), summary.getSizeNumFiles(), summary.getTotalSize(), summary.getPartitionColumnCount(), summary.getArrayColumnCount(), summary.getStructColumnCount(), summary.getMapColumnCount());
        System.out.println();
      }
      System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
    }

    /**
     * Exporting the MetadataSummary in JSON format.
     * @param metadataTableSummaryList List of Summary Objects to be printed
     * @param filename Fully qualified name of the output file
     */
    public void exportInCsv(List<MetadataTableSummary> metadataTableSummaryList, String filename) throws IOException {
      if (filename == null || filename.trim().isEmpty()) {
        filename = "./MetastoreSummary.csv";
      }

      PrintWriter pw = null;
      File csvOutputFile = null;
      try {
        csvOutputFile = new File(filename);
        pw = new PrintWriter(csvOutputFile);
        // print the header
        pw.println("Catalog Name, Database Name, Table Name, Column Count, Partition Count, Table Type, File Format, " +
              "Compression Type, Number of Rows, Number of Files, Size in Bytes, Partition Column Count, Array Column Count, Struct Column Count, Map Column Count");
        metadataTableSummaryList.stream()
            .map(MetadataTableSummary::toCSV)
            .forEach(pw::println);
      } catch(IOException e) {
        System.out.println("IOException occurred: " + e);
        throw e;
      } finally {
        pw.flush();
        pw.close();
      }
    }

    /**
     * Helper method of exportInJson.
     * @param jsonOutput A string, JSON formatted string about metadataSummary.
     * @param filename Path of a file in String where the summary needs to be output to.
     */
    private void writeJsonInFile(String jsonOutput, String filename) throws IOException {
      File jsonOutputFile;
      if (filename == null || filename.trim().isEmpty()) {
        filename = "./MetastoreSummary.json";
      }

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
      try {
        PrintWriter pw = new PrintWriter(jsonOutputFile);
        pw.println(jsonOutput);
        pw.flush();
        System.out.println("Summary written to " + jsonOutputFile);
      } catch (IOException ex) {
        // Print message as exception occurred when invalid path of local machine is passed
        System.out.println("Failed to write output file:" + ex.getMessage());
        throw ex;
      }
    }
}