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

import java.nio.file.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import javax.jdo.PersistenceManager;
import javax.jdo.datastore.JDOConnection;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;

public class MetaToolTaskMetadataSummary extends MetaToolTask {
    private PersistenceManager pm = null;

    @Override
    void execute() {
        try{
            String query = getCl().getMetadataSummaryFormat();
            ObjectStore objectStore = getObjectStore();
            pm = objectStore.getPersistenceManager();
            JDOConnection jdoConn = pm.getDataStoreConnection();
            Statement stmt = ((Connection) jdoConn.getNativeConnection()).createStatement();
            ObjectStore.updateMetastoreSummary(stmt);
            MetadataSummary metadataSummary = ObjectStore.getMetadataSummary(stmt);
            if(query.toLowerCase().trim().equals("-json")) {
                exportInJson(metadataSummary);
            } else if(query.toLowerCase().trim().equals("-console")) {
                exportInConsole(metadataSummary);
            } else {
                System.out.println("invalid format!");
            }
        }
        catch(SQLException e) {
            System.out.println(e);
        }
    }

    /**
     * Exporting the MetadataSummary in JSON format.
     * @param metadataSummary
     */
    public void exportInJson(MetadataSummary metadataSummary) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String jsonOutput = gson.toJson(metadataSummary);
        writeJsonInFile(jsonOutput);
    }

    /**
     * Exporting the MetadataSummary in CONSOLE.
     * @param metadataSummary
     */
    public void exportInConsole(MetadataSummary metadataSummary) {
        outputConsole(metadataSummary);
    }

    /**
     * Helper method of exportInConsole.
     * @param metadataSummary
     */
    private void outputConsole(MetadataSummary metadataSummary) {
        List<CatalogSummary> catalogSummaries = metadataSummary.getCatalog_names();
        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        System.out.println("                                                                                                    Metadata Summary                                                                                                        ");
        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        System.out.println("----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----Catalog Summary-----    ----    ----    ----    ---    -----     ----    ----    ----    -----     ");
        System.out.printf("%100s", "Catalog_Name");
        System.out.println();
        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        for(CatalogSummary ctlg: catalogSummaries) {
            System.out.format("%100s", ctlg.getCat_name());
            System.out.println();
        }
        System.out.println("----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----Database Summary----    ----    ----    ----    ----    -----     ----    ----    ----    -----  ");
        System.out.printf("%70s %70s", "db_name", "cat_name");
        System.out.println();
        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        for(CatalogSummary ctlg: catalogSummaries) {
            List<DatabaseSummary> databaseSummaries = ctlg.getDatabase_names();
            for(DatabaseSummary dbs: databaseSummaries) {
                System.out.format("%70s %70s", dbs.getDb_name(), dbs.getCat_name());
                System.out.println();
            }

        }
        System.out.println("----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----    ----Table Summary----    ----    ----    ----    ----    -----     ----    ----    ----    -----    ");
        System.out.printf("%10s %28s %35s %10s %20s %15s %10s %10s %10s %10s %15s", "cat_name", "db_name", "table_name", "column_count",
                "table_type", "file_format", "compression_type", "num_rows", "size_bytes", "num_files", "partition_col_count");
        System.out.println();
        System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        for(CatalogSummary ctlg: catalogSummaries) {
            List<DatabaseSummary> databaseSummaries = ctlg.getDatabase_names();
            for(DatabaseSummary dbs: databaseSummaries) {
                List<TableSummary> tableSummaries = dbs.getTable_names();
                for(TableSummary tbs: tableSummaries) {
                    System.out.format("%10s %28s %35s %10d %20s %15s %10s %10s %10s %10s %15s", tbs.getCat_name(), tbs.getDb_name(),
                            tbs.getTable_name(), tbs.getColumn_count(), tbs.getTable_type(), tbs.getFile_format(),
                            tbs.getCompression_type(), tbs.getSize_numRows(), tbs.getSize_bytes(),
                            tbs.getSize_numFiles(), tbs.getPartition_column_count());
                    System.out.println();
                }
            }
        }
        System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
    }

    /**
     * Helper method of exportInJson.
     * @param jsonOutput A string, JSON formatted string about metadataSummary.
     */
    private void writeJsonInFile(String jsonOutput) {
        try {
            File myObj = new File("/tmp/MetastoreSummary_JSON.txt");
            if (myObj.createNewFile()) {
                System.out.println("File created: " + myObj.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        // Creating an instance of file
        Path path
                = Paths.get("/tmp/MetastoreSummary_JSON.txt");

        byte[] arr = jsonOutput.getBytes();

        // Try block to check for exceptions
        try {
            Files.write(path, arr);
            System.out.println("write in file successfully");
        }
        // Catch block to handle the exceptions
        catch (IOException ex) {
            // Print message as exception occurred when invalid path of local machine is passed
            System.out.print("Invalid Path");
        }
    }
}
