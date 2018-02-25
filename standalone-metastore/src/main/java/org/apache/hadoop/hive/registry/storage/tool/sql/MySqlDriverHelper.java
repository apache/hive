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

package org.apache.hadoop.hive.registry.storage.tool.sql;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class MySqlDriverHelper {
    public static final String MYSQL_JAR_FILE_PATTERN = "mysql-connector-java.*?.jar";

    public static void downloadMySQLJarIfNeeded(StorageProviderConfiguration storageProperties, String bootstrapDirPath, String mysqlJarUrl) throws Exception {
        /* Due to license issues we will not be able to ship mysql driver.
               If the dbtype is mysql we will prompt user to download the jar and place
               it under bootstrap/lib and libs folder. This runs only one-time and for
               next time onwards we will check if the mysql jar exists in the path.
             */
        File bootstrapLibDir = new File (bootstrapDirPath + File.separator + "lib/");
        File libDir = new File (bootstrapDirPath + File.separator +  "../libs/");

        if(!libDir.exists()) {
            System.out.println(String.format("Directory : \"%s\" not found, trying to create it ...",libDir.getAbsolutePath()));
            if (!libDir.mkdir())
                throw new RuntimeException(String.format("Failed to create the directory : \"%s\"", libDir.getAbsolutePath()));
        }

        if (storageProperties.getDbType().equals(DatabaseType.MYSQL)
                && (!isMySQLJarFileAvailableOnAnyOfDirectories(Arrays.asList(bootstrapLibDir, libDir)))) {
            downloadMySQLJar(mysqlJarUrl, bootstrapLibDir);
        }
    }

    private static boolean isMySQLJarFileAvailableOnAnyOfDirectories(List<File> directories) {
        return directories.stream().anyMatch(dir -> MySqlDriverHelper.fileExists(dir, MYSQL_JAR_FILE_PATTERN));
    }

    private static void downloadMySQLJar(String mysqlJarUrl, File bootstrapLibDir) throws Exception {
        if (mysqlJarUrl == null || mysqlJarUrl.equals(""))
            throw new IllegalArgumentException("Missing mysql client jar url. " +
                    "Please pass mysql client jar url using -m option.");
        String mysqlJarFileName = MySqlDriverHelper.downloadMysqlJarAndCopyToLibDir(bootstrapLibDir, mysqlJarUrl, MYSQL_JAR_FILE_PATTERN);
        if (mysqlJarFileName != null) {
            File mysqlJarFile = new File(bootstrapLibDir+ File.separator + mysqlJarFileName);
            System.out.println("mysqlJarFile " + mysqlJarFile);
            Utils.loadJarIntoClasspath(mysqlJarFile);
        }
    }

    /*
      This method is solely for the use of downloading the mysql java driver jar.
      It will download the zip file from the provided url.
      Unzips the file and copies the jar into bootstrap/lib and libs.

      @params url mysql zip file URL
      @returns the mysql jar file name.
     */
    public static String downloadMysqlJarAndCopyToLibDir(File bootstrapLibDir, String url, String fileNamePattern) throws IOException {
        System.out.println("Downloading mysql jar from url: " + url);
        String tmpFileName;
        try {
            URL downloadUrl = new URL(url);
            String[] pathSegments = downloadUrl.getPath().split("/");
            String fileName = pathSegments[pathSegments.length - 1];
            String tmpDir = System.getProperty("java.io.tmpdir");
            tmpFileName = tmpDir + File.separator + fileName;
            System.out.println("Downloading file " + fileName + " into " + tmpDir);
            ReadableByteChannel rbc = Channels.newChannel(downloadUrl.openStream());
            FileOutputStream fos = new FileOutputStream(tmpFileName);
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        } catch(IOException ie) {
            System.err.println("Failed to download the mysql driver from " + url);
            throw new IOException(ie);
        }

        System.out.println("Copying mysql libraries into lib dir...");
        String libDir = bootstrapLibDir.getAbsolutePath() + File.separator + "../../libs/";
        System.out.println("Unzipping downloaded mysql driver and copying");
        try {
            String mysqlJarFileName = MySqlDriverHelper.copyFileFromZipToDir(tmpFileName, fileNamePattern, bootstrapLibDir);
            File bootstrapLibFile = new File(bootstrapLibDir + File.separator + mysqlJarFileName);
            File libFile = new File(libDir + File.separator + mysqlJarFileName);
            System.out.println("Copying file to libs " + libFile);
            MySqlDriverHelper.copyFile(bootstrapLibFile, libFile);
            return mysqlJarFileName;
        } catch (IOException ie) {
            ie.printStackTrace();
            System.err.println("Failed to copy mysql driver into " + bootstrapLibDir + " and " + libDir);
        } catch (Exception e ) {
            e.printStackTrace();
            System.err.println("Failed to copy mysql driver into " + bootstrapLibDir + " and " + libDir);
        }
        return null;
    }

    public static String copyFileFromZipToDir(String zipFile, String fileNamePattern, File dir) throws IOException{
        ZipFile zip = new ZipFile(zipFile);
        Enumeration zipFileEntries = zip.entries();
        int BUFFER = 2048;
        while (zipFileEntries.hasMoreElements()) {
            ZipEntry entry = (ZipEntry) zipFileEntries.nextElement();
            String currentEntry = entry.getName();
            if (currentEntry.matches(fileNamePattern)) {
                String[] currentEntrySegments = currentEntry.split(File.separator);
                String matchedFileName = currentEntrySegments[currentEntrySegments.length - 1];
                File file = new File(dir, matchedFileName);
                FileOutputStream fos = new FileOutputStream(file);
                BufferedOutputStream dest = new BufferedOutputStream(fos, BUFFER);
                byte data[] = new byte[BUFFER];
                BufferedInputStream is = new BufferedInputStream(zip.getInputStream(entry));
                int currentByte;
                while ((currentByte = is.read(data, 0, BUFFER)) != -1) {
                    dest.write(data, 0, currentByte);
                }
                dest.flush();
                dest.close();
                is.close();
                return matchedFileName;
            }
        }
        return null;
    }

    public static void copyFile(File sourceFile, File destFile) throws IOException {
        if (!sourceFile.exists()) {
            throw new IllegalArgumentException("Source File doesn't exists");
        }

        Files.copy(sourceFile.toPath(), destFile.toPath());
    }

    public static boolean fileExists(File dir, String regex) {
        File[] files = dir.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return pathname.getName().matches(regex);
            }
        });
        return files != null && files.length == 1;
    }

}
