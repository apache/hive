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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.cli;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.QTestSystemProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * S3 containerized object store for tests that need {@code s3a://} access.
 */
public final class S3Container {

  private static final Logger LOG = LoggerFactory.getLogger(S3Container.class);
  private static final DockerImageName RUSTFS_IMAGE =
      DockerImageName.parse("rustfs/rustfs:v1.0.0-rc.5-glibc");
  private static final String ACCESS_KEY = "rustfsadmin";
  private static final String SECRET_KEY = "rustfsadmin";
  private static final int S3_PORT = 9000;
  private static final int DOWNLOAD_CONNECT_TIMEOUT_MS = 30_000;
  private static final int DOWNLOAD_READ_TIMEOUT_MS = 60_000;
  private GenericContainer<?> container;
  private final BucketSpec bucketSpec;

  public S3Container(BucketSpec bucketSpec) {
    this.bucketSpec = bucketSpec;
  }

  @SuppressWarnings("resource")
  public void start() {
    String bucket = bucketSpec.bucket();
    container = new GenericContainer<>(RUSTFS_IMAGE)
        .withExposedPorts(S3_PORT, 9001)
        .withCreateContainerCmdModifier(cmd ->
            cmd.withEntrypoint("/bin/sh", "-c",
                "mkdir -p /data/" + bucket + " && exec /entrypoint.sh rustfs"))
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)))
        .withLogConsumer(outputFrame -> LOG.debug(outputFrame.getUtf8String().trim()));
    container.start();
    LOG.info("S3 container ready at {}", getS3Endpoint());
    downloadData();
    uploadDataToS3();
  }

  private void uploadDataToS3() {
    Configuration conf = new Configuration();
    applyS3Settings(conf);
    String bucket = bucketSpec.bucket();
    String keyPrefix = bucketSpec.keyPrefix();
    try (FileSystem fs = FileSystem.get(URI.create("s3a://" + bucket), conf)) {
      int count = 0;
      try (ZipInputStream zis = new ZipInputStream(new FileInputStream(dataPath().toFile()))) {
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
          if (entry.isDirectory()) {
            continue;
          }
          String key = keyPrefix.isEmpty() ? entry.getName() : keyPrefix + "/" + entry.getName();
          Path s3Path = new Path("s3a://" + bucket + "/" + key);
          try (OutputStream os = fs.create(s3Path, true)) {
            byte[] buf = new byte[8192];
            int len;
            while ((len = zis.read(buf)) != -1) {
              os.write(buf, 0, len);
            }
          }
          count++;
        }
      }
      LOG.info("Uploaded {} files to s3a://{}", count, bucket);
    } catch (IOException e) {
      throw new RuntimeException("Failed to upload data to S3 container", e);
    }
  }

  private static final java.nio.file.Path CACHE =
      Paths.get(QTestSystemProperties.getBuildDir(), "downloads", "S3Container");

  private void downloadData() {
    try {
      java.nio.file.Path file = dataPath();
      if (!Files.exists(file)) {
        Files.createDirectories(file.getParent());
        java.nio.file.Path tmp = Files.createTempFile(file.getParent(), "download-", ".tmp");
        try {
          LOG.info("Downloading data from {} to {}", bucketSpec.dataUrl, file);
          FileUtils.copyURLToFile(bucketSpec.dataUrl, tmp.toFile(),
              DOWNLOAD_CONNECT_TIMEOUT_MS, DOWNLOAD_READ_TIMEOUT_MS);
          Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
          Files.deleteIfExists(tmp);
          throw e;
        }
      }
      LOG.info("Data from {} are available in {}", bucketSpec.dataUrl, file);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private java.nio.file.Path dataPath() {
    return CACHE.resolve(URLEncoder.encode(bucketSpec.dataUrl.toString(), StandardCharsets.UTF_8));
  }

  public void stop() {
    if (container != null) {
      container.stop();
      container = null;
    }
  }

  private String getS3Endpoint() {
    String host = container.getHost();
    int mappedPort = container.getMappedPort(S3_PORT);
    return String.format("http://%s:%d", host, mappedPort);
  }

  public void applyS3Settings(Configuration conf) {
    if (container == null) {
      throw new IllegalStateException("S3 container has not been started");
    }
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.endpoint", getS3Endpoint());
    conf.set("fs.s3a.access.key", ACCESS_KEY);
    conf.set("fs.s3a.secret.key", SECRET_KEY);
    conf.setBoolean("fs.s3a.path.style.access", true);
    conf.setBoolean("fs.s3a.connection.ssl.enabled", false);
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A");
  }

  public static class BucketSpec {
    String bucketName;
    URL dataUrl;

    public BucketSpec(String bucketName, String dataUrl) {
      this.bucketName = bucketName;
      try {
        this.dataUrl = new URI(dataUrl).toURL();
      } catch (MalformedURLException | URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
    }

    String bucket() {
      int slash = bucketName.indexOf('/');
      return slash > 0 ? bucketName.substring(0, slash) : bucketName;
    }

    String keyPrefix() {
      int slash = bucketName.indexOf('/');
      return slash > 0 ? bucketName.substring(slash + 1) : "";
    }
  }
}
