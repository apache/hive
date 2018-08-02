package org.apache.hadoop.hive.registry.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


public class ClassLoaderCache {
  private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderCache.class);
  public static final String CACHE_SIZE_KEY = SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name();
  public static final String CACHE_EXPIRY_INTERVAL_KEY = SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name();

  private final LoadingCache<String, ClassLoader> loadingCache;
  private final SchemaRegistryClient schemaRegistryClient;
  private final File localJarsDir;

  public ClassLoaderCache(final SchemaRegistryClient schemaRegistryClient) {
    this.schemaRegistryClient = schemaRegistryClient;

    CacheLoader<String, ClassLoader> cacheLoader = new CacheLoader<String, ClassLoader>() {
      @Override
      public ClassLoader load(String fileId) throws Exception {
        File file = getFile(fileId);
        return new URLClassLoader(new URL[]{file.toURI().toURL()});
      }
    };

    SchemaRegistryClient.Configuration configuration = schemaRegistryClient.getConfiguration();
    loadingCache = CacheBuilder.newBuilder()
            .maximumSize(((Number) configuration.getValue(CACHE_SIZE_KEY)).longValue())
            .expireAfterAccess(((Number) configuration.getValue(CACHE_EXPIRY_INTERVAL_KEY)).longValue(),
                    TimeUnit.SECONDS)
            .build(cacheLoader);

    localJarsDir = new File((String) this.schemaRegistryClient.getConfiguration().getValue(SchemaRegistryClient.Configuration.LOCAL_JAR_PATH.name()));
    ensureLocalDirsExist();
  }

  private void ensureLocalDirsExist() {
    if (!localJarsDir.exists()) {
      if (!localJarsDir.mkdirs()) {
        LOG.error("Could not create given local jar storage dir: [{}]", localJarsDir.getAbsolutePath());
      }
    }
  }

  private File getFile(String fileId) throws IOException {
    ensureLocalDirsExist();

    File file = new File(localJarsDir, fileId);
    boolean created = file.createNewFile();
    if (created) {
      LOG.debug("File [{}] is created successfully", file);
    } else {
      LOG.debug("File [{}] already exists", file);
    }

    try (FileOutputStream fos = new FileOutputStream(file);
         FileInputStream fileInputStream = new FileInputStream(file)) {
      LOG.debug("Loading file: [{}]", file);

      if (fileInputStream.read() != -1) {
        LOG.debug("File [{}] is already written with content and returning the existing file", file);
        return file;
      } else {
        LOG.debug("File [{}] does not have content, downloading and storing started..", file);
        try (InputStream inputStream = schemaRegistryClient.downloadFile(fileId)) {
          IOUtils.copy(inputStream, fos);
          LOG.debug("Finished storing file [{}]", file);
        } catch (Exception e) {
          // truncate it now, so that next time it would be fetched again.
          fos.getChannel().truncate(0);
          throw e;
        }
      }
    }

    return file;
  }

  public ClassLoader getClassLoader(String fileId) {
    try {
      return loadingCache.get(fileId);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

}
