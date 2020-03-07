package hu.rxd.toolbox.qtest.diff;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedURL {

  Logger LOG = LoggerFactory.getLogger(CachedURL.class);

  private URL remoteUrl;
  private File cachedFile;
  private File tmpFile;

  public CachedURL(URL url, int secondsTtl) throws IOException {
    this(url, new File(System.getProperty(("java.io.tmpdir"))), secondsTtl);
  }

  public CachedURL(URL url) throws IOException {
    this(url, new File(System.getProperty(("java.io.tmpdir"))), -1);
  }

  public CachedURL(URL url, File downloadDir) throws IOException {
    this(url, downloadDir, -1);
  }

  public CachedURL(URL url, File downloadDir, int secondsTtl) throws IOException {
    remoteUrl = url;
    
    String sha1 = DigestUtils.sha1Hex(remoteUrl.toString());
    String baseName = new File(url.getPath()).getName();
    String localFileName = "hu.rxd.tmp-" + sha1 + "." + baseName;
    cachedFile = new File(downloadDir, localFileName);
    tmpFile = new File(downloadDir, localFileName + ".tmp");
    if (secondsTtl > 0 && olderThan(cachedFile, secondsTtl)) {
      LOG.info("removing cached file({}); older than {} seconds", cachedFile, secondsTtl);
      cachedFile.delete();
    }
  }

  private boolean olderThan(File file, int secondsTtl) throws IOException {
    if (!file.exists()) {
      return false;
    }
    FileTime modTime = Files.getLastModifiedTime(file.toPath());
    long tMod = modTime.toMillis();
    long now = System.currentTimeMillis();
    return (now - tMod > secondsTtl * 1000);
  }

  public File getFile() throws IOException {
    if (!cachedFile.exists()) {
      download();
    }
    LOG.info("serving: {} for {}", cachedFile, remoteUrl);
    return cachedFile;
  }

  public URL getURL() throws IOException {
    return getFile().toURI().toURL();
  }

  private void download() throws IOException {
    if (tmpFile.exists()) {
      tmpFile.delete();
    }

    LOG.info("downloading: {}", remoteUrl);
    try (OutputStream output = new FileOutputStream(tmpFile)) {
      try (InputStream input = remoteUrl.openStream()) {
        IOUtils.copy(input, output);
      }
    }
    LOG.info("downloaded: {}", remoteUrl);
    tmpFile.renameTo(cachedFile);
  }

}
