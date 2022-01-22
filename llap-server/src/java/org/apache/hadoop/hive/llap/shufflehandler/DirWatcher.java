/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.llap.shufflehandler;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.llap.shufflehandler.ShuffleHandler.AttemptPathIdentifier;

class DirWatcher {

  private static final Logger LOG = LoggerFactory.getLogger(DirWatcher.class);

  private static enum Type {
    BASE, // App Base Dir / ${dagDir}
    OUTPUT, // appBase/output/
    FINAL, // appBase/output/attemptDir
  }


  private static final String OUTPUT = "output";

  private final AttemptRegistrationListener listener;

  private final WatchService watchService;
  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private final WatcherCallable watcherCallable = new WatcherCallable();
  private final ListeningExecutorService watcherExecutorService;
  private volatile ListenableFuture<Void> watcherFuture;

  private final DelayQueue<WatchedPathInfo> watchedPathQueue = new DelayQueue<>();
  private final WatchExpirerCallable expirerCallable = new WatchExpirerCallable();
  private final ListeningExecutorService expirerExecutorService;
  private volatile ListenableFuture<Void> expirerFuture;

  private final ConcurrentMap<AttemptPathIdentifier, FoundPathInfo> foundAttempts = new ConcurrentHashMap<>();
  private final ConcurrentMap<Path, WatchedPathInfo> watchedPaths = new ConcurrentHashMap<>();
  private final ConcurrentMap<AttemptPathIdentifier, List<WatchKey>> watchesPerAttempt = new ConcurrentHashMap<>();

  DirWatcher(AttemptRegistrationListener listener) throws IOException {
    this.watchService = FileSystems.getDefault().newWatchService();
    this.listener = listener;
    ExecutorService executor1 = Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("DirWatcher").build());
    watcherExecutorService = MoreExecutors.listeningDecorator(executor1);

    ExecutorService executor2 = Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("WatchExpirer").build());
    expirerExecutorService = MoreExecutors.listeningDecorator(executor2);
  }

  /**
   * Register a base dir for an application
   * @param pathString the full path including jobId, user - /${local.dir}/appCache/${appId}/userCache/${user}
   * @param appId the appId
   * @param user the user
   * @param expiry when to expire the watch - in ms
   * @throws IOException
   */
  void registerDagDir(String pathString, String appId, int dagIdentifier, String user, long expiry) throws IOException {
    // The path string contains the dag Identifier
    Path path = FileSystems.getDefault().getPath(pathString);
    WatchedPathInfo watchedPathInfo =
        new WatchedPathInfo(System.currentTimeMillis() + expiry, Type.BASE, appId, dagIdentifier,
            user);
    watchedPaths.put(path, watchedPathInfo);
    WatchKey watchKey = path.register(watchService, ENTRY_CREATE);
    watchedPathInfo.setWatchKey(watchKey);
    watchedPathQueue.add(watchedPathInfo);

    // TODO Watches on the output dirs need to be cancelled at some point. For now - via the expiry.
  }

  void unregisterDagDir(String pathString, String appId, int dagIdentifier) {
    // TODO Implement to remove all watches for the specified pathString and it's sub-tree
  }

  /**
   * Invoke when a pathIdentifier has been found, or is no longer of interest
   * @param pathIdentifier
   */
  void attemptInfoFound(AttemptPathIdentifier pathIdentifier) {
    cancelWatchesForAttempt(pathIdentifier);
  }

  void start() {
    watcherFuture = watcherExecutorService.submit(watcherCallable);
    expirerFuture = expirerExecutorService.submit(expirerCallable);
  }

  void stop() throws IOException {
    shutdown.set(true);
    if (watcherFuture != null) {
      watcherFuture.cancel(true);
    }
    if (expirerFuture != null) {
      expirerFuture.cancel(true);
    }
    watchService.close();
    watcherExecutorService.shutdownNow();
    expirerExecutorService.shutdownNow();
  }



  private void registerDir(Path path, WatchedPathInfo watchedPathInfo) {
    watchedPaths.put(path, watchedPathInfo);
    try {
      WatchKey watchKey = path.register(watchService, ENTRY_CREATE);
      watchedPathInfo.setWatchKey(watchKey);
      watchedPathQueue.add(watchedPathInfo);
      if (watchedPathInfo.type == Type.FINAL) {
        trackWatchForAttempt(watchedPathInfo, watchKey);
      }
    } catch (IOException e) {
      LOG.warn("Unable to setup watch for: " + path);
    }
  }

  private void trackWatchForAttempt(WatchedPathInfo watchedPathInfo, WatchKey watchKey) {
    assert watchedPathInfo.pathIdentifier != null;
    // TODO May be possible to do finer-grained locks.
    synchronized (watchesPerAttempt) {
      List<WatchKey> list = watchesPerAttempt.get(watchedPathInfo.pathIdentifier);
      if (list == null) {
        list = new LinkedList<>();
        watchesPerAttempt.put(watchedPathInfo.pathIdentifier, list);
      }
      list.add(watchKey);
    }
  }

  private void cancelWatchesForAttempt(AttemptPathIdentifier pathIdentifier) {
    // TODO May be possible to do finer-grained locks.
    synchronized(watchesPerAttempt) {
      List<WatchKey> list = watchesPerAttempt.remove(pathIdentifier);
      if (list != null) {
        for (WatchKey watchKey : list) {
          watchKey.cancel();
        }
      }
    }
  }

  public void watch() {
    while (!shutdown.get()) {
      WatchKey watchKey;
      try {
        watchKey = watchService.take();
      } catch (InterruptedException e) {
        if (shutdown.get()) {
          LOG.info("Shutting down watcher");
          break;
        } else {
          LOG.error("Watcher interrupted before being shutdown");
          throw new RuntimeException("Watcher interrupted before being shutdown", e);
        }
      }
      Path watchedPath = (Path) watchKey.watchable();
      WatchedPathInfo parentWatchedPathInfo = watchedPaths.get(watchedPath);
      boolean cancelledWatch = false;
      for (WatchEvent<?> rawEvent : watchKey.pollEvents()) {
        if (rawEvent.kind().equals(OVERFLOW)) {
          // Ignoring and continuing to watch for additional elements in the dir.
          continue;
        }

        WatchEvent<Path> event = (WatchEvent<Path>) rawEvent;
        WatchedPathInfo watchedPathInfo;
        Path resolvedPath;

        switch (parentWatchedPathInfo.type) {
          case BASE:
            // Add the output dir to the watch set, scan it, and cancel current watch.
            if (event.context().getFileName().toString().equals(OUTPUT)) {
              resolvedPath = watchedPath.resolve(event.context());
              watchedPathInfo = new WatchedPathInfo(parentWatchedPathInfo, Type.OUTPUT, null);
              registerDir(resolvedPath, watchedPathInfo);
              // Scan the "output" directory for existing files, and add watches
              try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(resolvedPath)) {
                for (Path path : dirStream) {
                  // This would be an attempt directory. Add a watch, and track it.
                  if (path.toFile().isDirectory()) {
                    watchedPathInfo = new WatchedPathInfo(parentWatchedPathInfo, Type.FINAL, path.getFileName().toString());
                    registerDir(path, watchedPathInfo);
                    scanForFinalFiles(watchedPathInfo, path);
                  } else {
                    LOG.warn("Ignoring unexpected file: " + path);
                  }
                }
              } catch (IOException e) {
                LOG.warn("Unable to list files under: " + resolvedPath);
              }
              // Cancel the watchKey since the output dir has been found.
              cancelledWatch = true;
              watchKey.cancel();
            } else {
              LOG.warn("DEBUG: Found unexpected directory while looking for OUTPUT: " + event.context() + " under " + watchedPath);
            }
            break;
          case OUTPUT:
            // Add the attemptDir to the watch set, scan it and add to the list of found files
            resolvedPath = watchedPath.resolve(event.context());
            // New attempt path crated. Add a watch on it, and scan it for existing files.
            watchedPathInfo = new WatchedPathInfo(parentWatchedPathInfo, Type.FINAL, event.context().getFileName().toString());
            registerDir(resolvedPath, watchedPathInfo);
            scanForFinalFiles(watchedPathInfo, resolvedPath);

            break;
          case FINAL:
            resolvedPath = watchedPath.resolve(event.context());
            if (event.context().getFileName().toString().equals(ShuffleHandler.DATA_FILE_NAME)) {
              registerFoundAttempt(parentWatchedPathInfo.pathIdentifier, null, resolvedPath);
            } else if (event.context().getFileName().toString().equals(ShuffleHandler.INDEX_FILE_NAME)) {
              registerFoundAttempt(parentWatchedPathInfo.pathIdentifier, resolvedPath, null);
            } else {
              LOG.warn("Ignoring unexpected file: " + watchedPath.resolve(event.context()));
            }
            break;
        }

      }
      if (!cancelledWatch) {
        boolean valid = watchKey.reset();
        if (!valid) {
          LOG.warn("DEBUG: WatchKey: " + watchKey.watchable() + " no longer valid");
        }
      }
    }
  }

  private void scanForFinalFiles(WatchedPathInfo watchedPathInfo, Path path) {
    try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(path) ) {
      for (Path p : dirStream) {
        if (p.getFileName().toString().equals(ShuffleHandler.DATA_FILE_NAME)) {
          registerFoundAttempt(watchedPathInfo.pathIdentifier, null, path);
        } else if (p.getFileName().toString().equals(ShuffleHandler.INDEX_FILE_NAME)) {
          registerFoundAttempt(watchedPathInfo.pathIdentifier, path, null);
        } else {
          LOG.warn("Ignoring unknown file: " + p.getFileName());
        }
      }
    } catch (IOException e) {
      LOG.warn("Unable to open dir stream for attemptDir: " + path);
    }
  }

  private void registerFoundAttempt(AttemptPathIdentifier pathIdentifier, Path indexFile, Path dataFile) {
    FoundPathInfo pathInfo = foundAttempts.get(pathIdentifier);
    if (pathInfo == null) {
      pathInfo = new FoundPathInfo(indexFile, dataFile);
      foundAttempts.put(pathIdentifier, pathInfo);
    }
    if (pathInfo.isComplete()) {
      // Inform the shuffle handler
      listener.registerAttemptDirs(pathIdentifier,
          new ShuffleHandler.AttemptPathInfo(new org.apache.hadoop.fs.Path(indexFile.toUri()),
              new org.apache.hadoop.fs.Path(dataFile.toUri())));
      // Cancel existing watches
      cancelWatchesForAttempt(pathIdentifier);
      // Cleanup structures
      foundAttempts.remove(pathIdentifier);
    }
  }

  private class WatcherCallable implements Callable<Void> {

    @Override
    public Void call() throws Exception {
      watch();
      return null;
    }
  }

  private class WatchExpirerCallable implements Callable<Void> {

    @Override
    public Void call() {
      while (!shutdown.get()) {
        // Relying on watchService.close to clean up all pending watches
        WatchedPathInfo pathInfo;
        try {
          pathInfo = watchedPathQueue.take();
        } catch (InterruptedException e) {
          if (shutdown.get()) {
            LOG.info("Shutting down WatchExpirer");
            break;
          } else {
            LOG.error("WatchExpirer interrupted before being shutdown");
            throw new RuntimeException("WatchExpirer interrupted before being shutdown", e);
          }
        }
        WatchKey watchKey = pathInfo.getWatchKey();
        if (watchKey != null && watchKey.isValid()) {
          watchKey.cancel();
        }
      }
      return null;
    }
  }


  private static class FoundPathInfo {
    Path indexPath;
    Path dataPath;

    public FoundPathInfo(Path indexPath, Path dataPath) {
      this.indexPath = indexPath;
      this.dataPath = dataPath;
    }

    boolean isComplete() {
      return indexPath != null && dataPath != null;
    }
  }

  private static class WatchedPathInfo implements Delayed {
    final long expiry;
    final Type type;
    final String appId;
    final int dagId;
    final String user;
    final String attemptId;
    final AttemptPathIdentifier pathIdentifier;
    WatchKey watchKey;

    public WatchedPathInfo(long expiry, Type type, String jobId, int dagId, String user) {
      this.expiry = expiry;
      this.type = type;
      this.appId = jobId;
      this.dagId = dagId;
      this.user = user;
      this.attemptId = null;
      this.pathIdentifier = null;
    }

    public WatchedPathInfo(WatchedPathInfo other, Type type, String attemptId) {
      this.expiry = other.expiry;
      this.appId = other.appId;
      this.user = other.user;
      this.dagId = other.dagId;
      this.type = type;
      this.attemptId = attemptId;
      if (attemptId != null) {
        pathIdentifier = new AttemptPathIdentifier(appId, dagId, user, attemptId);
      } else {
        pathIdentifier = null;
      }
    }

    synchronized void setWatchKey(WatchKey watchKey) {
      this.watchKey = watchKey;
    }

    synchronized WatchKey getWatchKey() {
      return this.watchKey;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return expiry - System.currentTimeMillis();
    }

    @Override
    public int compareTo(Delayed o) {
      WatchedPathInfo other = (WatchedPathInfo)o;
      if (other.expiry > this.expiry) {
        return -1;
      } else if (other.expiry < this.expiry) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
