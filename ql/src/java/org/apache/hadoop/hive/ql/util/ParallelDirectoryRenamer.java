package org.apache.hadoop.hive.ql.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.BlobStorageUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Given a source directory and a destination directory, moves all the files under the source to the destination
 * folder. Rename operations are done using the specified {@link ExecutorService}.
 *
 * <p>
 *   This class is useful when running on blob stores where rename operations require copying data from one location
 *   to another. Specifically, this method should be used if the blobstore connector renames files under a directory
 *   sequentially. This class will issue the renames in parallel, which can offer significant performance
 *   improvements.
 * </p>
 */
public class ParallelDirectoryRenamer {

  private static final Logger LOG = LoggerFactory.getLogger(ParallelDirectoryRenamer.class);

  /**
   * Move all files under the srcPath to the destPath. The method preserves the behavior of a normal
   * {@link FileSystem#rename(Path, Path)} operation, regardless of whether or not the src and dst paths exist, or if
   * they are files or directories.
   *
   * <p>
   *   Both the source and destination {@link FileSystem}s must be be blobstores, and they should both be of the same
   *   class. This method is targeted for a very specific use case, copies of directories within the same blobstore.
   * </p>
   *
   * @param conf          the {@link Configuration} to use when setting permissions
   * @param srcFs         the source {@link FileSystem}
   * @param destFs        the destination {@link FileSystem}
   * @param srcPath       the source {@link Path}
   * @param destPath      the destination {@link Path}
   * @param inheritPerms  if true, renamed files with inherit their parent permissions, if false they will preserve
   *                      their original permissions
   * @param parentSession the parent {@link SessionState}
   * @param pool          the {@link ExecutorService} to use to issue all the {@link FileSystem#rename(Path, Path)}
   *                      requests
   *
   * @throws IOException   if their is an issuing renaming the files
   * @throws HiveException if any other exception occurs while renaming the files
   */
  public static void renameDirectoryInParallel(final Configuration conf, final FileSystem srcFs,
                                               final FileSystem destFs, final Path srcPath,
                                               final Path destPath, final boolean inheritPerms,
                                               final SessionState parentSession,
                                               ExecutorService pool) throws IOException, HiveException {

    Preconditions.checkArgument(srcFs.getClass().equals(destFs.getClass()),
            "Source and destination filesystems must of the same type");
    Preconditions.checkArgument(BlobStorageUtils.isBlobStorageFileSystem(conf, srcFs),
            "Source and destinations filesystems must both be blobstores");
    Preconditions.checkArgument(srcFs.exists(srcPath), "Source Path " + srcPath + " does not exist");

    if (srcFs.isDirectory(srcPath)) {

      // If the destination doesn't exist, create it and move all files under srcPath/ to destPath/
      // If the destination does exist, then move all files under destPath/srcPath.name/, this is inline with the
      // normal behavior of the FileSystem.rename operation
      Path basePath;
      if (!destFs.exists(destPath)) {
        destFs.mkdirs(destPath);
        basePath = destPath;
      } else {
        basePath = new Path(destPath, srcPath.getName());
        Preconditions.checkArgument(!destFs.exists(basePath), "Path " + basePath + " already exists");
      }

      final HadoopShims shims = ShimLoader.getHadoopShims();
      final HadoopShims.HdfsFileStatus desiredStatus = shims.getFullFileStatus(destFs.getConf(), destFs,
              destPath);

      List<Future<Void>> futures = new ArrayList<>();

      for (final FileStatus srcStatus : srcFs.listStatus(srcPath)) {
        final Path destFile = new Path(basePath, srcStatus.getPath().getName());
        final String group = srcStatus.getGroup();
        futures.add(pool.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            SessionState.setCurrentSessionState(parentSession);
            if (destFs.rename(srcStatus.getPath(), destFile)) {
              if (inheritPerms) {
                shims.setFullFileStatus(conf, desiredStatus, group, destFs, destFile, false);
              }
            } else {
              throw new IOException("rename for src path: " + srcStatus.getPath() + " to dest path: "
                      + destFile + " returned false");
            }
            return null;
          }
        }));
      }

      for (Future<Void> future : futures) {
        try {
          future.get();
        } catch (Exception e) {
          LOG.debug(e.getMessage());
          pool.shutdownNow();
          throw new HiveException(e);
        }
      }
    } else {
      if (!destFs.rename(srcPath, destPath)) {
        throw new IOException("rename for src path: " + srcPath + " to dest path: " + destPath + " returned false");
      }
    }
  }
}
