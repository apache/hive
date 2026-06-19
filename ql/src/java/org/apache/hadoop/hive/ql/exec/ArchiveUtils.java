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

package org.apache.hadoop.hive.ql.exec;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * ArchiveUtils.
 */
@SuppressWarnings("nls")
public final class ArchiveUtils {
  public static final String ARCHIVING_LEVEL = "archiving_level";

  /**
   * PartSpecInfo keeps fields and values extracted from partial partition info
   * which is prefix of the full info.
   */
  public static class PartSpecInfo {
    public List<FieldSchema> fields;
    public List<String> values;
    private PartSpecInfo(List<FieldSchema> fields, List<String> values) {
      this.fields = fields;
      this.values = values;
    }

    /**
     * Extract partial prefix specification from table and key-value map
     *
     * @param tbl table in which partition is
     * @param partSpec specification of partition
     * @return extracted specification
     */
    static public PartSpecInfo create(Table tbl, Map<String, String> partSpec)
        throws HiveException {
      // we have to check if we receive prefix of partition keys so in table
      // scheme like table/ds=2011-01-02/hr=13/
      // ARCHIVE PARTITION (ds='2011-01-02') will work and
      // ARCHIVE PARTITION(hr='13') won't
      List<FieldSchema> prefixFields = new ArrayList<FieldSchema>();
      List<String> prefixValues = new ArrayList<String>();
      List<FieldSchema> partCols = tbl.getPartCols();
      Iterator<String> itrPsKeys = partSpec.keySet().iterator();
      for (FieldSchema fs : partCols) {
        if (!itrPsKeys.hasNext()) {
          break;
        }
        if (!itrPsKeys.next().toLowerCase().equals(
            fs.getName().toLowerCase())) {
          throw new HiveException("Invalid partition specification: "
              + partSpec);
        }
        prefixFields.add(fs);
        prefixValues.add(partSpec.get(fs.getName()));
      }

      return new PartSpecInfo(prefixFields, prefixValues);
    }

    /**
     * Creates path where partitions matching prefix should lie in filesystem
     * @param tbl table in which partition is
     * @return expected location of partitions matching prefix in filesystem
     */
    public Path createPath(Table tbl) throws HiveException {
      String prefixSubdir;
      try {
        prefixSubdir = Warehouse.makePartName(fields, values);
      } catch (MetaException e) {
        throw new HiveException("Unable to get partitions directories prefix", e);
      }
      Path tableDir = tbl.getDataLocation();
      if (tableDir == null) {
        throw new HiveException("Table has no location set");
      }
      return new Path(tableDir, prefixSubdir);
    }
    /**
     * Generates name for prefix partial partition specification.
     */
    public String getName() throws HiveException {
      try {
        return Warehouse.makePartName(fields, values);
      } catch (MetaException e) {
        throw new HiveException("Unable to create partial name", e);
      }
    }
  }

  /**
   * HarPathHelper helps to create har:/ URIs for locations inside of archive.
   */
  public static class HarPathHelper {
    private final URI base, originalBase;

    /**
     * Creates helper for archive.
     * @param archive absolute location of archive in underlying filesystem
     * @param originalBase directory for which Hadoop archive was created
     */
    public HarPathHelper(HiveConf hconf, URI archive, URI originalBase) throws HiveException {
      this.originalBase = addSlash(originalBase);
      String parentHost = archive.getHost();
      String harHost = archive.getScheme();
      if (parentHost == null) {
        harHost += "-localhost";
      } else {
        harHost += "-" + parentHost;
      }

      // have to make sure there's slash after .har, otherwise resolve doesn't work
      String path = StringUtils.appendIfMissing(archive.getPath(), "/");
      if (!path.endsWith(".har/")) {
        throw new HiveException("HAR archive path must end with .har");
      }
      // harUri is used to access the partition's files, which are in the archive
      // The format of the RI is something like:
      // har://underlyingfsscheme-host:port/archivepath
      try {
        base = new URI("har", archive.getUserInfo(), harHost, archive.getPort(),
              path, archive.getQuery(), archive.getFragment());
      } catch (URISyntaxException e) {
        throw new HiveException("Could not create har URI from archive URI", e);
      }
    }

    public URI getHarUri(URI original) throws URISyntaxException {
      URI relative = originalBase.relativize(original);
      if (relative.isAbsolute()) {
        throw new URISyntaxException("Could not create URI for location.",
            "Relative: " + relative + " Base: " + base + " OriginalBase: "
                + originalBase);
      }
      return base.resolve(relative);
    }
  }

  /**
   * Makes sure, that URI points to directory by adding slash to it.
   * Useful in relativizing URIs.
   */
  public static URI addSlash(URI u) throws HiveException {
    if (u.getPath().endsWith("/")) {
      return u;
    }
    try {
      return new URI(u.getScheme(), u.getAuthority(), u.getPath() + "/", u.getQuery(), u.getFragment());
    } catch (URISyntaxException e) {
      throw new HiveException("Could not append slash to a URI", e);
    }
  }

  /**
   * Determines whether a partition has been archived
   *
   * @param p
   * @return is it archived?
   */
  public static boolean isArchived(Partition p) {
    return MetaStoreUtils.isArchived(p.getTPartition());

  }

  /**
   * Returns archiving level, which is how many fields were set in partial
   * specification ARCHIVE was run for
   */
  public static int getArchivingLevel(Partition p) throws HiveException {
    try {
      return MetaStoreUtils.getArchivingLevel(p.getTPartition());
    } catch (MetaException ex) {
      throw new HiveException(ex.getMessage(), ex);
    }
  }

  /**
   * Get a prefix of the given parition's string representation. The second
   * argument, level, is used for the prefix length. For example, partition
   * (ds='2010-01-01', hr='00', min='00'), level 1 will return 'ds=2010-01-01',
   * and level 2 will return 'ds=2010-01-01/hr=00'.
   *
   * @param p
   *          partition object
   * @param level
   *          level for prefix depth
   * @return prefix of partition's string representation
   * @throws HiveException
   */
  public static String getPartialName(Partition p, int level) throws HiveException {
    List<FieldSchema> fields = p.getTable().getPartCols().subList(0, level);
    List<String> values = p.getValues().subList(0, level);
    try {
      return Warehouse.makePartName(fields, values);
    } catch (MetaException e) {
      throw new HiveException("Wasn't able to generate name" +
                                " for partial specification");
    }
  }

  /**
   * Determines if one can insert into partition(s), or there's a conflict with
   * archive. It can be because partition is itself archived, or it is to be
   * created inside existing archive. The second case is when partition doesn't
   * exist yet, but it would be inside an archive if it existed. This one is
   * quite tricky to check, we need to find at least one partition inside
   * the parent directory. If it is archived and archiving level tells that
   * the archival was done of directory partition is in it means we cannot
   * insert; otherwise we can.
   * This method works both for full specifications and partial ones - in second
   * case it checks if any partition that could possibly match such
   * specification is inside archive.
   *
   * @param db - Hive object
   * @param tbl - table where partition is
   * @param partSpec - partition specification with possible nulls in case of
   * dynamic partiton inserts
   * @return null if partition can be inserted, string with colliding archive
   * name when it can't
   * @throws HiveException
   */
  public static String conflictingArchiveNameOrNull(Hive db, Table tbl,
        LinkedHashMap<String, String> partSpec)
      throws HiveException {

    List<FieldSchema> partKeys = tbl.getPartitionKeys();
    int partSpecLevel = 0;
    for (FieldSchema partKey : partKeys) {
      if (!partSpec.containsKey(partKey.getName())) {
        break;
      }
      partSpecLevel++;
    }

    if (partSpecLevel != partSpec.size()) {
      throw new HiveException(
          "partspec " + partSpec + " is wrong for table " + tbl.getTableName());
    }

    Map<String, String> spec = new HashMap<String, String>(partSpec);
    List<String> reversedKeys = new ArrayList<String>();
    for (FieldSchema fs : tbl.getPartCols()) {
      if (spec.containsKey(fs.getName())) {
        reversedKeys.add(fs.getName());
      }
    }

    Collections.reverse(reversedKeys);

    for (String rk : reversedKeys) {
      List<Partition> parts = db.getPartitions(tbl, spec, (short) 1);
      if (parts.size() != 0) {
        Partition p = parts.get(0);
        if (!isArchived(p)) {
          // if archiving was done at this or at upper level, every matched
          // partition would be archived, so it not being archived means
          // no archiving was done neither at this nor at upper level
          return null;
        }
        if (getArchivingLevel(p) > spec.size()) {
          // if archiving was done at this or at upper level its level
          // would be lesser or equal to specification size
          // it is not, which means no archiving at this or upper level
          return null;
        }
        return getPartialName(p, getArchivingLevel(p));
      }
      spec.remove(rk);
    }
    return null;
  }
}
