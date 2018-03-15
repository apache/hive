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


package org.apache.hadoop.hive.registry.storage.tool.shell;

import org.flywaydb.core.api.MigrationType;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.resolver.MigrationResolver;
import org.flywaydb.core.api.resolver.ResolvedMigration;
import org.flywaydb.core.internal.resolver.MigrationInfoHelper;
import org.flywaydb.core.internal.resolver.ResolvedMigrationComparator;
import org.flywaydb.core.internal.resolver.ResolvedMigrationImpl;
import org.flywaydb.core.internal.util.Location;
import org.flywaydb.core.internal.util.logging.Log;
import org.flywaydb.core.internal.util.logging.LogFactory;
import org.flywaydb.core.internal.util.Pair;
import org.flywaydb.core.internal.util.scanner.Resource;
import org.flywaydb.core.internal.util.scanner.Scanner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

public class ShellMigrationResolver implements MigrationResolver {
    private static final Log LOG = LogFactory.getLog(ShellMigrationResolver.class);

    /**
     * The scanner to use.
     */
    private final Scanner scanner;

    /**
     * The base directory on the classpath where to migrations are located.
     */
    private final Location location;

    /**
     * The prefix for shell migrations
     */
    private final String shellMigrationPrefix;

    /**
     * The separator for shell migrations
     */
    private final String shellMigrationSeparator;

    /**
     * The suffix for shell migrations
     */
    private final String shellMigrationSuffix;

    /**
     * Creates a new instance.
     *
     * @param classLoader             The ClassLoader for loading migrations on the classpath.
     * @param location                The location on the classpath where to migrations are located.
     * @param shellMigrationPrefix    The prefix for shell migrations
     * @param shellMigrationSeparator The separator for shell migrations
     * @param shellMigrationSuffix    The suffix for shell migrations
     */
    public ShellMigrationResolver(ClassLoader classLoader, String location, String shellMigrationPrefix,
                                  String shellMigrationSeparator, String shellMigrationSuffix) {
        this.scanner = new Scanner(classLoader);
        this.location = new Location(location);
        this.shellMigrationPrefix = shellMigrationPrefix;
        this.shellMigrationSeparator = shellMigrationSeparator;
        this.shellMigrationSuffix = shellMigrationSuffix;
    }

    public List<ResolvedMigration> resolveMigrations() {
        List<ResolvedMigration> migrations = new ArrayList<ResolvedMigration>();

        Resource[] resources = scanner.scanForResources(location, shellMigrationPrefix, shellMigrationSuffix);
        for (Resource resource : resources) {
            ResolvedMigrationImpl resolvedMigration = extractMigrationInfo(resource);
            resolvedMigration.setPhysicalLocation(resource.getLocationOnDisk());
            resolvedMigration.setExecutor(new ShellMigrationExecutor(resource));

            migrations.add(resolvedMigration);
        }

        Collections.sort(migrations, new ResolvedMigrationComparator());
        return migrations;
    }

    /**
     * Extracts the migration info for this resource.
     *
     * @param resource The resource to analyse.
     * @return The migration info.
     */
    private ResolvedMigrationImpl extractMigrationInfo(Resource resource) {
        ResolvedMigrationImpl migration = new ResolvedMigrationImpl();

        Pair<MigrationVersion, String> info =
                MigrationInfoHelper.extractVersionAndDescription(resource.getFilename(),
                        shellMigrationPrefix, shellMigrationSeparator, shellMigrationSuffix, false);
        migration.setVersion(info.getLeft());
        migration.setDescription(info.getRight());

        migration.setScript(extractScriptName(resource));

        migration.setChecksum(calculateChecksum(resource.loadAsBytes()));
        migration.setType(MigrationType.CUSTOM);
        return migration;
    }

    /**
     * Extracts the script name from this resource.
     *
     * @param resource The resource to process.
     * @return The script name.
     */
    /* private -> for testing */ String extractScriptName(Resource resource) {
        if (location.getPath().isEmpty()) {
            return resource.getLocation();
        }

        return resource.getLocation().substring(location.getPath().length() + 1);
    }

    /**
     * Calculates the checksum of these bytes.
     *
     * @param bytes The bytes to calculate the checksum for.
     * @return The crc-32 checksum of the bytes.
     */
    private static int calculateChecksum(byte[] bytes) {
        final CRC32 crc32 = new CRC32();
        crc32.update(bytes);
        return (int) crc32.getValue();
    }
}
