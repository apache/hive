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

import org.apache.commons.lang3.StringUtils;
import org.flywaydb.core.api.resolver.MigrationExecutor;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.util.logging.Log;
import org.flywaydb.core.internal.util.logging.LogFactory;
import org.flywaydb.core.internal.util.scanner.Resource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Database migration based on a shell file.
 */
public class ShellMigrationExecutor implements MigrationExecutor {
    private static final Log LOG = LogFactory.getLog(ShellMigrationExecutor.class);

    /**
     * The Resource pointing to the shell script.
     * The complete shell script is not held as a member field here because this would use the total size of all
     * shell migrations files in heap space during db migration, see issue 184.
     */
    private final Resource shellScriptResource;

    /**
     * Creates a new shell script migration based on this shell script.
     *
     * @param shellScriptResource The resource containing the sql script.
     */
    public ShellMigrationExecutor(Resource shellScriptResource) {
        this.shellScriptResource = shellScriptResource;
    }

    @Override
    public void execute(Connection connection) throws SQLException {
        String scriptLocation = this.shellScriptResource.getLocationOnDisk();
        try {
            List<String> args = new ArrayList<String>();
            args.add(scriptLocation);
            ProcessBuilder builder = new ProcessBuilder(args);
            builder.redirectErrorStream(true);
            Process process = builder.start();
            Scanner in = new Scanner(process.getInputStream());
            System.out.println(StringUtils.repeat("+",200));
            while (in.hasNextLine()) {
                System.out.println(in.nextLine());
            }
            int returnCode = process.waitFor();
            System.out.println(StringUtils.repeat("+",200));
            if (returnCode != 0) {
                throw new FlywayException("script exited with value : " + returnCode);
            }
        } catch (Exception e) {
            LOG.error(e.toString());
            // Only if SQLException or FlywaySqlScriptException is thrown flyway will mark the migration as failed in the metadata table
            throw new SQLException(String.format("Failed to run script \"%s\", %s", scriptLocation, e.getMessage()), e);
        }
    }

    @Override
    public boolean executeInTransaction() {
        return true;
    }
}