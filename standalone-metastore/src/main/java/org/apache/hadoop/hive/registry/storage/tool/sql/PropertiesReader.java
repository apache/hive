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

import java.io.IOException;
import java.util.Map;

public class PropertiesReader {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("USAGE: [config file path] [property key]");
            System.exit(1);
        }

        String configFilePath = args[0];
        String propertyKey = args[1];
        try {
            Map<String, Object> conf = Utils.readConfig(configFilePath);
            if (!conf.containsKey(propertyKey)) {
                System.err.println("The key " + propertyKey + " is not defined to the config file.");
                System.exit(3);
            }

            System.out.println(conf.get(propertyKey));
        } catch (IOException e) {
            System.err.println("Error occurred while reading config file: " + configFilePath);
            System.exit(2);
        }
    }
}
