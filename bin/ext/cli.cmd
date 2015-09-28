@echo off
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.

if [%1]==[cli_help] goto :cli_help

:cli
	call :update_cli
	call %HIVE_BIN_PATH%\ext\util\execHiveCmd.cmd %CLASS%
goto :EOF

:cli_help
	call :update_cli
	set HIVEARGS=--help
	call :cli
goto :EOF

:update_cli
	if [%USE_DEPRECATED_CLI%] == [] (
		set USE_DEPRECATED_CLI=false
	)

	if /I "%USE_DEPRECATED_CLI%" == "true" (
		call :old_cli
	) else (
		call :new_cli
	)
goto :EOF

:old_cli
	set CLASS=org.apache.hadoop.hive.cli.CliDriver
	pushd %HIVE_LIB%
	for /f %%a IN ('dir /b hive-cli-*.jar') do (
		set JAR=%HIVE_LIB%\%%a
	)
	popd
goto :EOF

:new_cli
	set CLASS=org.apache.hive.beeline.cli.HiveCli
	pushd %HIVE_LIB%
	for /f %%a IN ('dir /b hive-beeline-*.jar') do (
		set JAR=%HIVE_LIB%\%%a
	)
	popd
goto :EOF