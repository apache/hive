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


@rem The Hadoop command script
@rem
@rem Environment Variables
@rem
@rem   JAVA_HOME        The java implementation to use.  Overrides JAVA_HOME.
@rem
@rem   HADOOP_CLASSPATH Extra Java CLASSPATH entries.
@rem
@rem   HADOOP_HEAPSIZE  The maximum amount of heap to use, in MB.
@rem                    Default is 1000.
@rem
@rem   HADOOP_OPTS      Extra Java runtime options.
@rem
@rem   HADOOP_NAMENODE_OPTS       These options are added to HADOOP_OPTS
@rem   HADOOP_CLIENT_OPTS         when the respective command is run.
@rem   HADOOP_{COMMAND}_OPTS etc  HADOOP_JT_OPTS applies to JobTracker
@rem                              for e.g.  HADOOP_CLIENT_OPTS applies to
@rem                              more than one command (fs, dfs, fsck,
@rem                              dfsadmin etc)
@rem
@rem   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_HOME}/conf.
@rem
@rem   HADOOP_ROOT_LOGGER The root appender. Default is INFO,console
@rem

if not defined HADOOP_BIN_PATH ( 
  set HADOOP_BIN_PATH=%~dp0
)

if "%HADOOP_BIN_PATH:~-1%" == "\" (
  set HADOOP_BIN_PATH=%HADOOP_BIN_PATH:~0,-1%
)
call :updatepath %HADOOP_BIN_PATH%

set BIN=%~dp0
for %%i in (%BIN%.) do (
  set BIN=%%~dpi
)
if "%BIN:~-1%" == "\" (
  set BIN=%BIN:~0,-1%
)


@rem
@rem setup java environment variables
@rem

if not defined JAVA_HOME (
  echo Error: JAVA_HOME is not set.
  goto :eof
)

if not exist %JAVA_HOME%\bin\java.exe (
  echo Error: JAVA_HOME is incorrectly set.
  goto :eof
)

set JAVA=%JAVA_HOME%\bin\java
set JAVA_HEAP_MAX=-Xmx1000m

@rem
@rem check envvars which might override default args
@rem

if defined HADOOP_HEAPSIZE (
  set JAVA_HEAP_MAX=-Xmx%HADOOP_HEAPSIZE%m
)

@rem
@rem CLASSPATH initially contains %HADOOP_CONF_DIR%
@rem

set CLASSPATH=%HADOOP_CONF_DIR%
set CLASSPATH=%CLASSPATH%;%JAVA_HOME%\lib\tools.jar


set BUILD_ROOT="%BIN%"/build


if not defined HIVE_HADOOP_TEST_CLASSPATH (
  @echo Error: HIVE_HADOOP_TEST_CLASSPATH not defined.
  goto :eof
)



set CLASSPATH=%CLASSPATH%;%HIVE_HADOOP_TEST_CLASSPATH%
if not exist %BUILD_ROOT%/test/hadoop/logs (
  mkdir %BUILD_ROOT%/test/hadoop/logs
)

@rem
@rem add user-specified CLASSPATH last
@rem

if defined HADOOP_CLASSPATH (
  set CLASSPATH=%CLASSPATH%;%HADOOP_CLASSPATH%
)

if not defined HADOOP_LOG_DIR (
  set HADOOP_LOG_DIR=%BUILD_ROOT%\logs
)

if not defined HADOOP_LOGFILE (
  set HADOOP_LOGFILE=hadoop.log
)

if not defined HADOOP_ROOT_LOGGER (
  set HADOOP_ROOT_LOGGER=INFO,console,DRFA
)

@rem
@rem default policy file for service-level authorization
@rem

if not defined HADOOP_POLICYFILE (
  set HADOOP_POLICYFILE=hadoop-policy.xml
)
set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.log.dir=%HADOOP_LOG_DIR%
set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.log.file=%HADOOP_LOGFILE%
set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.root.logger=%HADOOP_ROOT_LOGGER%

if defined HADOOP_PREFIX (
  set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.home.dir=%HADOOP_PREFIX%
)

if defined HADOOP_IDENT_STRING (
  set HADOOP_OPTS=%$HADOOP_OPTS% -Dhadoop.id.str=%HADOOP_IDENT_STRING%
)

if defined JAVA_LIBRARY_PATH (
  set HADOOP_OPTS=%HADOOP_OPTS% -Djava.library.path=%JAVA_LIBRARY_PATH%
)
set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.policy.file=%HADOOP_POLICYFILE%

@rem Disable ipv6 as it can cause issues
set HADOOP_OPTS=%HADOOP_OPTS% -Djava.net.preferIPv4Stack=true

:main
  setlocal enabledelayedexpansion
  
  set hadoop-command=%1
  if not defined hadoop-command (
      goto print_usage
  )
  
  call :make_command_arguments %*
  set corecommands=fs version jar distcp daemonlog archive
  for %%i in ( %corecommands% ) do (
    if %hadoop-command% == %%i set corecommand=true  
  )
  if defined corecommand (
    call :%hadoop-command%
  ) else (
    set CLASSPATH=%CLASSPATH%;%CD%
    set CLASS=%hadoop-command%
  )
  call %JAVA% %JAVA_HEAP_MAX% %HADOOP_OPTS% -classpath %CLASSPATH% %CLASS% %hadoop-command-arguments%
  exit /b %ERRORLEVEL%
  goto :eof

:version 
  set CLASS=org.apache.hadoop.util.VersionInfo
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:jar
  set CLASS=org.apache.hadoop.util.RunJar
  goto :eof

:distcp
  set CLASS=org.apache.hadoop.tools.DistCp
  set CLASSPATH=%CLASSPATH%;%TOOL_PATH%
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:daemonlog
  set CLASS=org.apache.hadoop.log.LogLevel
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:archive
  set CLASS=org.apache.hadoop.tools.HadoopArchives
  set CLASSPATH=%CLASSPATH%;%TOOL_PATH%
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:updatepath
  set path_to_add=%*
  set current_path_comparable=%path:(x86)=%
  set current_path_comparable=%current_path_comparable: =_%
  set path_to_add_comparable=%path_to_add:(x86)=%
  set path_to_add_comparable=%path_to_add_comparable: =_%
  for %%i in ( %current_path_comparable% ) do (
    if /i "%%i" == "%path_to_add_comparable%" (
      set path_to_add_exist=true
    )
  )
  set system_path_comparable=
  set path_to_add_comparable=
  if not defined path_to_add_exist path=%path_to_add%;%path%
  set path_to_add=
  goto :eof

:make_command_arguments
  if "%2" == "" goto :eof
  set _count=0
  set _shift=1
  for %%i in (%*) do (
    set /a _count=!_count!+1
    if !_count! GTR %_shift% ( 
  if not defined _arguments (
   set _arguments=%%i
  ) else (
          set _arguments=!_arguments! %%i
  )
    )
  )
  
  set hadoop-command-arguments=%_arguments%
  goto :eof

:print_usage
  @echo Usage: hadoop COMMAND
  @echo where COMMAND is one of:
  @echo   fs                   run a generic filesystem user client
  @echo   version              print the version
  @echo   jar ^<jar^>            run a jar file
  @echo.
  @echo   distcp ^<srcurl^> ^<desturl^> copy file or directories recursively
  @echo   archive -archiveName NAME ^<src^>* ^<dest^> create a hadoop archive
  @echo   daemonlog            get/set the log level for each daemon
  @echo Most commands print help when invoked w/o parameters.

endlocal
