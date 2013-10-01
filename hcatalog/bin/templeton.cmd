@rem Licensed to the Apache Software Foundation (ASF) under one
@rem or more contributor license agreements.  See the NOTICE file
@rem distributed with this work for additional information
@rem regarding copyright ownership.  The ASF licenses this file
@rem to you under the Apache License, Version 2.0 (the
@rem "License"); you may not use this file except in compliance
@rem with the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing,
@rem software distributed under the License is distributed on an
@rem "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@rem KIND, either express or implied.  See the License for the
@rem specific language governing permissions and limitations
@rem under the License.
@echo off

setlocal enabledelayedexpansion

:main
  if "%1" == "--service" (
    set service_entry=true
    shift
  )

  @rem Init hadoop env variables (CLASSPATH, HADOOP_OPTS, etc)
  set HADOOP_OPTS=
  call %HADOOP_HOME%\bin\hadoop-config.cmd

  set templeton-service-name=%1

  if not defined templeton-service-name (
      goto print_usage
  )

  @rem
  @rem Compute the classpath
  @rem
  set TEMPLETON_CONF_DIR=%HCATALOG_HOME%\conf
  set TEMPLETON_CLASSPATH=%TEMPLETON_CONF_DIR%;%HCATALOG_HOME%;%HCATALOG_HOME%\share\webhcat\svr

  set TEMPLETON_CLASSPATH=!TEMPLETON_CLASSPATH!;%HCATALOG_HOME%\share\hcatalog\*
  set TEMPLETON_CLASSPATH=!TEMPLETON_CLASSPATH!;%HCATALOG_HOME%\share\webhcat\svr\*
  set TEMPLETON_CLASSPATH=!TEMPLETON_CLASSPATH!;%HCATALOG_HOME%\share\webhcat\svr\lib\*

  @rem TODO: append hcat classpath to the templeton classpath
  @rem append hadoop classpath
  set CLASSPATH=%TEMPLETON_CLASSPATH%;!CLASSPATH!

  @rem compute templeton ops
  if not defined TEMPLETON_LOG_DIR (
    set TEMPLETON_LOG_DIR=%HCATALOG_HOME%\logs
  )

  if not defined TEMPLETON_LOG4J (
    @rem must be prefixed with file: otherwise config is not picked up
    set TEMPLETON_LOG4J=file:%HCATALOG_HOME%\conf\webhcat-log4j.properties
  )
  set TEMPLETON_OPTS=-Dtempleton.log.dir=%TEMPLETON_LOG_DIR% -Dlog4j.configuration=%TEMPLETON_LOG4J% %HADOOP_OPTS%
  set arguments=%JAVA_HEAP_MAX% %TEMPLETON_OPTS% -classpath %CLASSPATH% org.apache.hcatalog.templeton.Main
  
  if defined service_entry (
    call :makeServiceXml %arguments%
  ) else (
    goto print_usage
  )
  
goto :eof

:makeServiceXml
  set arguments=%*
  @echo ^<service^>
  @echo   ^<id^>%templeton-service-name%^</id^>
  @echo   ^<name^>%templeton-service-name%^</name^>
  @echo   ^<description^>This service runs Apache Templeton^</description^>
  @echo   ^<executable^>%JAVA%^</executable^>
  @echo   ^<arguments^>%arguments%^</arguments^>
  @echo ^</service^>
  goto :eof
  
 :print_usage
  @echo Usage: templeton --service SERVICENAME
  @echo        where SERVICENAME is name of the windows service xml
  
endlocal
