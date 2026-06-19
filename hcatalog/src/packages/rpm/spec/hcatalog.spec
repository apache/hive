#   Licensed to the Apache Software Foundation (ASF) under one or more
#   contributor license agreements.  See the NOTICE file distributed with
#   this work for additional information regarding copyright ownership.
#   The ASF licenses this file to You under the Apache License, Version 2.0
#   (the "License"); you may not use this file except in compliance with
#   the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

#
# RPM Spec file for Hcatalog version @version@
#

%define name         hcatalog
%define version      @version@
%define release      @package.release@

# Installation Locations
%define _prefix      @package.prefix@
%define _bin_dir     %{_prefix}/bin
%define _conf_dir    @package.conf.dir@
%define _lib_dir     %{_prefix}/lib
%define _lib64_dir   %{_prefix}/lib64
%define _libexec_dir %{_prefix}/libexec
%define _log_dir     @package.log.dir@
%define _pid_dir     @package.pid.dir@
%define _sbin_dir    %{_prefix}/sbin
%define _share_dir   %{_prefix}/share
%define _var_dir     @package.var.dir@

# Build time settings
%define _build_dir  @package.build.dir@
%define _final_name @final.name@
%define debug_package %{nil}

# Disable brp-java-repack-jars
%define __os_install_post    \
    /usr/lib/rpm/redhat/brp-compress \
    %{!?__debug_package:/usr/lib/rpm/redhat/brp-strip %{__strip}} \
    /usr/lib/rpm/redhat/brp-strip-static-archive %{__strip} \
    /usr/lib/rpm/redhat/brp-strip-comment-note %{__strip} %{__objdump} \
    /usr/lib/rpm/brp-python-bytecompile %{nil}

# RPM searches perl files for dependancies and this breaks for non packaged perl lib
# like thrift so disable this
%define _use_internal_dependency_generator 0

Summary: Apache HCatalog is a table and storage management service for data created using Apache Hadoop.
License: Apache License, Version 2.0
URL: http://incubator.apache.org/hcatalog
Vendor: Apache Software Foundation
Group: Development/Libraries
Name: %{name}
Version: %{version}
Release: %{release} 
Source0: %{_final_name}.tar.gz
Prefix: %{_prefix}
Prefix: %{_conf_dir}
Prefix: %{_log_dir}
Prefix: %{_pid_dir}
Buildroot: %{_build_dir}
Requires: sh-utils, textutils, /usr/sbin/useradd, /usr/sbin/usermod, /sbin/chkconfig, /sbin/service
AutoReqProv: no
Provides: hcatalog

%description
The Apache HCatalog is a table and storage management service for data created using Apache Hadoop.

%prep
%setup -n %{_final_name}

%build
if [ -d ${RPM_BUILD_DIR}%{_prefix} ]; then
  rm -rf ${RPM_BUILD_DIR}%{_prefix}
fi

if [ -d ${RPM_BUILD_DIR}%{_log_dir} ]; then
  rm -rf ${RPM_BUILD_DIR}%{_log_dir}
fi

if [ -d ${RPM_BUILD_DIR}%{_conf_dir} ]; then
  rm -rf ${RPM_BUILD_DIR}%{_conf_dir}
fi

if [ -d ${RPM_BUILD_DIR}%{_pid_dir} ]; then
  rm -rf ${RPM_BUILD_DIR}%{_pid_dir}
fi

mkdir -p ${RPM_BUILD_DIR}%{_prefix}
mkdir -p ${RPM_BUILD_DIR}%{_bin_dir}
mkdir -p ${RPM_BUILD_DIR}%{_lib_dir}
%ifarch amd64 x86_64
mkdir -p ${RPM_BUILD_DIR}%{_lib64_dir}
%endif
mkdir -p ${RPM_BUILD_DIR}%{_libexec_dir}
mkdir -p ${RPM_BUILD_DIR}%{_log_dir}
mkdir -p ${RPM_BUILD_DIR}%{_conf_dir}
mkdir -p ${RPM_BUILD_DIR}%{_pid_dir}
mkdir -p ${RPM_BUILD_DIR}%{_sbin_dir}
mkdir -p ${RPM_BUILD_DIR}%{_share_dir}
mkdir -p ${RPM_BUILD_DIR}%{_var_dir}
mkdir -p ${RPM_BUILD_DIR}/etc/init.d

cp ${RPM_BUILD_DIR}/%{_final_name}/src/packages/rpm/init.d/hcatalog-server ${RPM_BUILD_DIR}/etc/init.d/hcatalog-server
chmod 0755 ${RPM_BUILD_DIR}/etc/init.d/hcatalog-server

#########################
#### INSTALL SECTION ####
#########################
%install
mv ${RPM_BUILD_DIR}/%{_final_name}/bin/* ${RPM_BUILD_DIR}%{_bin_dir}
mv ${RPM_BUILD_DIR}/%{_final_name}/libexec/* ${RPM_BUILD_DIR}%{_libexec_dir}
mv ${RPM_BUILD_DIR}/%{_final_name}/sbin/* ${RPM_BUILD_DIR}%{_sbin_dir}
mv ${RPM_BUILD_DIR}/%{_final_name}/etc/hcatalog/* ${RPM_BUILD_DIR}%{_conf_dir}
mv ${RPM_BUILD_DIR}/%{_final_name}/share/* ${RPM_BUILD_DIR}%{_share_dir}
rm -rf ${RPM_BUILD_DIR}/%{_final_name}

%pre
getent group hadoop 2>/dev/null >/dev/null || /usr/sbin/groupadd -r hadoop
/usr/sbin/useradd --comment "HCatalog" --shell /bin/bash -M -r -g hadoop --home /usr/share/hcatalog hcat 2> /dev/null || :

%post
bash ${RPM_INSTALL_PREFIX0}/sbin/update-hcatalog-env.sh \
       --prefix=${RPM_INSTALL_PREFIX0} \
       --bin-dir=${RPM_INSTALL_PREFIX0}/bin \
       --sbin-dir=${RPM_INSTALL_PREFIX0}/sbin \
       --conf-dir=${RPM_INSTALL_PREFIX1} \
       --log-dir=${RPM_INSTALL_PREFIX2} \
       --pid-dir=${RPM_INSTALL_PREFIX3} \
       --dbjars=/usr/share/java

if [ ! -f ${RPM_INSTALL_PREFIX1}/hive-site.xml ]; then
	cp ${RPM_INSTALL_PREFIX1}/proto-hive-site.xml ${RPM_INSTALL_PREFIX1}/hive-site.xml
fi

ln -sf ${RPM_INSTALL_PREFIX1} ${RPM_INSTALL_PREFIX0}/share/hcatalog/conf 

echo HCatalog installed, please take a moment to verify config in ${RPM_INSTALL_PREFIX1}/hcat-env.sh and ${RPM_INSTALL_PREFIX1}/hive-site.xml
%preun
bash ${RPM_INSTALL_PREFIX0}/sbin/update-hcatalog-env.sh \
       --prefix=${RPM_INSTALL_PREFIX0} \
       --bin-dir=${RPM_INSTALL_PREFIX0}/bin \
       --sbin-dir=${RPM_INSTALL_PREFIX0}/sbin \
       --conf-dir=${RPM_INSTALL_PREFIX1} \
       --log-dir=${RPM_INSTALL_PREFIX2} \
       --pid-dir=${RPM_INSTALL_PREFIX3} \
       --uninstall

rm -f ${RPM_INSTALL_PREFIX0}/share/hcatalog/conf

%files 
%defattr(-,root,root)
%attr(0755,hcat,hadoop) %{_log_dir}
%attr(0775,hcat,hadoop) %{_pid_dir}
%config(noreplace) %{_conf_dir}
%{_prefix}
%exclude %{_prefix}/sbin/hcat_server.sh
%exclude %{_prefix}/share/hcatalog/hcatalog-server-extensions*

%package server
Summary: HCatalog Server
Group: System/Daemons
Requires: hcatalog

%description server
HCatalog Metadata Store Server

%files server
%attr(0775,root,hadoop) /etc/init.d/hcatalog-server
%{_prefix}/sbin/hcat_server.sh
%{_prefix}/share/hcatalog/hcatalog-server-extensions*
