download() {
  url=$1;
  finalName=$2
  tarName=$(basename $url)
  rm -rf $BASE_DIR/$finalName
  if [[ ! -f $DOWNLOAD_DIR/$tarName ]]
  then
    curl -Sso $DOWNLOAD_DIR/$tarName $url
  else
    local md5File="$tarName".md5sum
    curl -Sso $DOWNLOAD_DIR/$md5File "$url".md5sum
    cd $DOWNLOAD_DIR
    if type md5sum >/dev/null && ! md5sum -c $md5File; then
      curl -Sso $DOWNLOAD_DIR/$tarName $url || return 1
    elif type md5 >/dev/null; then
      md5chksum=$(md5 -r $tarName)
      if [[ $(< $md5File) != $md5chksum ]]; then
        curl -Sso $DOWNLOAD_DIR/$tarName $url || return 1
      fi
    fi

    cd -
  fi

  tar -zxf $DOWNLOAD_DIR/$tarName -C $BASE_DIR
  mv $BASE_DIR/spark-$SPARK_VERSION-bin-hadoop3-beta1-without-hive $BASE_DIR/$finalName
}

set -x
/bin/pwd
BASE_DIR=./target
HIVE_ROOT=$BASE_DIR/../../../
DOWNLOAD_DIR=./../thirdparty
SPARK_VERSION=$1

mkdir -p $DOWNLOAD_DIR
download "http://d3jw87u4immizc.cloudfront.net/spark-tarball/spark-$SPARK_VERSION-bin-hadoop3-beta1-without-hive.tgz" "spark"
cp -f $HIVE_ROOT/data/conf/spark/log4j2.properties $BASE_DIR/spark/conf/

