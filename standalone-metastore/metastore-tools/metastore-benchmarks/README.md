## Hive Metastore micro-benchmarks

## Installation

    mvn clean install

You can run tests as well. Just set `HMS_HOST` environment variable to some HMS instance which is
capable of running your requests (non-kerberised one) and run

    mvn install

target directory has two mega-jars which have all the dependencies.

Alternatively you can use [bin/hbench](../bin/hbench) script which use Maven to run the code.

## HmsBench usage

    Usage: BenchmarkTool [-ChlV] [--sanitize] [--confdir=<confDir>]
                         [--params=<nParameters>] [--savedata=<dataSaveDir>]
                         [--separator=<csvSeparator>] [-d=<dbName>] [-H=URI]
                         [-L=<spinCount>] [-N=<instances>] [-o=<outputFile>]
                         [-P=<port>] [-t=<tableName>] [-T=<nThreads>] [-W=<warmup>]
                         [-E=<exclude>]... [-M=<matches>]...
          --confdir=<confDir>    configuration directory
          --params=<nParameters> number of table/partition parameters
                                   Default: 0
          --sanitize             sanitize results (remove outliers)
          --savedata=<dataSaveDir>
                                 save raw data in specified dir
          --separator=<csvSeparator>
                                 CSV field separator
                                   Default:
      -C, --csv                  produce CSV output
      -d, --db=<dbName>          database name
      -E, --exclude=<exclude>    test name patterns to exclude
      -h, --help                 Show this help message and exit.
      -H, --host=URI             HMS Host
      -l, --list                 list matching benchmarks
      -L, --spin=<spinCount>     spin count
                                   Default: 100
      -M, --pattern=<matches>    test name patterns
      -N, --number=<instances>   umber of object instances
                                   Default: 100
      -o, --output=<outputFile>  output file
      -P, --port=<port>          HMS Server port
                                   Default: 9083
      -t, --table=<tableName>    table name
      -T, --threads=<nThreads>   number of concurrent threads
                                   Default: 2
      -V, --version              Print version information and exit.
      -W, --warmup=<warmup>      warmup count
                                   Default: 15

### Using single jar

    java -jar hbench-jar-with-dependencies.jar <optins> [test]...

### Using hbench on kerberized cluster

    java -jar hbench-jar-with-dependencies.jar -H `hostname` <optins> [test]...

### Examples
1. Run all tests with default settings
    java -jar hmsbench-jar-with-dependencies.jar -d `metastore_db_name` -H `hostname`

2. Run tests with 500 objects created, 10 times warm-up and exclude concurrent operations and drop operations

    java -jar hmsbench-jar-with-dependencies.jar -d `metastore_db_name` -H `hostname` -N 500 -W 10 -E 'drop.*' -E 'concurrent.*'

3. Run tests, produce output in tab-separated format and write individual data points in 'data' directory

    java -jar hmsbench-jar-with-dependencies.jar -d `metastore_db_name` -H `hostname` -o result.csv --csv --savedata data

4. Run tests on localhost
 * save raw data in directory /tmp/benchdata
 * sanitize results (remove outliers)
 * produce tab-separated file
 * use table name 'testbench'
 * create 100 parameters in partition tests
 * run with 100 and thousand partitions


       java -jar hmsbench-jar-with-dependencies.jar -H `hostname` \
            --savedata /tmp/benchdata \
            --sanitize \
            -N 100 -N 1000 \
            -o bench_results.csv -C \
            -d testbench \
            --params=100

Result:

        Operation                      Mean     Med      Min      Max      Err%
        addPartition                   16.97    16.89    13.84    27.10    8.849
        addPartitions.100              315.9    313.7    274.2    387.0    6.485
        addPartitions.1000             3016     3017     2854     3226     2.861
        concurrentPartitionAdd#2.100   1289     1289     1158     1434     4.872
        concurrentPartitionAdd#2.1000  1.221e+04 1.226e+04 1.074e+04 1.354e+04 5.077
        createTable                    18.21    18.15    14.78    24.17    10.30
        dropDatabase                   31.13    30.86    26.46    39.09    8.192
        dropDatabase.100               1436     1435     1165     1637     5.929
        dropDatabase.1000              1.376e+04 1.371e+04 1.272e+04 1.516e+04 3.864
        dropPartition                  29.43    28.81    24.79    63.24    13.97
        dropPartitions.100             686.5    680.3    575.1    819.8    6.544
        dropPartitions.1000            6247     6166     5616     7535     6.435
        dropTable                      27.53    27.34    23.23    35.35    9.241
        dropTableWithPartitions        36.41    36.19    31.33    50.41    8.310
        dropTableWithPartitions.100    793.3    792.0    687.9    987.4    7.293
        dropTableWithPartitions.1000   6981     6964     6336     9179     5.115
        getNid                         0.6760   0.6512   0.4482   1.530    21.93
        getPartition                   6.242    6.227    5.155    9.791    11.27
        getPartitionNames              4.888    4.660    3.842    13.12    22.53
        getPartitionNames.100          5.031    4.957    3.995    7.156    10.77
        getPartitionNames.1000         8.998    8.915    8.016    12.65    7.520
        getPartitions.100              9.717    9.475    7.883    13.08    9.835
        getPartitions.1000             32.60    32.03    28.30    50.02    9.036
        getPartitionsByNames           6.506    6.384    4.810    9.503    15.51
        getPartitionsByNames.100       9.312    9.025    7.955    18.44    14.46
        getPartitionsByNames.1000      38.47    37.49    34.57    62.51    10.23
        getTable                       4.092    3.868    3.132    12.20    24.56
        listDatabases                  0.6919   0.6835   0.5309   1.053    12.25
        listPartition                  5.556    5.465    4.737    7.969    10.00
        listPartitions.100             9.087    8.874    7.630    12.13    10.86
        listPartitions.1000            33.79    32.55    28.63    46.15    11.14
        listTables                     0.9851   0.9761   0.7948   1.378    12.07
        listTables.100                 1.416    1.374    1.051    3.228    16.68
        listTables.1000                4.327    4.183    3.484    6.604    14.38
        renameTable                    46.67    46.09    40.16    62.46    7.536
        renameTable.100                915.8    915.9    831.0    1022     3.833
        renameTable.1000               9015     8972     8073     1.137e+04 4.228
