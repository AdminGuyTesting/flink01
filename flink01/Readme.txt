--------------------------------
--------------------------------
20241216
--------------------------------
Basic flink Job in functions
No user interface for now
Reading of avro files in java 11. Had to use old libraries and copy these libraries
to the /opt/flink/lib folder on task and jobmanager

--------------------------------
Without the additional libraries the job won't run with error Missing Class
--------------------------------

-rw-r--r-- 1 root  root     224277 Dec 11 13:00 ant-contrib-1.0b3.jar
-rw-r--r-- 1 root  root      36455 Dec 11 13:00 ant-eclipse-1.0-jvm1.2.jar
-rwxr-xr-x 1 root  root     593627 Dec 13 14:42 avro-1.11.0.jar
-rw-r--r-- 1 root  root     263268 Dec 11 13:00 avro-1.5.3.jar
-rwxr-xr-x 1 root  root      72776 Dec 13 14:42 avro-compiler-1.6.3.jar
-rw-r--r-- 1 root  root     168042 Dec 11 13:00 avro-ipc-1.5.3.jar
-rw-r--r-- 1 root  root      89086 Dec 11 13:00 avro-mapred-1.5.3.jar
-rwxr-xr-x 1 root  root      27479 Dec 13 14:42 avro-maven-plugin-1.8.2.jar
-rw-r--r-- 1 root  root     109043 Dec 11 13:00 commons-io-1.4.jar
-rw-r--r-- 1 flink flink    198363 Jul 25 09:26 flink-cep-1.20.0.jar
-rw-r--r-- 1 flink flink    563068 Jul 25 09:30 flink-connector-files-1.20.0.jar
-rw-r--r-- 1 flink flink    102373 Jul 25 09:34 flink-csv-1.20.0.jar
-rw-r--r-- 1 flink flink 126353861 Jul 25 09:42 flink-dist-1.20.0.jar
-rw-r--r-- 1 flink flink    203640 Jul 25 09:34 flink-json-1.20.0.jar
-rw-r--r-- 1 flink flink  21060638 Jul 25 09:40 flink-scala_2.12-1.20.0.jar
-rw-r--r-- 1 root  root    6740707 Dec  9 13:35 flink-sql-parquet-1.18.1.jar
-rw-r--r-- 1 flink flink  15709269 Jul 25 09:41 flink-table-api-java-uber-1.20.0.jar
-rw-r--r-- 1 flink flink  38423593 Jul 25 09:40 flink-table-planner-loader-1.20.0.jar
-rw-r--r-- 1 flink flink   3540887 Jul 25 09:26 flink-table-runtime-1.20.0.jar
-rw-r--r-- 1 root  root     706710 Dec 11 13:00 hsqldb-1.8.0.10.jar
-rwxr-xr-x 1 root  root      78488 Dec 13 16:01 jackson-annotations-2.18.1.jar
-rwxr-xr-x 1 root  root     581927 Dec 13 16:01 jackson-core-2.17.2.jar
-rw-r--r-- 1 root  root     207430 Dec 11 13:00 jackson-core-asl-1.7.3.jar
-rwxr-xr-x 1 root  root    1649385 Dec 13 16:01 jackson-databind-2.17.1.jar
-rw-r--r-- 1 root  root     625229 Dec 11 13:00 jackson-mapper-asl-1.7.3.jar
-rw-r--r-- 1 root  root      53244 Dec 11 13:00 jopt-simple-3.2.jar
-rw-r--r-- 1 flink flink    208006 Jan 14  2022 log4j-1.2-api-2.17.1.jar
-rw-r--r-- 1 flink flink    301872 Jan 14  2022 log4j-api-2.17.1.jar
-rw-r--r-- 1 flink flink   1790452 Jan 14  2022 log4j-core-2.17.1.jar
-rw-r--r-- 1 flink flink     24279 Jan 14  2022 log4j-slf4j-impl-2.17.1.jar
-rw-r--r-- 1 root  root      29555 Dec 11 13:00 paranamer-2.3.jar
-rw-r--r-- 1 root  root     995720 Dec 11 13:00 snappy-java-1.0.3.2.jar


--------------------------------
--------------------------------
--------------------------------
--------------------------------
