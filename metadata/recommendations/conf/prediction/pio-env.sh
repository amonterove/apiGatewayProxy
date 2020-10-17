#!/usr/bin/env bash
#
# Copy this file as pio-env.sh and edit it for your site's configuration.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# PredictionIO Main Configuration
#
# This section controls core behavior of PredictionIO. It is very likely that
# you need to change these to fit your site.

# SPARK_HOME: Apache Spark is a hard dependency and must be configured.
# SPARK_HOME=$PIO_HOME/vendors/spark-1.5.1-bin-hadoop2.6
SPARK_HOME=/usr/local/spark

# POSTGRES_JDBC_DRIVER=$PIO_HOME/lib/postgresql-9.4-1204.jdbc41.jar
# MYSQL_JDBC_DRIVER=$PIO_HOME/lib/mysql-connector-java-5.1.37.jar

# ES_CONF_DIR: You must configure this if you have advanced configuration for
#              your Elasticsearch setup.
# ES_CONF_DIR=/opt/elasticsearch
ES_CONF_DIR=/usr/local/elasticsearch

# HADOOP_CONF_DIR: You must configure this if you intend to run PredictionIO
#                  with Hadoop 2.
# HADOOP_CONF_DIR=/opt/hadoop
HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop

# HBASE_CONF_DIR: You must configure this if you intend to run PredictionIO
#                 with HBase on a remote cluster.
# HBASE_CONF_DIR=$PIO_HOME/vendors/hbase-1.0.0/conf
HBASE_CONF_DIR=/usr/local/hbase/conf

# Filesystem paths where PredictionIO uses as block storage.
# PIO_FS_BASEDIR=$HOME/.pio_store
PIO_FS_BASEDIR=$HOME/prediction-data
PIO_FS_ENGINESDIR=$PIO_FS_BASEDIR/engines
PIO_FS_TMPDIR=$PIO_FS_BASEDIR/tmp

# PredictionIO Storage Configuration
#
# This section controls programs that make use of PredictionIO's built-in
# storage facilities. Default values are shown below.
#
# For more information on storage configuration please refer to
# http://predictionio.incubator.apache.org/system/anotherdatastore/

# Storage Repositories

# Default is to use PostgreSQL
PIO_STORAGE_REPOSITORIES_METADATA_NAME=pio_meta
PIO_STORAGE_REPOSITORIES_METADATA_SOURCE=ELASTICSEARCH

PIO_STORAGE_REPOSITORIES_EVENTDATA_NAME=pio_event
PIO_STORAGE_REPOSITORIES_EVENTDATA_SOURCE=HBASE

PIO_STORAGE_REPOSITORIES_MODELDATA_NAME=pio_model
PIO_STORAGE_REPOSITORIES_MODELDATA_SOURCE=HDFS

# Storage Data Sources

# PostgreSQL Default Settings
# Please change "pio" to your database name in PIO_STORAGE_SOURCES_PGSQL_URL
# Please change PIO_STORAGE_SOURCES_PGSQL_USERNAME and
# PIO_STORAGE_SOURCES_PGSQL_PASSWORD accordingly
# PIO_STORAGE_SOURCES_PGSQL_TYPE=jdbc
# PIO_STORAGE_SOURCES_PGSQL_URL=jdbc:postgresql://localhost/pio
# PIO_STORAGE_SOURCES_PGSQL_USERNAME=pio
# PIO_STORAGE_SOURCES_PGSQL_PASSWORD=pio

# MySQL Example
# PIO_STORAGE_SOURCES_MYSQL_TYPE=jdbc
# PIO_STORAGE_SOURCES_MYSQL_URL=jdbc:mysql://localhost/pio
# PIO_STORAGE_SOURCES_MYSQL_USERNAME=pio
# PIO_STORAGE_SOURCES_MYSQL_PASSWORD=pio

# Elasticsearch Example
PIO_STORAGE_SOURCES_ELASTICSEARCH_TYPE=elasticsearch
PIO_STORAGE_SOURCES_ELASTICSEARCH_CLUSTERNAME=pio-xxx.someurl.com
PIO_STORAGE_SOURCES_ELASTICSEARCH_HOSTS=elastic-xxx-01.someurl.com,elastic-xxx-02.someurl.com
PIO_STORAGE_SOURCES_ELASTICSEARCH_PORTS=9300,9300
PIO_STORAGE_SOURCES_ELASTICSEARCH_HOME=/usr/local/elasticsearch

# Local File System Example
# PIO_STORAGE_SOURCES_LOCALFS_TYPE=localfs
# PIO_STORAGE_SOURCES_LOCALFS_PATH=$PIO_FS_BASEDIR/models

PIO_STORAGE_SOURCES_HDFS_TYPE=hdfs
PIO_STORAGE_SOURCES_HDFS_PATH=hdfs://hadoop-xxx-01.someurl.com:9000/models

# HBase Example
PIO_STORAGE_SOURCES_HBASE_TYPE=hbase
PIO_STORAGE_SOURCES_HBASE_HOME=/usr/local/hbase
PIO_STORAGE_SOURCES_HBASE_HOSTS=hadoop-xxx-01.someurl.com,hadoop-xxx-02.someurl.com
PIO_STORAGE_SOURCES_HBASE_PORTS=16020,16020