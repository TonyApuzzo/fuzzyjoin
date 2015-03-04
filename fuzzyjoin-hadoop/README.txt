                                README
                                ======

Date: 2011-04-12 09:58:13 PDT


Author: Rares Vernica <rares (at) ics.uci.edu>

Table of Contents
=================
1 Copyright 
2 Overview 
3 Quick Start 
    3.1 Build 
    3.2 Self-join 
        3.2.1 Upload raw data 
        3.2.2 Generate records 
        3.2.3 Balance records across nodes 
        3.2.4 Run set-similarity self-join 
    3.3 R-S join 
        3.3.1 Upload raw data 
        3.3.2 Generate records 
        3.3.3 Balance records across nodes 
        3.3.4 Run set-similarity join 
4 Configuration 
5 Directory Structure and Tasks 
6 Dataset 
7 Source Code Overview 


1 Copyright 
~~~~~~~~~~~~
Copyright 2010-2011 The Regents of the University of California

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License.  You
may obtain a copy of the License at

     [http://www.apache.org/licenses/LICENSE-2.0]

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS"; BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied.  See the License for the specific language governing
permissions and limitations under the License.

2 Overview 
~~~~~~~~~~~
This guide describes how to use the source code developed for the study in:


  Efficient Parallel Set-Similarity Joins Using MapReduce.
  Rares Vernica, Michael J. Carey, Chen Li
  SIGMOD 2010

3 Quick Start 
~~~~~~~~~~~~~~
The only requirement for running the code is a Hadoop cluster. It does
not have to be a full-fledged cluster, a single-node
pseudo-distributed installation of Hadoop is enough. For more details
about starting a Hadoop cluster please see
[http://hadoop.apache.org/common/docs/current/quickstart.html] The code
works with Hadoop version 0.17 or higher.

3.1 Build 
==========

  $ cd fuzzyjoin-hadoop
  fuzzyjoin-hadoop$ ant

3.2 Self-join 
==============
Here are the steps to perform a self-join on a small sample of the
DBLP dataset. We use 100 DBLP entries, title and authors as the join
attributes, Jaccard similarity and a 0.5 similarity threshold.

3.2.1 Upload raw data 
----------------------

  fuzzyjoin-hadoop$ hadoop fs -put \
    ../data/dblp-small/raw-000 dblp-small/raw-000

The file =dblp-small.raw.txt= contains one record per line. On each
line the fields are separated by "=:=" and contain DBLP id,
publication title, authors (concatenated with " ") and other
information available about the publication (concatenated with " ").

3.2.2 Generate records 
-----------------------

  fuzzyjoin-hadoop$ hadoop jar target/fuzzyjoin-hadoop-0.0.2.jar \
    recordbuild -conf src/main/resources/fuzzyjoin/dblp.quickstart.xml

This job assigns unique record-IDs to each record. The RIDs are
integers and are appended in front of each record. After this job,
each record contains five fields: RID, DBLP id, title, authors, other
information.

3.2.3 Balance records across nodes 
-----------------------------------

  fuzzyjoin-hadoop$ hadoop jar target/fuzzyjoin-hadoop-0.0.2.jar \
    recordbalance -conf src/main/resources/fuzzyjoin/dblp.quickstart.xml

To skip this step, run:


  fuzzyjoin-hadoop$ hadoop fs -mv \
    dblp-small/recordsbulk-000 dblp-small/records-000

3.2.4 Run set-similarity self-join 
-----------------------------------

  fuzzyjoin-hadoop$ hadoop jar target/fuzzyjoin-hadoop-0.0.2.jar \
    fuzzyjoin -conf src/main/resources/fuzzyjoin/dblp.quickstart.xml

This will run the three stages required to do fuzzy joins: token
ordering (Tokens), kernel (RIDPairs), and record join
(RecordPairs). It will use the basic alternative for each stage. In
total it will run five Hadoop jobs (TokensBasic.phase1,
TokenBasic.phase2, RIDPairsImproved, RecordPairsBasic.phase1,
RecordPairsBasic.phase2).

Each stage can be run separately using different alternatives by
replacing =fuzzyjoin= in the above command with the name of the stage
and the alternative. For example, to run the one-phase token ordering
(TokensImproved), type:


  fuzzyjoin-hadoop$ hadoop jar target/fuzzyjoin-hadoop-0.0.2.jar \
    tokensimproved -conf src/main/resources/fuzzyjoin/dblp.quickstart.xml

To get the list with all the available stages and alternatives, type:


  fuzzyjoin-hadoop$ hadoop jar target/fuzzyjoin-hadoop-0.0.2.jar

To see the results, type:


  fuzzyjoin-hadoop$ hadoop fs -cat "dblp-small/recordpairs-000/part-*"

Each line contains a pair of records that fuzzy join and their
similarity. The format of the line is =record 1;threshold;record2=,
where =record1= and =record2= have the same format as described in
step 3.

3.3 R-S join 
=============
Here are the steps to perform a join between a small sample of the
DBLP dataset and a small sample of the CITESEERX dataset. We use 100
DBLP entries and 100 CITESEERX entries, title and authors as the join
attributes, Jaccard similarity and a 0.5 similarity threshold.

3.3.1 Upload raw data 
----------------------

  fuzzyjoin-hadoop$ hadoop fs -put \
    ../data/pub-small/raw.dblp-000 pub-small/raw.dblp-000
  fuzzyjoin-hadoop$ hadoop fs -put \
    ../data/pub-small/raw.csx-000 pub-small/raw.csx-000

The =raw= directory contains two files, one for each dataset.

3.3.2 Generate records 
-----------------------

  fuzzyjoin-hadoop$ hadoop jar target/fuzzyjoin-hadoop-0.0.2.jar \
    recordbuild -conf src/main/resources/fuzzyjoin/pub.quickstart.xml \
    -Dfuzzyjoin.data.suffix.input=dblp
  fuzzyjoin-hadoop$ hadoop jar target/fuzzyjoin-hadoop-0.0.2.jar \
    recordbuild -conf src/main/resources/fuzzyjoin/pub.quickstart.xml \
    -Dfuzzyjoin.data.suffix.input=csx

      Each job generates records for one of the datasets.

3.3.3 Balance records across nodes 
-----------------------------------

  fuzzyjoin-hadoop$ hadoop jar target/fuzzyjoin-hadoop-0.0.2.jar \
    recordbalance -conf src/main/resources/fuzzyjoin/pub.quickstart.xml \
    -Dfuzzyjoin.data.suffix.input=dblp
  fuzzyjoin-hadoop$ hadoop jar target/fuzzyjoin-hadoop-0.0.2.jar \
    recordbalance -conf src/main/resources/fuzzyjoin/pub.quickstart.xml \
    -Dfuzzyjoin.data.suffix.input=csx

To skip this step, run:


  fuzzyjoin-hadoop$ hadoop fs -mv \
    pub-small/recordsbulk.dblp-000 pub-small/records.dblp-000
  fuzzyjoin-hadoop$ hadoop fs -mv \
    pub-small/recordsbulk.csx-000 pub-small/records.csx-000

3.3.4 Run set-similarity join 
------------------------------

  fuzzyjoin-hadoop$ hadoop jar target/fuzzyjoin-hadoop-0.0.2.jar \
    fuzzyjoin -conf src/main/resources/fuzzyjoin/pub.quickstart.xml

To see the results, type:


  fuzzyjoin-hadoop$ hadoop fs -cat "pub-small/recordpairs-000/part-*"

Each line contains a pair of records that fuzzy join and their
similarity. The format of the line is
=record-DBLP;threshold;record-CITESEERX=, where =record-DBLP= and
=record-CITESEERX= have the same format as described in the self-join
case.

4 Configuration 
~~~~~~~~~~~~~~~~
The XML files provided with the =-conf= argument above contain various
configuration parameters. Using the configuration parameters, a user
can specify the location of the data, the similarity function and
threshold, the join attributes and other settings. Moreover the user
can specify additional parameters in the command line using the =-D=
option.

The default parameters and more details about each parameter are in:


  fuzzyjoin-hadoop/src/main/resources/fuzzyjoin/default.xml

All these parameters and other constants are defined in:


  fuzzyjoin-core/src/main/java/edu/uci/ics/fuzzyjoin/FuzzyJoinConfig.java
  fuzzyjoin-hadoop/src/main/java/edu/uci/ics/fuzzyjoin/hadoop/FuzzyJoinDriver.java

5 Directory Structure and Tasks 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The following directory structure is used for self-joins:


   
  |- raw-000
  |- recordsbulk-000
  |- recordsbulk-001
  |- ...
  |- records-000
  |- records-001
  |- ...
  |- tokens-000
  |- ...
  |- tokens.phase1-000
  |- ...
  |- ridpairs-000
  |- ...
  |- recordpairs-000
  |- ...
  |- recordpairs.phase1-000

The =raw-000= directory contains the original files, one record per
line. The =recordsbulk= directory contains the original data where
each record starts with an integer RID. The number after the directory
name represents the copy number (=000= is the original data, =001= is
the first copy, etc.). The =records= directory contains the same data
as the =recordsbulk= directory except that multiple copies are
aggregated and the data is balanced across nodes. The number after the
directory name represents how many copies are aggregated (=000= is of
only one copy: =recordsbulk-000=, =001= is for two copies:
=recordsbulk-000= and =recordsbulk-001=, etc.). So =records-n=
represents an increased dataset, where =n= denotes how many times the
dataset was increased. For the rest of the directories the number
after the directory name has the same meaning. The =tokens= directory
contains the list of tokens. The =ridpairs= directory contains the RID
pairs that fuzzy-join. The =recordpairs= directory contains the record
pairs that fuzzy-join. The =phase1= prefix that appears for some
directories represents the output of the first MapReduce job for the
tasks with two MapReduce jobs (i.e., =tokensbasic= and
=recordpairsbasic=).

Bellow is a table with each task input and output directories:

  Task                         Input                         Output       
 ----------------------------+-----------------------------+-------------
  recordbuild                  raw                           recordsbulk  
  recordbalance                recordsbulk                   records      
  tokens basic/improved        records                       tokens       
  ridpairs improved/ppjoin     records, tokens               ridpairs     
  recordpairs basic/improved   records, ridpairs             recordpairs  
  recordgenerate               recordsbulk-000, tokens-000   recordsbulk  

For R-S joins, the first few directories also carry the name of the
dataset (name of the R dataset or of the S dataset) in order to
differentiate between them:


   
  |- raw.DATASET_R-000
  |- raw.DATASET_S-000
  |- recordsbulk.DATASET_R-000
  |- recordsbulk.DATASET_R-001
  |- ...
  |- recordsbulk.DATASET_S-000
  |- recordsbulk.DATASET_S-001
  |- ...
  |- records.DATASET_R-000
  |- records.DATASET_R-001
  |- ...
  |- records.DATASET_S-000
  |- records.DATASET_S-001

where =DATASET_R= and =DATASET_S= are the names of the two
datasets. In our R-S join example we used =dblp= for =DATASET_R= and
=csx= for =DATASET_S=.

6 Dataset 
~~~~~~~~~~
By default the dataset is assumed to have one record per line. The
fields of each record are delimited by "=:=". The first filed of each
record is an integer RID. This settings can be changed in:


  fuzzyjoin-core/src/main/java/edu/uci/ics/fuzzyjoin/FuzzyJoinConfig.java

The dataset can be increased using the =recordgenerate= task:


  fuzzyjoin-hadoop$ hadoop jar target/fuzzyjoin-hadoop-0.0.2.jar \
    recordgenerate -conf src/main/resources/fuzzyjoin/dblp.quickstart.xml \
    -Dfuzzyjoin.data.copy=10 \
    -Dfuzzyjoin.data.norecords=100

This stats =9= MapReduce jobs, each of them generating a new copy of
the dataset. The =fuzzyjoin.data.copy= parameter specifies the number
of times the dataset should be increased, while the
=fuzzyjoin.data.norecords= parameter specifies the number of records
in the *original* dataset (it is used to generate unique and
increasing RIDs). All the following tasks also need to have the same
value for the =fuzzyjoin.data.copy= parameter in order to use the
increased dataset. This task can only be ran after running
=recordbuild= and =tokensbasic= or =tokensimproved= on the original
dataset. After this task, the =recordbuild= task needs to be ran (it
cannot be skipped on the increased dataset):


  fuzzyjoin-hadoop$ hadoop jar target/fuzzyjoin-hadoop-0.0.2.jar \
    recordbalance -conf src/main/resources/fuzzyjoin/dblp.quickstart.xml \
    -Dfuzzyjoin.data.copy=10
  fuzzyjoin-hadoop$ hadoop jar target/fuzzyjoin-hadoop-0.0.2.jar \
    fuzzyjoin -conf src/main/resources/fuzzyjoin/dblp.quickstart.xml \
    -Dfuzzyjoin.data.copy=10

7 Source Code Overview 
~~~~~~~~~~~~~~~~~~~~~~~
The source code is divided into two modules:
- =fuzzyjoin-core=: general fuzzy-join code in
  =fuzzyjoin-core/src/main/java=
  - =edu.uci.ics.fuzzyjoin=: main memory fuzzy-join
  - =edu.uci.ics.fuzzyjoin.similarity=: similarity functions and
    filters
  - =edu.uci.ics.fuzzyjoin.invertedlist=: inverted lists index
  - =edu.uci.ics.fuzzyjoin.recordgroup=: alternatives for grouping records
  - =edu.uci.ics.fuzzyjoin.tokenizer=: tokenizes
  - =edu.uci.ics.fuzzyjoin.tokenorder=: alternatives for ordering tokens
- =fuzzyjoin-hadoop=: Hadoop specific fuzzy-join code in
  =fuzzyjoin-hadoop/src/main/java=
  - =edu.uci.ics.fuzzyjoin.hadoop=: main program
  - =edu.uci.ics.fuzzyjoin.hadoop.datagen=: classes for building
    records and increasing dataset size
  - =edu.uci.ics.fuzzyjoin.hadoop.recordpairs=: Stage 3
  - =edu.uci.ics.fuzzyjoin.hadoop.ridpairs=: Stage 2
  - =edu.uci.ics.fuzzyjoin.hadoop.ridrecordpairs=: alternative to
    Stage 2 and 3 where records are not projected
  - =edu.uci.ics.fuzzyjoin.hadoop.tokens=: Stage 1
