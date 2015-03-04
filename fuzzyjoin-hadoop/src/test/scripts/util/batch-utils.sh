#!/bin/bash
#
# Copyright 2010-2011 The Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS"; BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.
# 
# Author: Rares Vernica <rares (at) ics.uci.edu>

## -----------------------------------------------------
## Main Functions
## -----------------------------------------------------

## -----------------------------------------------------
## init
## -----------------------------------------------------
init() {
    echo "-- - Init Hadoop - --"
    progress_init 5

    progress "Stop all"
    if [ ! -e $H_CONF/masters ]
    then
        linkConf masters-000/masters-010 masters
    fi
    if [ ! -e $H_CONF/slaves ]
    then
        linkConf slaves-000/slaves-010 slaves
    fi
    $H_BIN/stop-all.sh >> $LOG; sleep 5

    progress "Update slaves"
    linkConf slaves-000/slaves-$SLAVES_0 slaves

    progress "Copy config"
    updateConf

    progress "Format HDFS"
    echo | $PSSH $HDFS_FORMAT >> $LOG
    echo "Y" | $H namenode -format &>> $LOG

    progress "Start all"
    $H_BIN/start-dfs.sh >> $LOG
    $H_BIN/start-mapred.sh >> $LOG
    waitToStart $SLAVES

    progress
}

## -----------------------------------------------------
## Upload to HDFS
## -----------------------------------------------------
upload() {
    echo "-- - Upload to HDFS - --"
    progress_init 2

    progress "Create directories"
    d=$HDFS_DATA_DIR
    $H fs -rmr $d &>> $LOG
    $H fs -mkdir $d
    $H fs -mkdir $d/raw-000

    progress "Upload raw data"
    $H fs -copyFromLocal $LOCAL_DATA_RAW_DBLP $d/raw-000/
    if [ "$DATASET" = "pub" ]
    then
        $H fs -copyFromLocal $LOCAL_DATA_RAW_CITESEERX $d/raw-000/
    fi
        
    progress
}

## -----------------------------------------------------
## Prepare Data
## -----------------------------------------------------
prepare() {
    echo "-- - Prepare Data - --"
    i=5
    if [ "$DATASET" = "pub" ]
    then
        i=10
    fi
    progress_init $i

    j="dblp"
    if [ "$DATASET" = "pub" ]
    then
        JB_RUN="-Dfuzzyjoin.data.suffix.input=$j"
    fi
    progress "Generate x1 $j"
    runJob /dummyjob recordbuild 1

    progress "Balance x1 $j"
    runJob /dummyjob recordbalance 1

    progress "Tokens x1 $j"
    runJob /dummyjob tokensimproved 1

    JB_RUN="$JB_RUN -Dfuzzyjoin.data.copy=$SIZE"

    progress "Generate x$SIZE $j"
    runJob /dummyjob recordgenerate 1

    i=`expr $SLAVES \* $CORE_MAX`
    JB_RUN="$JB_RUN -Dfuzzyjoin.data.norecords= -Dmapred.map.tasks=$i -Dmapred.reduce.tasks=$i"

    progress "Balance x$SIZE $j"
    runJob /dummyjob recordbalance 1

    if [ "$DATASET" = "pub" ]
    then
        j="csx"
        JB_RUN="-Dfuzzyjoin.data.suffix.input=$j"

        progress "Generate x1 $j"
        runJob /dummyjob recordbuild 1

        progress "Balance x1 $j"
        runJob /dummyjob recordbalance 1

        progress "Tokens x1 $j"
        runJob /dummyjob tokensimproved 1
        
        JB_RUN="$JB_RUN -Dfuzzyjoin.data.copy=$SIZE"

        progress "Generate x$SIZE $j"
        runJob /dummyjob recordgenerate 1

        i=`expr $SLAVES \* $CORE_MAX`
        JB_RUN="$JB_RUN -Dfuzzyjoin.data.norecords= -Dmapred.map.tasks=$i -Dmapred.reduce.tasks=$i"
        
        progress "Balance x$SIZE $j"
        runJob /dummyjob recordbalance 1
    fi

    progress
}

## -----------------------------------------------------
## Run
## -----------------------------------------------------
run() {
    echo "-- - Run Experiments - --"
    progress_init 6

    i=`expr $SLAVES \* $CORE_MAX`
    JB_RUN="-Dfuzzyjoin.data.copy=$SIZE -Dmapred.map.tasks=$i -Dmapred.reduce.tasks=$i"

    ###
    ### Split Size
    ###
    ### Speedup
    # case $SLAVES in
    #      2) M_SZ_TOKEN=436027279 ;; # speedup,dblp,10,2
    #      4) M_SZ_TOKEN=218013639 ;; # speedup,dblp,10,4
    #      8) M_SZ_TOKEN=109006819 ;; # speedup,dblp,10,8,
    #     10) M_SZ_TOKEN=87205455  ;; # perf/speedup,dblp,10,10
    # esac
    ### Scaleup
    # M_SZ_TOKEN=218013639 # perf,dblp,25,10; scaleup,dblp,5-25,2-10
    ### Perf 
    # M_SZ_TOKEN=43602727 # perf,dblp,5,10
    # M_SZ_TOKEN=174410910
    # JB_RUN="$JB_RUN -Dmapred.min.split.size=$M_SZ_TOKEN"
    M_SZ=9223372036854775807        # Long.MAX_VALUE
    JB_RUN="$JB_RUN -Dmapred.min.split.size=$M_SZ"

    n=$SLAVES_0
    if [ "$EXPERIMENT" = "perf" ]
    then
        n=$SIZE
    fi
    e=$EXPERIMENT

    progress "Stage 1 Basic"
    runJob /$e/1/b/$n tokensbasic

    progress "Stage 1 Improved"
    runJob /$e/1/i/$n tokensimproved

    progress "Stage 2 Improved"
    runJob /$e/2/i/$n ridpairsimproved

    progress "Stage 2 PPJoin"
    runJob /$e/2/p/$n ridpairsppjoin

    progress "Stage 3 Basic"
    runJob /$e/3/b/$n recordpairsbasic

    progress "Stage 3 Improved"
    runJob /$e/3/i/$n recordpairsimproved

    ### End-to-end
    # runJob /$e/bib/$n fuzzyjoin
    # JB_RUN="$JB_RUN -Dfuzzyjoin.version=bpb"
    # runJob /$e/bpb/$n fuzzyjoin
    # JB_RUN="$JB_RUN -Dfuzzyjoin.version=bpi"
    # runJob /$e/bpi/$n fuzzyjoin

    progress
}
## -----------------------------------------------------
## Secondary Functions
## -----------------------------------------------------

## -----------------------------------------------------
## Link Configuration File
## -----------------------------------------------------
linkConf() {
   args=( $@ )

   pushd . > /dev/null
   cd $H_CONF
   rm ${args[1]} >> $LOG
   ln -s ${args[0]} ${args[1]}
   popd > /dev/null
}

## -----------------------------------------------------
## Link and Copy Configuration File
## -----------------------------------------------------
linkAndCopyConf() {
   args=( $@ )

   linkConf ${args[0]} ${args[1]}
   copyConf ${args[1]}
}

## -----------------------------------------------------
## Copy Configuration File
## -----------------------------------------------------
copyConf() {
   args=( $@ )

   file=$H_CONF/${args[0]}
   $PSCP $file $file >> $LOG
}

## -----------------------------------------------------
## Update Configuration Files
## -----------------------------------------------------
updateConf() {
   linkAndCopyConf hadoop-env-batch.sh hadoop-env.sh
   linkAndCopyConf core-site-batch.xml core-site.xml
   linkAndCopyConf hdfs-site-batch.xml hdfs-site.xml
   linkAndCopyConf mapred-site-batch.xml mapred-site.xml
   copyConf masters
   copyConf slaves
}

## -----------------------------------------------------
## Progress Init
## -----------------------------------------------------
progress_init() {
    progress_steps=$1
    progress_step=0
}

## -----------------------------------------------------
## Progress
## -----------------------------------------------------
progress() {
    p=`printf "%*s" $progress_step ""`
    p=${p// /#}
    p=`printf " [%*s]" -$progress_steps $p`
    m=$@
    if [ -n "$m" ]
    then
        m=$m...
    fi
    m=`printf "%-40s" "$m"`
    echo -ne "$p $m\r"

    if [ "$progress_steps" -eq "$progress_step" ]
    then
        echo
    fi

    (( progress_step += 1 ))
}

## -----------------------------------------------------
## Wait for HDFS to Start
## -----------------------------------------------------
waitToStart() {
   args=( $@ )

   is_not_started=1
   while [ "$is_not_started" -eq 1 ]
   do
       sleep 5
       $H dfsadmin -report | grep "Datanodes available: $SLAVES" >> $LOG
       is_not_started=$?
   done
}

## -----------------------------------------------------
## Run Job
## runJob(
##     path,
##     tokensbasic|
##     tokensimproved|
##     ridpairsimproved|
##     ridpairsppjoin|
##     recordpairsbasic|
##     recordpairsimproved)
## -----------------------------------------------------
runJob() {
   args=( $@ )

   mkdir -p $LOG_DIR

   exp=$EXP_DIR/$DATASET/${args[0]}
   fexp=$exp.txt
   fexp_log=${exp}_log.txt
   mkdir -p `expr match "$exp" '\(.*/\)'`
   rm $fexp $fexp_log &>> $LOG

   echo $exp >> $LOG
   echo $JB_RUN >> $LOG

   trails="1 2 3"
   if [ -n "${args[2]}" ]
   then
       trails=${args[2]}
   fi

   for trail in $trails
   do

      flog=$LOG_DIR/`date $DATE_FORMAT`.txt
      echo "Log file: $flog" >> $LOG

      $H jar $JAR ${args[1]} $JB $JB_RUN >& $flog

      t=`tail -1 $flog | cut --delimiter=" " --fields=4`
      if [ -z $t ]
      then
          echo 
          echo "Experiment Failed. Log saved in $flog."
          exit 1
      fi
      echo $t >> $fexp
      echo $flog >> $fexp_log

   done # trail
}

## -----------------------------------------------------
