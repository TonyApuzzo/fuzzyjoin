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

source ./conf.sh

CASES="1/b 1/i 2/i 2/p 3/b 3/i"
UTIL=$PWD
PLOT=$UTIL/../plot
AVG="python $UTIL/average.py"

current_path=`pwd`
basic_only="1/b 3/b"
non_basic_only="1/i 2/i 2/p 3/i"

## -----------------------------------------------------
## AVERAGE
## -----------------------------------------------------

## -----------------------------------------------------
## average runtimes
## -----------------------------------------------------
function avg() {
    echo "-- - Compute Averages - --"
    EXPERIMENT=$1
    DATASET=$2

    pushd . >> $LOG
    cd $EXP_DIR/$DATASET/$EXPERIMENT
    for c in $CASES
    do
        pushd . >> $LOG
        cd $c
        echo "-- $c --" >> $LOG
        rm avg.txt 2>> $LOG
        if [ "$EXPERIMENT" = "perf" ]
        then
            l="5 10 25"
        else
            l="2 4 8 10"
        fi
        for exp in `echo $l`
        do
            if [ "$EXPERIMENT" = "perf" ]
            then
                echo $exp `$AVG $exp.txt` >> avg.txt
            else
                exp_zeroed=`printf %03d $exp`            
                echo $exp `$AVG $exp_zeroed.txt` >> avg.txt
            fi
        done
        cat avg.txt >> $LOG
        popd >> $LOG
    done
    popd >> $LOG
}

## -----------------------------------------------------
## combine average runtimes
## -----------------------------------------------------
function combine() {
    echo "-- - Combine Averages - --"
    EXPERIMENT=$1
    DATASET=$2

    pushd . >> $LOG
    cd $EXP_DIR/$DATASET/$EXPERIMENT
    mkdir bib bpb bpi 2>> $LOG
    for c in bib bpb bpi
    do
        rm $c/avg.txt 2>> $LOG
        if [ "$EXPERIMENT" = "perf" ]
        then
            n=3
        else
            n=4
        fi
        for l in `seq $n`
        do
            echo -n "$c $l:" >> $LOG
            t=0
            line_new=""
            for i in `seq 0 2`
            do
                echo -n "${c:$i:1} " >> $LOG
                s=`expr $i + 1`
                line=`head -$l $s/${c:$i:1}/avg.txt | tail -1`

                if [ "$i" -eq "0" ]
                then
                    k=`echo $line | cut --delimiter=" " --fields=1`
                fi

                v=`echo $line | cut --delimiter=" " --fields=2`
                t=`echo $t + $v | bc`
                line_new="$line_new $v"
            done
            echo >> $LOG
            line_new="$k $t$line_new"
            echo $line_new >> $c/avg.txt
        done
    done
    popd >> $LOG
}

## -----------------------------------------------------
## network communication
## -----------------------------------------------------
function net() {
    echo "-- - Compute Network Communication - --"
    EXPERIMENT=$1
    DATASET=$2

    EXP_DIR=../../exps/asterix
    p=$PWD/..

    outf="net.txt"
    pushd . >> $LOG
    cd $EXP_DIR/$DATASET/$EXPERIMENT
    for c in $CASES
    do
        pushd . >> $LOG
        cd $c
        echo "-- $c --" >> $LOG
        rm $outf 2>> $LOG
        if [ "$EXPERIMENT" = "perf" ]
        then
            l="5 10 25"
        else
            l="2 4 8 10"
        fi
        for exp in `echo $l`
        do
            echo $PWD $exp
            expf=$exp
            if [ "$EXPERIMENT" != "perf" ]
            then
                expf=`printf %03d $exp`            
            fi
            f=`head -1 ../../2/i/${expf}_log.txt`
            echo $f
            din=`grep "Map input bytes" $p/$f | cut --delimiter="=" --fields=2`
            echo $din
            f=`head -1 ${expf}_log.txt`
            echo $f
            dsout=`grep "Map output bytes" $p/$f | cut --delimiter="=" --fields=2`
            t=0
            for dout in $dsout
            do
                echo $dout
                t=$(($t + $dout))
            done
            echo $t
            t=`echo $t / $din | bc -l`
            echo $t
            echo $exp $t >> $outf
        done
        cat $outf >> $LOG
        popd >> $LOG
    done
    popd >> $LOG
}

## -----------------------------------------------------
## PLOT
## -----------------------------------------------------

## -----------------------------------------------------
## plot performance
## -----------------------------------------------------
function plot_perf() {
    echo "-- - Plot Figure - --"
    DATASET=$1

    pushd . >> $LOG
    cd $EXP_DIR/$DATASET/perf
    if [ "$DATASET" = "dblp" ]
    then
        python $PLOT/bars.py --ymax=900 --ygrid=200 --format="%d %f %f %f %f" bib,bpb,bpi /avg.txt
    else
        python $PLOT/bars.py --ymax=2400 --ygrid=400 --format="%d %f %f %f %f" bib,bpb,bpi /avg.txt
    fi
    popd >> $LOG
}

## -----------------------------------------------------
## plot speedup
## -----------------------------------------------------
function plot_speedup() {
    echo "-- - Plot Figure - --"
    DATASET=$1

    pushd . >> $LOG
    cd $EXP_DIR/$DATASET/speedup

    cd 1
    o="--labels=BTO,OPTO"
    # python $PLOT/speedup.py $o --ygrid=40 b,i /avg.txt
    python $PLOT/speeduprel.py $o b,i /avg.txt >> $LOG
    cd ..

    cd 2
    o="--labels=BK,PK"
    if [ "$DATASET" = "dblp" ]
    then
        # python $PLOT/speedup.py $o --ymax=800 --ygrid=200 i,p /avg.txt
        python $PLOT/speeduprel.py $o i,p /avg.txt >> $LOG
    else
        # python $PLOT/speedup.py $o --ymax=2000 --ygrid=400 i,p /avg.txt
        python $PLOT/speeduprel.py $o --ymax=15 i,p /avg.txt >> $LOG
    fi
    cd ..
 
    cd 3
    o="--labels=BRJ,OPRJ"
    if [ "$DATASET" = "dblp" ]
    then
        # python $PLOT/speedup.py $o --ymax=300 --ygrid=50 b,i /avg.txt
        python $PLOT/speeduprel.py $o --labels="BRJ,OPRJ" b,i /avg.txt >> $LOG
    else
        # python $PLOT/speedup.py $o --ymax=1800 --ygrid=200 b,i /avg.txt
        python $PLOT/speeduprel.py $o --ymax=15 b,i /avg.txt >> $LOG
    fi
    cd ..

    f="%d %f %f %f %f"
    o="--labels=BTO-BK-BRJ,BTO-PK-BRJ,BTO-PK-OPRJ"
    if [ "$DATASET" = "dblp" ]
    then
        # python $PLOT/speedup.py --legend=40,70 --ymax=1300 --ygrid=200 --format="$f" $o bib,bpb,bpi /avg.txt
        python $PLOT/speeduprel.py --format="$f" $o bib,bpb,bpi /avg.txt >> $LOG
    else
        # python $PLOT/speedup.py --legend=40,70 --ymax=4000 --ygrid=1000 --format="$f" $o bib,bpb,bpi /avg.txt
        python $PLOT/speeduprel.py --format="$f" $o --ymax=5 bib,bpb,bpi /avg.txt >> $LOG
    fi

    popd >> $LOG
}

## -----------------------------------------------------
## plot scaleup
## -----------------------------------------------------
function plot_scaleup() {
    echo "-- - Plot Figure - --"
    DATASET=$1

    pushd . >> $LOG
    cd $EXP_DIR/$DATASET/scaleup
    l="--legend=75,10"

    cd 1
    o="--labels=BTO,OPTO"
    if [ "$DATASET" = "dblp" ]
    then
        python $PLOT/scaleup.py $o $l --ymax=160 --ygrid=40 b,i /avg.txt 
        # python $PLOT/scaleuprel.py b,i /avg.txt >> $LOG
    fi
    cd ..

    cd 2
    o="--labels=BK,PK"
    if [ "$DATASET" = "dblp" ]
    then
        python $PLOT/scaleup.py $o $l --ymax=600 --ygrid=100 i,p /avg.txt
        # python $PLOT/scaleuprel.py i,p /avg.txt >> $LOG
    else
        python $PLOT/scaleup.py $o $l --ymax=1400 --ygrid=200 i,p /avg.txt
        # python $PLOT/scaleuprel.py i,p /avg.txt >> $LOG
    fi
    cd ..

    cd 3
    o="--labels=BRJ,OPRJ"
    if [ "$DATASET" = "dblp" ]
    then
        python $PLOT/scaleup.py $o $l --ymax=200 --ygrid=50 b,i /avg.txt
        # python $PLOT/scaleuprel.py b,i /avg.txt >> $LOG
    else
        python $PLOT/scaleup.py $o $l --ymax=1000 --ygrid=200 b,i /avg.txt
    fi
    cd ..

    f="%d %f %f %f %f"
    o="--labels=BTO-BK-BRJ,BTO-PK-BRJ,BTO-PK-OPRJ"
    if [ "$DATASET" = "dblp" ]
    then
        python $PLOT/scaleup.py --ymax=1000 --ygrid=200 --format="$f" $o --legend=40,10 --ideal=0 bib,bpb,bpi /avg.txt
#         python $PLOT/scaleuprel.py --format="$f" $o bib,bpb,bpi /avg.txt >> $LOG
    else
        python $PLOT/scaleup.py --ymax=3500 --ygrid=500 --format="$f" $o bib,bpb,bpi /avg.txt
    fi

    popd >> $LOG
}

## -----------------------------------------------------
## plot network communication
## -----------------------------------------------------
function plot_net() {
    echo "-- - Plot Figure - --"
    EXPERIMENT=$1
    DATASET=$2

#     EXP_DIR=../../exps/asterix

    pushd . >> $LOG
    cd $EXP_DIR/$DATASET/$EXPERIMENT

#     cd 1
#     python $PLOT/net.py --labels="BTO,OPTO" --ymin=0 --ymax=1.4 --ygrid=.2 b,i /net.txt
#     cd ..

#     cd 2
#     python $PLOT/net.py --labels="BK,PK" --ymin=0 --ymax=1.4 --ygrid=.2 i,p /net.txt
#     cd ..

#     cd 3
#     python $PLOT/net.py --labels="BRJ,OPRJ" --ymin=0 --ymax=1.4 --ygrid=.2 b,i /net.txt
#     cd ..

    python $PLOT/net.py --labels="1-BTO,1-OPTO,2-BK,2-PK,3-BRJ,3-OPRJ" --exp=$EXPERIMENT --ymin=0 --ymax=1.4 --ygrid=.2 1/b,1/i,2/i,2/p,3/b,3/i /net.txt

    popd >> $LOG
}

## -----------------------------------------------------

function avg_across() {
    echo "-- - avg across $@ - --"
    args=( $@ )
    current_path="${EXP_DIR}${args[0]}"
    cd $current_path
    f=avg_${args[1]}
    rm $f
    for dir in $( find -L . -type d -name "[0-9]*" -not -name "." -not -path "*.svn*" -printf "%f\n"  | sort --numeric-sort )
    do
        echo $dir `average.py $dir/${args[1]}` >> $f
    done
    cat $f
}

function job_time() {
    echo job_time

    for path in $basic_only
    do
        echo $path
        cd $path

        for exp in 2 4 10
        do            
            echo "    $exp"
            exp_zeroed=`printf %03d $exp`
            
            for job in 1 2
            do 
                echo "        $job"
                cat ${exp_zeroed}_log.txt | jobtime.sh $job > ${exp_zeroed}_job$job.txt
                cat ${exp_zeroed}_job$job.txt
            done
        done

        cd $current_path
    done
}

function job_avg() {
    echo job_avg

    for path in $basic_only
    do
        echo $path
        cd $path

        for job in 1 2
        do 
            echo "    $job"
            rm avg_job$job.txt

            for exp in 2 4 10
            do
                echo "        $exp"
                exp_zeroed=`printf %03d $exp`
            
                echo $exp `average.py ${exp_zeroed}_job$job.txt` >> avg_job$job.txt
            done
            cat avg_job$job.txt
        done
        
        cd $current_path
    done
}

function plot_job_avg_speedup() {
    echo plot_job_avg_speedup

    for path in $basic_only
    do
        echo $path
        cd $path

#         speedup.py avg_job1,avg_job2 .txt
        speeduprel.py avg_job1,avg_job2 .txt

        cd $current_path
    done
}

function plot_job_avg_scaleup() {
    echo plot_job_avg_scaleup

    for path in $basic_only
    do
        echo $path
        cd $path

        scaleup.py avg_job1,avg_job2 .txt
        scaleuprel.py avg_job1,avg_job2 .txt

        cd $current_path
    done
}

function job_log() {
    echo "-- - job_log $@ - --"
    args=( $@ )
    current_path="${EXP_DIR}${args[0]}"
    cd $current_path
    for exp in `echo ${args[1]} | sed "s/,/ /g"`
    do
        if [ -z ${args[2]} ]
        then
            echo $exp
            exp_zeroed=`printf %03d $exp`            
            joblog.sh ${exp_zeroed}_log.txt 3
        else
            for job in `seq ${args[2]}`
            do 
                echo "$exp $job"            
                exp_zeroed=`printf %03d $exp`            
                joblog.sh ${exp_zeroed}_log.txt 3 $job
            done    
        fi
    done
    ls *.log
}

function job_part() {
    echo job_part

    for path in $basic_only
    do
        echo $path
        cd $path

        for job in 1 2
        do 
            echo "    $job"

            rm avg_job${job}_part.txt

            for exp in 2 4 10
            do
                echo "        $exp"
                exp_zeroed=`printf %03d $exp`                
                echo $exp `cat ${exp_zeroed}_job$job.log | job.py s avg` >> avg_job${job}_part.txt
                cat ${exp_zeroed}_job$job.log | job.py s w
            done
        done
        
        cd $current_path
    done

    for path in $non_basic_only
    do
        echo $path
        cd $path

        rm avg_job_part.txt

        for exp in 2 4 10
        do
            echo "    $exp"
            exp_zeroed=`printf %03d $exp`
            echo $exp `cat ${exp_zeroed}_job.log | job.py s avg` >> avg_job_part.txt
            cat ${exp_zeroed}_job$job.log | job.py s w
        done

        cd $current_path
    done

}

function plot_job_part_speedup() {
    echo plot_job_part_speedup

    for path in $basic_only
    do
        echo $path
        cd $path

        for job in 1 2
        do 
            echo "    $job"
            speeduprel.py --format="%d %f %f %f %f %f %f" --labels=Start,Map,Shuffle,Merge,Reduce,Finish --mask=011110 --ymin=0 . /avg_job${job}_part.txt
        done
        
        cd $current_path
    done

    for path in $non_basic_only
    do
        echo $path
        cd $path
        speeduprel.py --format="%d %f %f %f %f %f %f" --labels=Start,Map,Shuffle,Merge,Reduce,Finish --mask=011110 --ymin=0 . /avg_job${job}_part.txt
        cd $current_path
    done

}

function plot_job_part_scaleup() {
    echo plot_job_part_scaleup

    for path in $basic_only
    do
        echo $path
        cd $path
        for job in 1 2
        do 
            echo "    $job"
        done
        scaleuprel.py --format="%d %f %f %f %f %f %f" --labels=Start,Map,Shuffle,Merge,Reduce,Finish --mask=011110 --ymin=0 . /avg_job${job}_part.txt
        cd $current_path
    done

    for path in $non_basic_only
    do
        echo $path
        cd $path
        scaleuprel.py --format="%d %f %f %f %f %f %f" --labels=Start,Map,Shuffle,Merge,Reduce,Finish --mask=011110 --ymin=0 . /avg_job${job}_part.txt
        cd $current_path
    done

}

function print() {
    echo print

    lpr 1/b_i-avg.eps
    lpr 1/rel-b_i-avg.eps

    lpr 2/i_p-avg.eps
    lpr 2/rel-i_p-avg.eps

    lpr 3/b_i-avg.eps
    lpr 3/rel-b_i-avg.eps

    lpr bib_bpb_bpi-avg.eps
    lpr rel-bib_bpb_bpi-avg.eps
}

# plot_perf dblp
# plot_speedup dblp
# plot_scaleup dblp

# job_time
# job_avg
# plot_job_avg_speedup
# plot_job_avg_scaleup

# plot_job_part_speedup
# plot_job_part_scaleup

# print
