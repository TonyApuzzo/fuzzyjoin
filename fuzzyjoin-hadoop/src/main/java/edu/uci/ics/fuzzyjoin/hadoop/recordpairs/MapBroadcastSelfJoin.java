/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 * 
 * Author: Rares Vernica <rares (at) ics.uci.edu>
 */

package edu.uci.ics.fuzzyjoin.hadoop.recordpairs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.RIDPairSimilarity;
import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;
import edu.uci.ics.fuzzyjoin.hadoop.IntPairWritable;

/**
 * @author rares
 * 
 *         KEY1: Unused
 * 
 *         VALUE1: Record
 * 
 *         KEY2: RID1, RID2
 * 
 *         VALUE2: Record; Similarity
 */
public class MapBroadcastSelfJoin extends MapReduceBase implements
        Mapper<Object, Text, IntPairWritable, Text> {

    private static final Log LOG = LogFactory
            .getLog(MapBroadcastSelfJoin.class);

    private final HashMap<Integer, ArrayList<RIDPairSimilarity>> joinIndex = new HashMap<Integer, ArrayList<RIDPairSimilarity>>();

    private final IntPairWritable key = new IntPairWritable();

    private void addJoinIndex(Path path) {
        try {
            BufferedReader fis = new BufferedReader(new FileReader(
                    path.toString()));
            String line = null;
            while ((line = fis.readLine()) != null) {
                String[] splits = line.split(" ");
                int rid1 = Integer.parseInt(splits[0]);
                int rid2 = Integer.parseInt(splits[1]);
                float similarity = Float.parseFloat(splits[2]);

                if (rid1 < rid2) {
                    int r = rid1;
                    rid1 = rid2;
                    rid2 = r;
                }

                RIDPairSimilarity ridSimilarity = new RIDPairSimilarity(rid1,
                        rid2, similarity);

                // Use ArrayList to lower the memory requirements. This causes
                // duplicated to be outputed. The duplicates need to be
                // eliminated in the Reduce.
                ArrayList<RIDPairSimilarity> ridSimilarities = joinIndex
                        .get(rid1);
                if (ridSimilarities != null) {
                    ridSimilarities.add(ridSimilarity);
                } else {
                    ridSimilarities = new ArrayList<RIDPairSimilarity>();
                    ridSimilarities.add(ridSimilarity);
                    joinIndex.put(rid1, ridSimilarities);
                }

                ridSimilarities = joinIndex.get(rid2);
                if (ridSimilarities != null) {
                    ridSimilarities.add(ridSimilarity);
                } else {
                    ridSimilarities = new ArrayList<RIDPairSimilarity>();
                    ridSimilarities.add(ridSimilarity);
                    joinIndex.put(rid2, ridSimilarities);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void configure(JobConf job) {
        LOG.info("Configure START");
        //
        // read join index
        //
        Path[] cache = null;
        try {
            cache = DistributedCache.getLocalCacheFiles(job);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (cache == null) {
            addJoinIndex(new Path(
                    job.get(FuzzyJoinDriver.DATA_JOININDEX_PROPERTY)));
        } else {
            for (Path path : cache) {
                addJoinIndex(path);
            }
        }
        LOG.info("Configure END");
    }

    public void map(Object unused, Text record,
            OutputCollector<IntPairWritable, Text> output, Reporter reporter)
            throws IOException {
        String valueSplit[] = record.toString().split(
                FuzzyJoinConfig.RECORD_SEPARATOR_REGEX);
        int rid = Integer.valueOf(valueSplit[FuzzyJoinConfig.RECORD_KEY]);
        String recordStr = record.toString();
        ArrayList<RIDPairSimilarity> set = joinIndex.get(rid);
        if (set != null) {
            for (RIDPairSimilarity ridSimilarity : set) {
                key.set(ridSimilarity.rid1, ridSimilarity.rid2);
                record.set("" + ridSimilarity.similarity
                        + FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR + recordStr);
                output.collect(key, record);
            }
        }
    }
}
