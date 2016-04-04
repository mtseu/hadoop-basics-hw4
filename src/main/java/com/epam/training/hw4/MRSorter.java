package com.epam.training.hw4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

/**
 * Created by miket on 4/4/16.
 */
public class MRSorter extends Configured implements Tool {
    public static class IdTStampComKey implements WritableComparable<IdTStampComKey> {
        private Text id;
        private Text tstamp;

        public IdTStampComKey() {
            id = new Text();
            tstamp = new Text();
        }

        public IdTStampComKey(Text id, Text tstamp) {
            this.id = id;
            this.tstamp = tstamp;
        }

        public Text getId() {
            return id;
        }

        public void setId(Text id) {
            this.id = id;
        }

        public Text getTstamp() {
            return tstamp;
        }

        public void setTstamp(Text tstamp) {
            this.tstamp = tstamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IdTStampComKey that = (IdTStampComKey) o;

            if (!id.equals(that.id)) return false;
            return tstamp.equals(that.tstamp);

        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + tstamp.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return (new StringBuilder(id.toString()).append("\t").append(tstamp)).toString();
        }

        @Override
        public int compareTo(IdTStampComKey other) {
            int res = id.compareTo(other.getId());
            if(res == 0) {
                res = tstamp.compareTo(other.getTstamp());
            }

            return res;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            id.write(out);
            tstamp.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            id.readFields(in);
            tstamp.readFields(in);
        }
    }

    public static class SomeVal implements Writable {
        Text streamId;

        public SomeVal() {
            super();
            this.streamId = new Text();
        }

        public SomeVal(Text streamId) {
            this.streamId = streamId;
        }

        public Text getStreamId() {
            return streamId;
        }

        public void setStreamId(Text streamId) {
            this.streamId = streamId;
        }

        @Override
        public String toString() {
            return "StreamID:" + streamId.toString();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            streamId.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            streamId.readFields(in);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SomeVal someVal = (SomeVal) o;

            return streamId.equals(someVal.streamId);

        }

        @Override
        public int hashCode() {
            return streamId.hashCode();
        }
    }

    public static class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<IdTStampComKey, SomeVal> {
        HashPartitioner<Text, SomeVal> helper = new HashPartitioner<>();
        // I saw this in examples (why is it needed? have no idea)
        Text idKey = new Text(); // will calc part-n based on this and relax GC by reusing this obj.

        @Override
        public int getPartition(IdTStampComKey idTStampComKey, SomeVal someVal, int numPartitions) {
            idKey.set(idTStampComKey.getId());
            return helper.getPartition(idKey, someVal, numPartitions);
        }
    }

    public static class GroupByIdComparator extends WritableComparator {
        public GroupByIdComparator() {
            super(IdTStampComKey.class, true);
        }

        @Override
        public int compare(WritableComparable first, WritableComparable second) {
            return ((IdTStampComKey)first).getId().compareTo(((IdTStampComKey)second).getId());
        }
    }

    public static class SortComparator extends WritableComparator {
        public SortComparator() {
            super(IdTStampComKey.class, true);
        }

        @Override
        public int compare(WritableComparable first, WritableComparable second) {
            IdTStampComKey theFirst = (IdTStampComKey)first;
            IdTStampComKey theSecond = (IdTStampComKey)second;

            int res = theFirst.getId().compareTo(theSecond.getId());
            if(res == 0) {
                res = theFirst.getTstamp().compareTo(theSecond.getTstamp());
            }

            return res;
        }
    }

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, IdTStampComKey, SomeVal>
    {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");

            context.write(
                    new IdTStampComKey(new Text(fields[2]), new Text(fields[1])),
                    new SomeVal(new Text(fields[21])));
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<IdTStampComKey, SomeVal, IdTStampComKey, SomeVal>
    {
        private final static Text SITE_IMPRESSION_STREAM_ID = new Text("1");
        private final static Text NULL = new Text("null");

        private Text maxId;
        private long maxVal;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            maxVal = 0;
            maxId = new Text();
        }

        @Override
        protected void reduce(IdTStampComKey key, Iterable<SomeVal> values, Context context) throws IOException, InterruptedException {
            long counter = 0;
            for(SomeVal val: values) {
                if(!NULL.equals(key.getId())) {
                    if (SITE_IMPRESSION_STREAM_ID.compareTo((val.getStreamId())) == 0) {
                        counter++;
                    }
                }

                // do not reduce anything - just output sorted rows
                context.write(key, val);
            }

            if(counter > maxVal) {
                maxId.set(key.getId());
                maxVal = counter;
            }


        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            context.getCounter("I_PIN_YOU_ID_MAX_SITE_IMPRESSION", maxId.toString()).setValue(maxVal);
        }
    }


    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "HW4");

        job.setJarByClass(MRSorter.class);

        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);

        job.setOutputKeyClass(IdTStampComKey.class);
        job.setOutputValueClass(SomeVal.class);

        job.setPartitionerClass(Partitioner.class);
        job.setGroupingComparatorClass(GroupByIdComparator.class);
        job.setSortComparatorClass(SortComparator.class);

        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // job.getCounters().addGroup("I_PIN_YOU_ID_MAX_SITE_IMPRESSION", "HW4 Counters Group");

        boolean res = job.waitForCompletion(true);

        if(res) {
            printMax(job);
        } else {
            System.out.println("Job Failed");
        }

        return res?0:1;
    }

    private void printMax(Job job) throws Exception {
        Iterator<Counter> maxCounters = job.getCounters().getGroup("I_PIN_YOU_ID_MAX_SITE_IMPRESSION").iterator();
        long maxCount = 0;
        String iPinUId = "";
        int countOfCounters = 0;

        while (maxCounters.hasNext()) {
            Counter maxCnt = maxCounters.next();
            if(maxCnt.getValue() > maxCount) {
                maxCount = maxCnt.getValue();
                iPinUId = maxCnt.getName();
            }
            countOfCounters++;
        }

        System.out.println("The max Site Impressions=" + maxCount + " for IPinYouId=" + iPinUId
                + " out of " + countOfCounters + " counters checked.");
    }


    public static void main(String[] args) {
        int res = 0;
        try {
            System.exit(ToolRunner.run(new Configuration(), new MRSorter(), args));
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
