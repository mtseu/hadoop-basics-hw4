import com.epam.training.hw4.MRSorter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

/**
 * Created by miket on 4/4/16.
 */
public class MRSorterTest {
    MapReduceDriver<Object, Text, MRSorter.IdTStampComKey, MRSorter.SomeVal, MRSorter.IdTStampComKey, MRSorter.SomeVal> mrD;

    @Before
    public void setUp() throws Exception {
        mrD = MapReduceDriver.newMapReduceDriver(new MRSorter.Mapper(), new MRSorter.Reducer());
        mrD.setKeyGroupingComparator(new MRSorter.GroupByIdComparator());
        mrD.setKeyOrderComparator(new MRSorter.SortComparator());
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void mapperTest() throws Exception {
        mrD
        .withInput(new LongWritable(),
            new Text(
                "whatever\t20130606\tIPinUID1\twhatever\twhatever\twhatever\twhatever\t1\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\t0"))
        .withInput(new LongWritable(),
            new Text(
                "whatever\t20130607\tIPinUID2\twhatever\twhatever\twhatever\twhatever\t1\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\t1"))
        .withInput(new LongWritable(),
            new Text(
                "whatever\t20130605\tIPinUID2\twhatever\twhatever\twhatever\twhatever\t1\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\t1"))
        .withInput(new LongWritable(),
            new Text(
                "whatever\t20130606\tIPinUID2\twhatever\twhatever\twhatever\twhatever\t1\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\t1"))
        .withInput(new LongWritable(),
            new Text(
                "whatever\t20130605\tIPinUID1\twhatever\twhatever\twhatever\twhatever\t1\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\twhatever\t1"))
        .withOutput(new MRSorter.IdTStampComKey(new Text("IPinUID1"), new Text("20130605")), new MRSorter.SomeVal(new Text("1")))
        .withOutput(new MRSorter.IdTStampComKey(new Text("IPinUID1"), new Text("20130606")), new MRSorter.SomeVal(new Text("0")))
        .withOutput(new MRSorter.IdTStampComKey(new Text("IPinUID2"), new Text("20130605")), new MRSorter.SomeVal(new Text("1")))
        .withOutput(new MRSorter.IdTStampComKey(new Text("IPinUID2"), new Text("20130606")), new MRSorter.SomeVal(new Text("1")))
        .withOutput(new MRSorter.IdTStampComKey(new Text("IPinUID2"), new Text("20130607")), new MRSorter.SomeVal(new Text("1")))
        .runTest(true);

        Assert.assertEquals(
                "There shoud be a single counter",
                1, mrD.getCounters().getGroup("I_PIN_YOU_ID_MAX_SITE_IMPRESSION").size());
        Assert.assertEquals(
                "The counter should correspond to IPinUID of max",
                3L, mrD.getCounters().getGroup("I_PIN_YOU_ID_MAX_SITE_IMPRESSION").findCounter("IPinUID2").getValue());
    }

    @Test
    public void reducerTest() throws Exception {
//        rD.withInput(
//                new Text("127.0.0.1"),
//                new ArrayList<Agg.Aggs>() {{
//                    add(new Agg.Aggs(new Text("127.0.0.1"), new IntWritable(1), new IntWritable(100)));
//                    add(new Agg.Aggs(new Text("127.0.0.1"), new IntWritable(1), new IntWritable(500)));
//                }}
//        ).withOutput(
//                new Text("127.0.0.1"),
//                new Agg.Aggs(new Text("127.0.0.1"), new IntWritable(2), new IntWritable(600))
//        ).runTest();
    }

}
