package com.refactorlabs.cs378.assign8;

import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.Map.*;

/**
 * Example MapReduce program that performs word count.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class FilteringOutputs extends Configured implements Tool{

    /**
     * Each count output from the map() function is "1", so to minimize small
     * object creation we can use a constant for this output value/object.
     */
    public final static LongWritable ONE = new LongWritable(1L);

    /**
     * The Map class for word count.  Extends class Mapper, provided by Hadoop.
     * This class defines the map() function for the word count example.
     */
    public static class MapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";
        public enum SessionCounters {
            Submitter,
            Sharer,
            Clicker,
            Shower,
            Visitor,
            Other
        }
        /**
         * Local variable "word" will contain the word identified in the input.
         * The Hadoop Text object is mutable, so we can reuse the same object and
         * simply reset its value as each word in the input is encountered.
         */
        private AvroMultipleOutputs mos;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            mos = new AvroMultipleOutputs(context);
        }
        @Override
        public void cleanup(Context context) throws IOException,
                InterruptedException {
            mos.close();

        }
        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {
            Session session=value.datum();
            context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
            //System.out.println("In Mapper1");
            boolean CHANGE = false, CONTACT_FORM_STATUS = false, EDIT = false, SUBMIT = false, SHARE = false, CLICK = false, SHOW = false, VISIT = false;

            for(Event e:session.getEvents()){
                //System.out.println("vid: "+vin);
                if(e.getEventType().toString().equals("CHANGE")){
                    CHANGE=true;
                }else if(e.getEventType().toString().equals("CONTACT_FORM_STATUS")){
                    CONTACT_FORM_STATUS=true;
                }else if(e.getEventType().toString().equals("EDIT")){
                    EDIT=true;
                }else if(e.getEventType().toString().equals("SUBMIT")){
                    SUBMIT=true;
                }else if(e.getEventType().toString().equals("SHARE")){
                    SHARE=true;
                }else if(e.getEventType().toString().equals("CLICK")){
                    CLICK=true;
                }else if(e.getEventType().toString().equals("SHOW")){
                    SHOW=true;
                }else if(e.getEventType().toString().equals("VISIT")){
                    VISIT=true;
                }
            }


            if(CHANGE || CONTACT_FORM_STATUS || EDIT||SUBMIT){
                mos.write("submitter", new AvroKey<CharSequence>(key.toString()), new AvroValue<Session>(value.datum()), "submitter");
                context.getCounter(SessionCounters.Submitter).increment(1L);
            }else if(SHARE){
                context.getCounter(SessionCounters.Sharer).increment(1L);
                mos.write("sharer", new AvroKey<CharSequence>(key.toString()), new AvroValue<Session>(value.datum()), "sharer");
            }else if(CLICK){
                context.getCounter(SessionCounters.Clicker).increment(1L);
                mos.write("clicker", new AvroKey<CharSequence>(key.toString()), new AvroValue<Session>(value.datum()), "clicker");
            }else if(SHOW){
                context.getCounter(SessionCounters.Shower).increment(1L);
                mos.write("shower", new AvroKey<CharSequence>(key.toString()), new AvroValue<Session>(value.datum()), "shower");
            }else if(VISIT){
                context.getCounter(SessionCounters.Visitor).increment(1L);
                mos.write("visitor", new AvroKey<CharSequence>(key.toString()), new AvroValue<Session>(value.datum()), "visitor");
            } else {
                context.getCounter(SessionCounters.Other).increment(1L);
                mos.write("other", new AvroKey<CharSequence>(key.toString()), new AvroValue<Session>(value.datum()), "other");
            }


        }
    }




    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: FilteringOutputs <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = new Job(conf, "FilteringOutputs");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(FilteringOutputs.class);
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Map
        AvroMultipleOutputs.addNamedOutput(job, "submitter", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING),Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, "sharer", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING),Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, "clicker", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING),Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, "shower", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING),Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, "visitor", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING),Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, "other", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING),Session.getClassSchema());


        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job,Session.getClassSchema());
        //job.setMapperClass(MapClass.class);

        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job,Session.getClassSchema());
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);


        // Specify the Reduce
        job.setNumReduceTasks(0);
        String[] inputPaths = appArgs[0].split(",");

        // Grab the input file and output directory from the command line.
        for ( String inputPath : inputPaths ) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);
        System.out.println("End of run");

        return 0;
    }

    private static void printClassPath() {
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        URL[] urls = ((URLClassLoader) cl).getURLs();
        System.out.println("classpath BEGIN");
        for (URL url : urls) {
            System.out.println(url.getFile());
        }
        System.out.println("classpath END");
    }

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        printClassPath();
        int res = ToolRunner.run(new Configuration(), new FilteringOutputs(), args);
        System.exit(res);
    }
}
