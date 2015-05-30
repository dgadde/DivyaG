package com.refactorlabs.cs378.assign7;

import org.apache.avro.mapred.AvroValue;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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
public class ReduceSideJoin extends Configured implements Tool{

	/**
	 * Each count output from the map() function is "1", so to minimize small
	 * object creation we can use a constant for this output value/object.
	 */
	public final static LongWritable ONE = new LongWritable(1L);

	/**
	 * The Map class for word count.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word count example.
	 */
	public static class MapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<VinImpressionCounts>> {

		/**
		 * Counter group for the mapper.  Individual counters are grouped for the mapper.
		 */
		private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";
        private HashMap<String, VinImpressionCounts> map=new HashMap<String,VinImpressionCounts>();
		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		@Override
		public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
				throws IOException, InterruptedException {
            Session session=value.datum();
			context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
            //System.out.println("In Mapper1");
            map.clear();
            //System.out.println("In Mapper2");
            VinImpressionCounts vimps;

            //System.out.println("User id: "+key.toString());
            //System.out.println("Event size: "+session.getEvents().size());
            for(Event e:session.getEvents()){
                String vin=e.getVin().toString();
                //System.out.println("vid: "+vin);
                if(map.containsKey(vin)){
                    vimps=map.get(vin);
                }else {
                    HashMap<CharSequence, Long> clicks=new HashMap<CharSequence,Long>();
                    vimps=new VinImpressionCounts(0L,clicks,0L,0L,0L);
                    map.put(vin,vimps);
                }
            vimps.setUniqueUser(1L);
            vimps.setUniqueUserVdpView(0L);

                if(e.getEventType().toString().equals("SHARE") && e.getEventSubtype().toString().equals("MARKET_REPORT")){
                    System.out.println("event MARKET_REPORT");
                    vimps.setShareMarketReport(1L);
            }else if(e.getEventType().toString().equals("SUBMIT") && e.getEventSubtype().toString().equals("CONTACT_FORM")){
                    System.out.println("event CONTACT_FORM");
                    vimps.setSubmitContactForm(1L);
            }else if(e.getEventType().toString().equals("CLICK")){
                    System.out.println("event  Click: "+e.getEventSubtype().toString());
                    vimps.getClicks().put(e.getEventSubtype().toString(),1L);
            }

            }
            //System.out.println("In Mapper4");
            System.out.println("map size: "+map.size());
            for(Entry<String,VinImpressionCounts> v:map.entrySet()){
                //System.out.println("Outputting vinmps+"+v.getValue().getUniqueUser());
                context.write(new Text(v.getKey()),new AvroValue<VinImpressionCounts>(v.getValue()));
            }



        }
	}

   public static class MapClass2 extends Mapper<LongWritable, Text, Text, AvroValue<VinImpressionCounts>> {


        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

        private HashMap<CharSequence, Long> clicks=new HashMap<CharSequence,Long>();

        private VinImpressionCounts vimps=new VinImpressionCounts(0L,clicks,0L,0L,0L);
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
            String[] tokens=value.toString().split(",");
            if(tokens[2].equals("count")){
                return;
            }
            System.out.println("2In Mapper2, size of input: "+tokens.length);

            System.out.println("PArsing: "+ tokens[2]);
            long count=Long.parseLong(tokens[2].trim());
            vimps.setUniqueUserVdpView(count);
            context.write(new Text(tokens[0]),new AvroValue<VinImpressionCounts>(vimps));
        }
    }

	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word count example.
	 */
	public static class ReduceClass extends Reducer<Text, AvroValue<VinImpressionCounts>, AvroKey<Pair<CharSequence, VinImpressionCounts>>, NullWritable> {

		/**
		 * Counter group for the reducer.  Individual counters are grouped for the reducer.
		 */
		private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";
        private long unique_users=0;
        private long share_market_report;
        private long submit_contact_form;
        private long unique_user_vdp_view;
        private long[] clicksum;
        //0-ALTERNATIVE, 1-CONTACT_BANNER, 2-CONTACT_BUTTON, 3-DEALER_PHONE, 4-FEATURES, 5-GET_DIRECTIONS,
        // 6-BADGES, 7-TEST_DRIVE_LINK, 8-VEHICLE_HISTORY

		@Override
		public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0L;
            //System.out.println("In Reducer1");

            context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);
            VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();

            unique_users=0;
            share_market_report=0;
            submit_contact_form=0;
            unique_user_vdp_view=0;
            clicksum=new long[9];

            // Sum up the counts for the current word, specified in object "key".
            for (AvroValue<VinImpressionCounts> value : values) {
                VinImpressionCounts vin=value.datum();
                if(vin.getUniqueUser()==0){
                    unique_user_vdp_view+=vin.getUniqueUserVdpView();
                    continue;
                }

                Map<CharSequence, Long> clicks=vin.getClicks();
                for(Entry<CharSequence, Long> e:clicks.entrySet()){

                    if(e.getKey().equals("ALTERNATIVE")){
                        clicksum[0]+=e.getValue();
                    }else if(e.getKey().equals("CONTACT_BANNER")){
                        clicksum[1]+=e.getValue();
                    }else if(e.getKey().equals("CONTACT_BUTTON")){
                        clicksum[2]+=e.getValue();
                    }else if(e.getKey().equals("DEALER_PHONE")){
                        clicksum[3]+=e.getValue();
                    }else if(e.getKey().equals("FEATURES")){
                        clicksum[4]+=e.getValue();
                    }else if(e.getKey().equals("GET_DIRECTIONS")){
                        clicksum[5]+=e.getValue();
                    }else if(e.getKey().equals("BADGES")){
                        clicksum[6]+=e.getValue();
                    }else if(e.getKey().equals("TEST_DRIVE_LINK")){
                        clicksum[7]+=e.getValue();
                    }else if(e.getKey().equals("VEHICLE_HISTORY")){
                        clicksum[8]+=e.getValue();
                    }
                }
                unique_users+=vin.getUniqueUser();
                share_market_report+=vin.getShareMarketReport();
                submit_contact_form+=vin.getSubmitContactForm();

                  }
            if(unique_users!=0) {

                HashMap<CharSequence, Long> clicks = new HashMap<CharSequence, Long>();
                clicks.put("ALTERNATIVE", clicksum[0]);
                clicks.put("CONTACT_BANNER", clicksum[1]);
                clicks.put("CONTACT_BUTTON", clicksum[2]);
                clicks.put("DEALER_PHONE", clicksum[3]);
                clicks.put("FEATURES", clicksum[4]);
                clicks.put("GET_DIRECTIONS", clicksum[5]);
                clicks.put("BADGES", clicksum[6]);
                clicks.put("TEST_DRIVE_LINK", clicksum[7]);
                clicks.put("VEHICLE_HISTORY", clicksum[8]);
                System.out.println("share_market_report: "+share_market_report);
                System.out.println("submit_contact_form: "+submit_contact_form);
                System.out.println("FEATURES: "+clicksum[4]);
                builder.setUniqueUser(unique_users);
                builder.setShareMarketReport(share_market_report);
                builder.setSubmitContactForm(submit_contact_form);
                builder.setUniqueUserVdpView(unique_user_vdp_view);
                builder.setClicks(clicks);


                // Emit the total count for the word.
                context.write(
                        new AvroKey<Pair<CharSequence, VinImpressionCounts>>
                                (new Pair<CharSequence, VinImpressionCounts>(key.toString(), builder.build())),
                        NullWritable.get());
            }
			}
	}

	/**
	 * The main method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ReduceSideJoin <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = new Job(conf, "ReduceSideJoin");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String[] inputPaths = appArgs[0].split(",");

        MultipleInputs.addInputPath(job, new Path(inputPaths[0]),AvroKeyValueInputFormat.class,MapClass.class);
        MultipleInputs.addInputPath(job, new Path(inputPaths[1]),TextInputFormat.class,MapClass2.class);
        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(ReduceSideJoin.class);
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Map

        //job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job,Session.getClassSchema());
        //job.setMapperClass(MapClass.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(job,
                Pair.getPairSchema(Schema.create(Schema.Type.STRING), VinImpressionCounts.getClassSchema()));
        job.setOutputValueClass(NullWritable.class);
        // Grab the input file and output directory from the command line.
        /*for ( String inputPath : inputPaths ) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }*/
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
        int res = ToolRunner.run(new Configuration(), new ReduceSideJoin(), args);
        System.exit(res);
    }
}
