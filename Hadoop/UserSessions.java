package com.refactorlabs.cs378.assign8;

import org.apache.avro.mapred.AvroValue;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

/**
 * Example MapReduce program that performs word count.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class UserSessions extends Configured implements Tool{

	/**
	 * Each count output from the map() function is "1", so to minimize small
	 * object creation we can use a constant for this output value/object.
	 */
	public final static LongWritable ONE = new LongWritable(1L);

	/**
	 * The Map class for word count.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word count example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, AvroValue<Event>> {

		/**
		 * Counter group for the mapper.  Individual counters are grouped for the mapper.
		 */
		private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\t");

			context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

			// For each word in the input line, emit a count of 1 for that word.
            //1:user_id	2:event_type	3:page	4:referrer	5:referring_domain	6:event_timestamp	7:city	8:region
            // 9:vin	10:vehicle_condition	11:year	make	12:model
            // 13:trim	14:body_style	15:subtrim	16:cab_style	17:initial_price	18:mileage	19:mpg
            // 20:exterior_color	21:interior_color	22:engine_displacement	23:engine	24:transmission	25:drive_type	26:fuel	27:image_count	28:initial_carfax_free_report
            // 29:carfax_one_owner	30:initial_cpo	31features
            //assert tokens.length==30;
            //System.out.println("tokens.length:"+tokens.length);
            word.set(tokens[0]);
            Event.Builder builder = Event.newBuilder();
            String[] eventTypes=tokens[1].split(" ");
            if(eventTypes[0].equals("change")){
                builder.setEventType(EventType.CHANGE);
                if(eventTypes[1].equals("contact")){
                    builder.setEventSubtype(EventSubtype.CONTACT_FORM);
                }

            }
            else if(eventTypes[0].equals("click")){
                builder.setEventType(EventType.CLICK);
                if(eventTypes[1].equals("alternative")){
                    builder.setEventSubtype(EventSubtype.ALTERNATIVE);
                }
                else if(eventTypes[1].equals("contact") & eventTypes[2].equals("banner")){
                    builder.setEventSubtype(EventSubtype.CONTACT_BANNER);
                }
                else if(eventTypes[1].equals("contact")& eventTypes[2].equals("button")){
                    builder.setEventSubtype(EventSubtype.CONTACT_BUTTON);
                }
                else if(eventTypes[1].equals("dealer")){
                    builder.setEventSubtype(EventSubtype.DEALER_PHONE);
                }
                else if(eventTypes[1].equals("features")){
                    builder.setEventSubtype(EventSubtype.FEATURES);
                }
                else if(eventTypes[1].equals("get")){
                    builder.setEventSubtype(EventSubtype.GET_DIRECTIONS);
                }
                else if(eventTypes[1].equals("show")){
                    builder.setEventSubtype(EventSubtype.BADGES);
                }
                else if(eventTypes[1].equals("test")){
                    builder.setEventSubtype(EventSubtype.TEST_DRIVE_LINK);
                }
                else if(eventTypes[1].equals("vehicle")){
                    builder.setEventSubtype(EventSubtype.VEHICLE_HISTORY);
                }
            }
            else if(eventTypes[0].equals("contact")){
                builder.setEventType(EventType.CONTACT_FORM_STATUS);
                if(eventTypes[2].equals("success")){
                    builder.setEventSubtype(EventSubtype.SUCCESS);
                }
                else {
                    builder.setEventSubtype(EventSubtype.ERROR);
                }
            }
            else if(eventTypes[0].equals("edit")){
                builder.setEventType(EventType.EDIT);
                if(eventTypes[1].equals("contact")){
                    builder.setEventSubtype(EventSubtype.CONTACT_FORM);
                }
            }
            else if(eventTypes[0].equals("share")){
                builder.setEventType(EventType.SHARE);
                if(eventTypes[1].equals("market")){
                    builder.setEventSubtype(EventSubtype.MARKET_REPORT);
                }
            }
            else if(eventTypes[0].equals("show")){
                builder.setEventType(EventType.SHOW);
                if(eventTypes[1].equals("badge")){
                    builder.setEventSubtype(EventSubtype.BADGE_DETAIL);
                }
                else if(eventTypes[1].equals("photo")){
                    builder.setEventSubtype(EventSubtype.PHOTO_MODAL);
                }
            }
            else if(eventTypes[0].equals("submit")){
                builder.setEventType(EventType.SUBMIT);
                if(eventTypes[1].equals("contact")){
                    builder.setEventSubtype(EventSubtype.CONTACT_FORM);
                }
            }
            else if(eventTypes[0].equals("visit") || eventTypes[0].equals("visit_market_report_listing")){
                builder.setEventType(EventType.VISIT);
                if(eventTypes.length==1){
                    builder.setEventSubtype(EventSubtype.MARKET_REPORT);
                }
                else if(eventTypes[1].equals("alternatives")){
                    builder.setEventSubtype(EventSubtype.ALTERNATIVE);
                }
                else if(eventTypes[1].equals("badges")){
                    builder.setEventSubtype(EventSubtype.BADGES);
                }
                else if(eventTypes[1].equals("contact")){
                    builder.setEventSubtype(EventSubtype.CONTACT_FORM);
                }
                else if(eventTypes[1].equals("features")){
                    builder.setEventSubtype(EventSubtype.FEATURES);
                }
                else if(eventTypes[1].equals("vehicle")){
                    builder.setEventSubtype(EventSubtype.VEHICLE_HISTORY);
                }

            }
            builder.setPage(tokens[2]);
            builder.setReferrer(tokens[3]);
            builder.setReferringDomain(tokens[4]);
            builder.setEventTime(tokens[5]);
            builder.setCity(tokens[6]);
            builder.setRegion(tokens[7]);
            builder.setVin(tokens[8]);
            builder.setVehicleCondition(tokens[9]);
            builder.setYear(tokens[10]);
            builder.setMake(tokens[11]);
            builder.setModel(tokens[12]);
            builder.setTrim(tokens[13]);
            builder.setBodyStyle(tokens[14]);
            builder.setSubtrim(tokens[15]);
            builder.setCabStyle(tokens[16]);
            builder.setInitialPrice(tokens[17]);
            builder.setMileage(tokens[18]);
            builder.setMpg(tokens[19]);
            builder.setExteriorColor(tokens[20]);
            builder.setInteriorColor(tokens[21]);
            builder.setEngineDisplacement(tokens[22]);
            builder.setEngine(tokens[23]);
            builder.setTransmission(tokens[24]);
            builder.setDriveType(tokens[25]);
            builder.setFuel(tokens[26]);
            builder.setImageCount(tokens[27]);
            builder.setInitialCarfaxFreeReport(tokens[28]);
            builder.setCarfaxOneOwner(tokens[29]);
            builder.setInitialCpo(tokens[30]);
            String[] features=tokens[31].split(":");
            List<String> ll=Arrays.asList(features);
            Collections.sort(ll);
            List<CharSequence> featureList=new ArrayList<CharSequence>(ll);
            builder.setFeatures(featureList);
            context.write(word, new AvroValue(builder.build()));




            /*
            word.set(new String("page:"+tokens[2]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("referring_domain:"+tokens[4]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("city:"+tokens[6]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("region:"+tokens[7]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("vehicle_condition:"+tokens[9]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("year:"+tokens[10]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("make:"+tokens[11]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("model:"+tokens[12]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("trim:"+tokens[13]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("body_style:"+tokens[14]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("subtrim:"+tokens[15]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("cab_style:"+tokens[16]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("mpg:"+tokens[19]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("exterior_color:"+tokens[20]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("interior_color:"+tokens[21]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("engine_displacement:"+tokens[22]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("engine:"+tokens[23]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("transmission:"+tokens[24]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("drive_type:"+tokens[25]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("fuel:"+tokens[26]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("initial_carfax_free_report:"+tokens[28]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("carfax_one_owner:"+tokens[29]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("initial_cpo:"+tokens[30]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            word.set(new String("features:"+tokens[31]));
            context.write(word, ONE);
            context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
*/

        }
	}

	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word count example.
	 */
	public static class ReduceClass extends Reducer<Text, AvroValue<Event>, AvroKey<CharSequence>, AvroValue<Session>> {

		/**
		 * Counter group for the reducer.  Individual counters are grouped for the reducer.
		 */
		private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

		@Override
		public void reduce(Text key, Iterable<AvroValue<Event>> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0L;

			context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);
            Session.Builder builder = Session.newBuilder();

            List<Event> events=new ArrayList<Event>();

            // Sum up the counts for the current word, specified in object "key".

			for (AvroValue<Event> value : values) {
                events.add(Event.newBuilder(value.datum()).build());
            }
            if(events.size()<=1000){
            Collections.sort(events,new Comparator<Event>() {
                public int compare(Event e1, Event e2) {

                    return e1.getEventTime().toString().compareTo(e2.getEventTime().toString());
                }
            });
            builder.setEvents(events);
            builder.setUserId(key.toString());
			// Emit the total count for the word.
            context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Session>(builder.build()) );
			}}
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
            System.err.println("Usage: UserSessions <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = new Job(conf, "UserSessions");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(UserSessions.class);
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MapClass.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, Event.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(job,Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());
        // Grab the input file and output directory from the command line.
        String[] inputPaths = appArgs[0].split(",");
        for ( String inputPath : inputPaths ) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

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
        int res = ToolRunner.run(new Configuration(), new UserSessions(), args);
        System.exit(res);
    }
}
