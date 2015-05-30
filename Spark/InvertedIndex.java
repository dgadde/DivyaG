package com.refactorlabs.cs378.assign11;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import org.apache.commons.lang.StringUtils;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Collections;
import java.util.Iterator;
import java.util.*;

/**
 * WordCount application for Spark.
 */
public class InvertedIndex {
	public static void main(String[] args) {
		Utils.printClassPath();

		String inputFilename = args[0];
		String outputFilename = args[1];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(InvertedIndex.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the input data
		JavaRDD<String> input = sc.textFile(inputFilename);

		// Split the input into words
		FlatMapFunction<String, String> splitFunction = new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String line) throws Exception {
				StringTokenizer tokenizer = new StringTokenizer(line);
				List<String> wordList = Lists.newArrayList();
                String verse=tokenizer.nextToken();
				// For each word in the input line, emit that word.
				while (tokenizer.hasMoreTokens()) {
					wordList.add(tokenizer.nextToken());
				}
				return wordList;
			}
		};

        PairFlatMapFunction<String, String, String> splitF = new PairFlatMapFunction<String, String, String>() {
            @Override
            public Iterable<Tuple2<String,String>> call(String value) throws Exception {
                String line =StringUtils.lowerCase(StringUtils.replaceEach(value.toString(), new String[]{",", "'", "\"", ".", "?", "!", "(", ";", "_", "-",")"}, new String[]{" ", " ", " ", " ", " ", " ", " ", " ", " ", " "," "}));

                StringTokenizer tokenizer = new StringTokenizer(line);
                List<Tuple2<String, String>> wordList = Lists.newArrayList();
                String verse=tokenizer.nextToken();
                HashSet<String> hs=new HashSet<String>();
                // For each word in the input line, emit that word.
                while (tokenizer.hasMoreTokens()) {
                    hs.add(tokenizer.nextToken());
                    //wordList.add(new Tuple2(tokenizer.nextToken(),verse));
                }
                for(String token:hs){
                    wordList.add(new Tuple2(token,verse));
                }
                return wordList;
            }
        };

		//JavaRDD<String> words = input.flatMap(splitF);

        JavaPairRDD<String, Iterable<String>> pairs=input.flatMapToPair(splitF).groupByKey().sortByKey().mapValues(new Function<Iterable<String>, Iterable<String>>(){
            public Iterable<String> call(Iterable<String> s){
                Iterator<String> iter=s.iterator();
                ArrayList<String> arr=new ArrayList<String>();
                while(iter.hasNext()){
                    arr.add(iter.next());
                }
                Collections.sort(arr);
                return arr;
            }
        } );
        pairs.saveAsTextFile(outputFilename);


		// Transform into word and count
		PairFunction<String, String, Integer> addCountFunction = new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2(s, 1);
			}
		};
		Function2<Integer, Integer, Integer> sumFunction = new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer count1, Integer count2) throws Exception {
				return count1 + count2;
			}
		};

		//JavaPairRDD<String, Integer> counts = words.mapToPair(addCountFunction).reduceByKey(sumFunction).sortByKey();

		// Save the word count to a test file (initiates evaluation)
		//counts.saveAsTextFile(outputFilename);

		// Shut down the context
		sc.stop();
	}

}
