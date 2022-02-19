package miu.edu.cs523.kafkaConsumer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.JSONObject;

public class SparkKafkaConsumer {

	private static JavaStreamingContext streamingContext;
	private static Configuration conf = new Configuration();
	public static final String hdfs = "hdfs://localhost:8020";
	public static final String hpath = "/user/cloudera/home/tweets/";
	public static final String fileName = "tweet.txt";
	private static FSDataOutputStream out = null;

	public static void main(String[] args) throws InterruptedException {

		conf.set("fs.defaultFS", hdfs);
		conf.set("hadoop.job.ugi", "hdfs");
		conf.set("dfs.replication", "1");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "false");

		SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkKafkaConsumer");
		streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
		streamingContext.sparkContext().setLogLevel("WARN");
		JavaInputDStream<ConsumerRecord<String, String>> inputDStream = createInputDStream(streamingContext);
		consumeAndStoreStreams(inputDStream);
		Runtime.getRuntime().addShutdownHook(shutDownHookListener);

		streamingContext.start();
		streamingContext.awaitTermination();

	}

	private static void consumeAndStoreStreams(JavaInputDStream<ConsumerRecord<String, String>> inputDStream) {

		JavaDStream<String> dstream = inputDStream
				.map(stream -> convertTweetToJSON(stream.value()))
				.filter(tweet -> tweet != null)
				.filter(tweet -> !tweet.isEmpty());

		dstream.foreachRDD((rdd, time) -> {

			System.out.println("rdd value: " + rdd.count());
			if (!rdd.isEmpty()) {
				
				try {
									
					FileSystem fs = FileSystem.get(conf);
					System.out.println(fs.getHomeDirectory().getName());
					Path filenamePath = new Path(hpath + fileName);
					
					if(!fs.exists(filenamePath)){
						out = fs.create(filenamePath);
					}else{
						out = fs.append(filenamePath);
					}
					
					rdd.collect().forEach(string -> {
						
						System.out.println("Data received ---" + string);
						
						try {
							
							out.writeBytes(string + "\r\n");
						} catch (Exception e) {
							e.printStackTrace();
						}
						
					});
					
					
					System.out.println("written to file");
					out.close();
					
				} catch (Exception e) {
					
					e.printStackTrace();
				}
				
			}
		});
	}

	private static JavaInputDStream<ConsumerRecord<String, String>> createInputDStream(JavaStreamingContext streamingContext) {

		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ApplicationConfig.Kafka.KAFKA_BROKERS);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ApplicationConfig.Kafka.KEY_DESERIALIZER_CLASS);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ApplicationConfig.Kafka.VALUE_DESERIALIZER_CALSS);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, ApplicationConfig.Kafka.GROUP_ID);
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ApplicationConfig.Kafka.AUTO_OFFSET_RESET);
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		Collection<String> topics = Arrays.asList(ApplicationConfig.Kafka.TOPIC);

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies
						.PreferConsistent(), ConsumerStrategies
						.<String, String> Subscribe(topics, kafkaParams));

		return stream;
	}

	private static String convertTweetToJSON(String tweet) {
		
		String COMMA = ",";

		try {

			JSONObject tweetObject = new JSONObject(tweet);;

			String createdAt = tweetObject.getString("created_at");
			if(!validateTweetDate(createdAt)) {
				return null;
			}

			
			String text = tweetObject.getString("text");
			text = text.replaceAll("[\\n]", "");

			JSONObject userObject = tweetObject.getJSONObject("user");
			String name = userObject.getString("name");
			name = name.replaceAll("[\\n]", "");
			int followersCount = userObject.getInt("followers_count");
			int friendsCount = userObject.getInt("friends_count");
			int retweetCount = tweetObject.getInt("retweet_count");
			int replyCount = tweetObject.getInt("reply_count");
			String lang = tweetObject.getString("lang");
			lang = lang.replaceAll("[\\n]", "");
			
			//if the tweet text is null ignore the tweet
			if (tweet == null  || (tweet != null && tweet.trim() == null)) {
				return null;
			}
			
			StringBuilder sb = new StringBuilder();
			sb.append(createdAt).append(COMMA).append(text).append(COMMA).append(name).append(COMMA).append(followersCount).append(COMMA).append(friendsCount)
			.append(COMMA).append(retweetCount).append(COMMA).append(replyCount).append(COMMA).append(lang);
			String line = sb.toString();
			line = line.replaceAll("[\\n]", "");
			
			System.out.println("HERE> " + line);

			return line;

		} catch (Exception e) {

			return null;
		}
	}

	private static boolean validateTweetDate(String createdAt) {
		//check if createdAt is a correct date time format
		try {
			DateFormat formatter = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
			Date date = (Date)formatter.parse(createdAt);
			System.out.println("valid date: " + date);  
			return true;
			
		} catch (Exception e) {
			System.out.println("Invalid date format");  
			return false;
		}
	}

	private static Thread shutDownHookListener = new Thread() {

		@Override
		public void run() {
			streamingContext.close();
		};
	};
	
	

}
