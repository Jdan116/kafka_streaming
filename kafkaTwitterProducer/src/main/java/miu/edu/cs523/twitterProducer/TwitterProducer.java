package miu.edu.cs523.twitterProducer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        // create twitter client and connect
        Client client = createTwitterClient(msgQueue);
        client.connect();


        // create a kafka producer
        KafkaProducer<String, String> producer = createProducer();


        // loop to send tweets to kafka

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
                System.out.println("writing message: " + msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
                logger.error("End of application");
            }
            if(msg != null) {
             logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg),
                        //callback function in case of error
                        (recordMetadata, exception) -> {
                            if(exception != null ) {
                            	logger.error("exception when producing: ", exception);
                            }
                        }
                );
            }
        }

    }

    private KafkaProducer<String, String> createProducer() {
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hoseBirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("#ethereum");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(TwitterConfig.CONSUMER_KEY, TwitterConfig.CONSUMER_SECRET, TwitterConfig.TOKEN, TwitterConfig.SECRET);


        ClientBuilder builder = new ClientBuilder()
                .name("HoseBird-Client-01") // optional: mainly for the logs
                .hosts(hoseBirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();

    }
}
