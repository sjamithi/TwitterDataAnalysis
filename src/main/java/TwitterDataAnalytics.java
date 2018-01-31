import com.google.common.collect.Iterables;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

public class TwitterDataAnalytics {
        public static void main(String[] args) throws IOException, InterruptedException, TwitterException {

            Properties props = new Properties();

            String topicName = "twitterData";

            //Assign localhost id
            props.put("bootstrap.servers", "localhost:9092");

            //Set acknowledgements for producer requests.
            props.put("acks", "all");

                    //If the request fails, the producer can automatically retry,
                    props.put("retries", 0);

            //Specify buffer size in config
            props.put("batch.size", 16384);

            //Reduce the no of requests less than 0
            props.put("linger.ms", 1);

            //The buffer.memory controls the total amount of memory available to the producer for buffering.
            props.put("buffer.memory", 33554432);

            props.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");

            props.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
            String consumerKey = "X4MlpKe95iO7C2xdrSt8kk2cr";
            String consumerSecret = "cna0fFXmAzfRLLHwkkaDfxYavqeAkHkZwO38AKrmOAX8xeT3Za";
            String accessToken = "1047103668-xIjGMsHDhRYO7kCAWPfs8clqodrE48SSWd0dQvb";
            String accessTokenSecret = "UuMfqBGRPKmWxpQjwL2J9FvzzrXFKxGH5IRI0M1rvM2MI";

            // Set the system properties so that Twitter4j library used by Twitter stream
//             can use them to generate OAuth credentials

            System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
            System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
            System.setProperty("twitter4j.oauth.accessToken", accessToken);
            System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

            SparkConf sparkConf=new SparkConf().setAppName("Tweets Android").setMaster("local[2]");
            JavaStreamingContext sc=new JavaStreamingContext(sparkConf,new Duration(5000));
            Scanner scanner = new Scanner(System.in);
            System.out.println("Enter the brand name: ");
            String brand = scanner.next();

            String[] filters={brand};

            final Configuration conf = new ConfigurationBuilder().setDebugEnabled(false)
                    .setOAuthConsumerKey(consumerKey)
                    .setOAuthConsumerSecret(consumerSecret)
                    .setOAuthAccessToken(accessToken)
                    .setOAuthAccessTokenSecret(accessTokenSecret)
                    .build();

            // Create Twitter authorization object by passing prepared configuration containing consumer and access keys and tokens
            final Authorization twitterAuth = new OAuthAuthorization(conf);
            // Create a data stream using streaming context and Twitter authorization
            final JavaReceiverInputDStream<Status> inputDStream = TwitterUtils.createStream(sc, twitterAuth, filters);
            // Create a new stream by filtering the non english tweets from earlier streams
            final JavaDStream<Status> enTweetsDStream = inputDStream.filter((status) -> "en".equalsIgnoreCase(status.getLang()));
            // Convert stream to pair stream with key as user screen name and value as tweet text
            final JavaPairDStream<String, String> userTweetsStream =
                    enTweetsDStream.mapToPair(
                            (status) -> new Tuple2<>(status.getUser().getScreenName(), status.getText())
                    );

            // Group the tweets for each user
            final JavaPairDStream<String, Iterable<String>> tweetsReducedByUser = userTweetsStream.groupByKey();
            // Create a new pair stream by replacing iterable of tweets in older pair stream to number of tweets
            final JavaPairDStream<String, Integer> tweetsMappedByUser = tweetsReducedByUser.mapToPair(
                    userTweets -> new Tuple2<>(userTweets._1, Iterables.size(userTweets._2))
            );
            // Iterate over the stream's RDDs and print each element on console
            tweetsMappedByUser.foreachRDD((VoidFunction<JavaPairRDD<String, Integer>>) pairRDD -> {
                pairRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {

                    @Override
                    public void call(Tuple2<String, Integer> t) throws Exception {
                        Producer<String, String> producer = new KafkaProducer<String, String>(props);

                        String jsonString = new JSONObject()
                                        .put(t._1(), t._2).toString();

                        System.out.println(jsonString);
                            producer.send(new ProducerRecord<String, String>(topicName,
                                    jsonString));
                    }

                });
            });
            sc.start();
            sc.awaitTermination();


        }

        private static Tweet buildNewTweet(Status status) {
        return new Tweet(status.getUser().getId(),
                status.getUser().getName(),
                status.getUser().getScreenName(),
                status.getUser().getMiniProfileImageURL(),
                replaceNewLines(status.getText()),
                status.getGeoLocation() == null ? null : status.getGeoLocation().getLatitude(),
                status.getGeoLocation() == null ? null : status.getGeoLocation().getLongitude(),
                status.getLang(),
                status.getSource(),
                0,
                new Date(),
                status.getRetweetCount());
    }

    private static String replaceNewLines(String text) {
        return text.replace("\n", "");
    }
    }

