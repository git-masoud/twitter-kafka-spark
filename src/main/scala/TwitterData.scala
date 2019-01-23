import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import org.apache.spark.streaming.twitter.TwitterUtils

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object TwitterData {

  val appName = "TwitterData"
  val sc=new SparkConf().setAppName("appName")
    .setMaster("local[*]")
  //create context
  val ssc = new StreamingContext(sc, Seconds(10))


  // values of Twitter API.
  val consumerKey = "" // Your consumerKey
  val consumerSecret = "" // your API secret
  val accessToken ="" // your access token
  val accessTokenSecret = "" // your token secret

  def init() ={

  }

  def main(args: Array[String]): Unit = {
    //Connection to Twitter API
    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)

    val auth = new OAuthAuthorization(cb.build)
    val tweets = TwitterUtils.createStream(ssc, Some(auth))
    val englishTweets = tweets.filter(_.getLang() == "en")
    val statuses = englishTweets.map(status => (status.getText(),status.getUser.getName(),status.getUser.getScreenName(),status.getCreatedAt.toString))


    statuses.foreachRDD { (rdd, time) =>

      rdd.foreachPartition { partitionIter =>
        val props = new Properties()
        val bootstrap = "localhost:19092,localhost:29092,localhost:39092" //-- your external ip of GCP VM, example: 10.0.0.1:9092
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("bootstrap.servers", bootstrap)
        val producer = new KafkaProducer[String, String](props)
        partitionIter.foreach { elem =>
          val dat = elem.toString()
          val data = new ProducerRecord[String, String]("twitter", null, dat) // "llamada" is the name of Kafka topic
          producer.send(data)
        }
        producer.flush()
        producer.close()
      }
    }
    ssc.start()
    ssc.awaitTermination()

  }

}
