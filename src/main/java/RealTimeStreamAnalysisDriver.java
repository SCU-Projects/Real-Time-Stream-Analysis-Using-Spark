import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RealTimeStreamAnalysisDriver {

    public static void main(String[] args) throws Exception {
        // Configure and initialize the SparkStreamingContext
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("SparkRealTimeStreamingApp");
        JavaStreamingContext streamingContext =
                new JavaStreamingContext(conf, Durations.seconds(5));
        Logger.getAnonymousLogger().setLevel(Level.SEVERE);

        // Receive streaming data from the source
        JavaReceiverInputDStream<String> udpPacket = streamingContext.socketTextStream(Constants.HOST, Constants.PORT);
        List<Long> octetsList = new ArrayList<>();
        udpPacket.foreachRDD(packet -> octetsList.add(Long.parseLong(packet.toString().split(",")[16])));
        Long max = Long.MIN_VALUE;
        Long mean = Long.valueOf(0);
        for(Long octet : octetsList){
            System.out.println(octet+"\t");
            if(max < octet)
                max = octet;
            mean += octet;
        }
        mean /= octetsList.size();
        System.out.println("Max:" + max + "\tAvg:" + mean);
        // Execute the Spark workflow defined above
        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(15000);
        streamingContext.stop();
    }


}
