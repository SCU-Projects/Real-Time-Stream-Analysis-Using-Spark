import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RealTimeStreamAnalysisDriver {

    static Long expiryTime = null;
    static Queue<Long> processQueue = new LinkedList<>();

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("SparkRealTimeStreamingApp");
        JavaStreamingContext streamingContext =
                new JavaStreamingContext(conf, Durations.seconds(20));
        Logger.getAnonymousLogger().setLevel(Level.SEVERE);
        JavaReceiverInputDStream<String> udpPacket = streamingContext.socketTextStream(Constants.HOST, Constants.PORT);
        List<Long> octetsList = new ArrayList<>();
        udpPacket.foreachRDD(packetRdd -> processOctetList(packetRdd.collect()));
        streamingContext.start();
        streamingContext.awaitTermination();

    }

    static void processOctetList(List<String> packetList) {
        System.out.println("Size" + packetList.size());
        for (String packet : packetList) {
            Long currentTime = Long.parseLong(packet.split(",")[4]);
            if (expiryTime == null)
                expiryTime = currentTime + Long.valueOf(60000);
            if (expiryTime <= currentTime) {
                getMaxAndAvgForQueue(processQueue, processQueue.size());
                expiryTime += Long.valueOf(60000);
            }
            processQueue.add(Long.parseLong(packet.split(",")[15]));
        }
    }

    static void getMaxAndAvgForQueue(Queue<Long> toProcessQueue, Integer size) {

        Integer queueSize = size;
        if (size == 0)
            return;
        Long max = Long.MIN_VALUE;
        Long mean = Long.valueOf(0);
        while (size > 0) {
            Long octet = toProcessQueue.remove();
            if (max < octet)
                max = octet;
            mean += octet;
            size--;
        }
        mean /= queueSize;
        System.out.println("Max:" + max + "\tAvg:" + mean);

    }
}

