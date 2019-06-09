import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaUtils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import scala.collection.immutable.Stream;

import javax.swing.plaf.synth.SynthTextAreaUI;
import java.io.PrintStream;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RealTimeStreamAnalysisDriver {

    static LocalDateTime expiryTime = null;
    static Queue<Long> processQueue = new LinkedList<>();
    //    static Queue<Long> patternQueue = new LinkedList<>();
    static Integer processedCount = 0;
    static Integer inputCount = 0;
    static Long octetTotal = Long.valueOf(0);
    private static boolean isStarted = false;

    public static void main(String[] args) throws Exception {
        PrintStream fileOut = new PrintStream("/Users/reshmasubramaniam/Desktop/Real-Time-Stream-Analysis-" +
                "Using-Spark/src/main/resources/output_transmissionRate100_transmissionTime30.txt");
        System.setOut(fileOut);
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("SparkRealTimeStreamingApp");
        // Create the context
        JavaStreamingContext streamingContext =
                new JavaStreamingContext(conf, Durations.seconds(Constants.WINDOW_TIME));
        Logger.getAnonymousLogger().setLevel(Level.SEVERE);
        // Create a DStream that will connect to hostname:port
        JavaReceiverInputDStream<String> udpPacket = streamingContext.socketTextStream(Constants.HOST, Constants.PORT);
        System.out.println(String.format("%-10s \t\t\t\t\t %-45s", "Time", "dOctects"));
        System.out.println("---------------------------------------------------------------");
        udpPacket.foreachRDD(packetRdd -> processOctetList(packetRdd.collect()));
        streamingContext.start();
        streamingContext.awaitTermination();

    }

    static void processOctetList(List<String> packetList) {
        inputCount += packetList.size();
        //System.out.println("Total input count:" + inputCount);
        //System.out.println("Current packet size:" + packetList.size());
        if ((packetList.size() > 0)) {
            isStarted = true;
            for (String packet : packetList) {
                LocalDateTime currentTime = getUnixDate(Long.parseLong(packet.split(",")[4]));
                Long octet = Long.parseLong(packet.split(",")[15]);
                if (expiryTime == null)
                    expiryTime = currentTime.plusSeconds(Constants.WINDOW_TIME).withNano(0);
                if (expiryTime.compareTo(currentTime) <= 0) {
                    System.out.println("\nWindow time:" + expiryTime.minusSeconds(Constants.WINDOW_TIME).toString() + " <-> " + currentTime.toString());
                    getMaxAndAvgForQueue(processQueue, processQueue.size());
                    expiryTime = currentTime.plusSeconds(Constants.WINDOW_TIME);
                }
                processQueue.add(octet);
                System.out.println(currentTime.toString() + "\t\t\t" + octet);
            }
        }

        if (isStarted && packetList.size() == 0) {
            LocalDateTime prevWindowEnd = expiryTime.minusSeconds(Constants.WINDOW_TIME);
            System.out.println("\nWindow time:" + prevWindowEnd.toString() + " <-> " + expiryTime.toString());
            getMaxAndAvgForQueue(processQueue, processQueue.size());
            System.out.println("Octet total:" + octetTotal);
            isStarted = false;
            //System.exit(0);
        }
    }

    static void getMaxAndAvgForQueue(Queue<Long> toProcessQueue, Integer size) {
        processedCount += size;
        //System.out.println("Processed : "+ size + "Total arrived: " + processedCount);
        Integer queueSize = size;
        if (size == 0)
            return;
        Long max = Long.MIN_VALUE;
        Long mean = Long.valueOf(0);
        while (size > 0) {
            Long octet = toProcessQueue.remove();
            octetTotal += octet;
            if (max < octet)
                max = octet;
            mean += octet;
            size--;
        }
        mean /= queueSize;
        System.out.println("\nLargest Value :" + max + "\nMoving Average:" + mean + "\n\n");
    }


    private static LocalDateTime getUnixDate(Long milliseconds) {
        Instant instant = Instant.ofEpochMilli(milliseconds);
        LocalDateTime convertedDate = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
        return convertedDate;
    }

}

