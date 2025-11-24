package ma.atif;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogAnalysisRDD {
    public static void main(String[] args) {
        String logFile = "access.log"; // replace with full path if needed

        SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> logs = sc.textFile(logFile);

        // Regex to parse Apache log line
        String logPattern =
                "^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(.*?)\\]\\s+\"(\\S+)\\s+(\\S+)\\s+\\S+\"\\s+(\\d{3})\\s+(\\d+).*";
        Pattern pattern = Pattern.compile(logPattern);

        // Extract fields: IP, date/time, method, resource, status, size
        JavaRDD<LogEntry> parsedLogs = logs.map(line -> {
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                String ip = matcher.group(1);
                String datetime = matcher.group(2);
                String method = matcher.group(3);
                String resource = matcher.group(4);
                int status = Integer.parseInt(matcher.group(5));
                int size = Integer.parseInt(matcher.group(6));
                return new LogEntry(ip, datetime, method, resource, status, size);
            } else {
                return null;
            }
        }).filter(entry -> entry != null);

        // Q1: Total requests
        long totalRequests = parsedLogs.count();
        System.out.println("Q1: Total requests = " + totalRequests);

        // Q2: Total errors (status >= 400)
        long totalErrors = parsedLogs.filter(e -> e.status >= 400).count();
        System.out.println("Q2: Total errors = " + totalErrors);

        // Q3: Error percentage
        double errorPercentage = (totalErrors * 100.0) / totalRequests;
        System.out.println("Q3: Error percentage = " + errorPercentage + "%");

        // Q4: Top 5 IPs
        List<Tuple2<String, Long>> topIPs = parsedLogs
                .mapToPair(e -> new Tuple2<>(e.ip, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .take(5);

        System.out.println("Q4: Top 5 IPs:");
        for (int i = 0; i < topIPs.size(); i++) {
            Tuple2<String, Long> t = topIPs.get(i);
            System.out.println((i+1) + ". " + t._1 + " -> " + t._2 + " requests");
        }

        // Q5: Top 5 resources
        List<Tuple2<String, Long>> topResources = parsedLogs
                .mapToPair(e -> new Tuple2<>(e.resource, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .take(5);

        System.out.println("Q5: Top 5 resources:");
        for (int i = 0; i < topResources.size(); i++) {
            Tuple2<String, Long> t = topResources.get(i);
            System.out.println((i+1) + ". " + t._1 + " -> " + t._2 + " requests");
        }

        // Q6: Requests per HTTP code
        List<Tuple2<Integer, Long>> codes = parsedLogs
                .mapToPair(e -> new Tuple2<>(e.status, 1L))
                .reduceByKey(Long::sum)
                .collect();

        System.out.println("Q6: Requests per HTTP status code:");
        for (Tuple2<Integer, Long> t : codes) {
            System.out.println("Status " + t._1 + " -> " + t._2 + " requests");
        }

        sc.close();
    }

    // Helper class
    static class LogEntry {
        String ip;
        String datetime;
        String method;
        String resource;
        int status;
        int size;

        public LogEntry(String ip, String datetime, String method, String resource, int status, int size) {
            this.ip = ip;
            this.datetime = datetime;
            this.method = method;
            this.resource = resource;
            this.status = status;
            this.size = size;
        }
    }
}
