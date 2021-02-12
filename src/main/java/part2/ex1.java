package part2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class ex1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ex1g").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path = "file:///" + System.getProperty("user.dir") + "/geo/ais_dynamic_messages_unipi.csv";
        JavaRDD<String> ais = jsc.textFile(path, 4);

        final String header = ais.first();
        ais = ais.filter((t) -> {
            return !t.equalsIgnoreCase(header);
        });

        JavaPairRDD<String, Ship> shipPairs = ais.mapToPair((String line) -> {
            String[] items = line.split("\\;", -1);
            Ship ship = new Ship(
                    items[0],
                    items[1],
                    Double.parseDouble(items[2]),
                    Double.parseDouble(items[3]),
                    Long.parseLong(items[4]),
                    items[5],
                    items[6]
            );
            return new Tuple2<>(items[0], ship);
        });

        shipPairs = shipPairs.reduceByKey((s1, s2) -> {
            int count = s1.count + s2.count;
            double maxlon = Math.max(s1.lon, s2.lon);
            double minlon = Math.min(s1.lon, s2.lon);
            double maxlat = Math.max(s1.lat, s2.lat);
            double minlat = Math.min(s1.lat, s2.lat);
            long maxt = Math.max(s1.timestamp, s2.timestamp);
            long mint = Math.min(s1.timestamp, s2.timestamp);

            return new Ship(count, maxlon, minlon, maxlat, minlat, maxt, mint);
        });

        int shipCount = (int) shipPairs.keys().count();
        System.out.println("Ship count: "+shipCount);

        shipPairs.foreach(s -> {
            System.out.println("Ship "+s._1+" data count: "+s._2.count);
        });

        double maxlon = 0;
        double minlon = 0;
        double maxlat = 0;
        double minlat = 0;
        long maxt = 0;
        long mint = 0;

        for(Ship ship: shipPairs.values().collect())
        {
            maxlon = Math.max(maxlon, ship.maxlon);
            minlon = Math.min(minlon, ship.minlon);
            maxlat = Math.max(maxlat, ship.maxlat);
            minlat = Math.min(minlat, ship.minlat);
            maxt = Math.max(maxt, ship.maxt);
            mint = Math.min(mint, ship.mint);
        }

        System.out.println("Max longitude: "+maxlon);
        System.out.println("Min longitude: "+minlon);
        System.out.println("Max latitude: "+maxlat);
        System.out.println("Min latitude: "+minlat);
        System.out.println("Max timestamp: "+maxt);
        System.out.println("Min timestamp: "+mint);
    }
}
