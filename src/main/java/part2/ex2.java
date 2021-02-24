package part2;

import com.vividsolutions.jts.geom.Coordinate;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import part2.model.Ship2;
import scala.Tuple2;

import java.util.ArrayList;

public class ex2
{
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("ex2g").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path = "file:///" + System.getProperty("user.dir") + "/geo/ais_dynamic_messages_unipi.csv";
        JavaRDD<String> ais = jsc.textFile(path, 4);

        final String header = ais.first();
        ais = ais.filter((t) -> !t.equalsIgnoreCase(header));

        JavaPairRDD<String, Ship2> shipPairs = ais.mapToPair((String line) -> {
            String[] items = line.split("\\;", -1);
            Ship2 ship = new Ship2(
                    items[0],
                    items[1],
                    new Coordinate(Double.parseDouble(items[2]), Double.parseDouble(items[3])),
                    Long.parseLong(items[4]),
                    items[5]
            );
            return new Tuple2<>(items[0], ship);
        });

        JavaPairRDD<String, ArrayList<Ship2>> ships = shipPairs.aggregateByKey(new ArrayList<>(),
        (s1, s2) -> {
            if(s1.isEmpty())
            {
                s1.add(s2);
            }
            else
            {
                Ship2 s = s1.get(s1.size() - 1);
                s2.speed = getSpeed(s, s2);
                if(s2.speed < 100)
                {
                    s1.add(s2);
                }
            }
            return s1;
        }, (s1,s2) -> {
            s1.addAll(s2);
            return s1;
        });

        ships.coalesce(1).saveAsTextFile("file:///"+System.getProperty("user.dir")+"/output/part2/ex2");
    }
    
    private static double getSpeed(Ship2 start, Ship2 end)
    {
        double dist = haversineDistance(start.coordinate, end.coordinate);
        double timeDiff = (end.timestamp - start.timestamp) / 1000d / 60d / 60d;
        return (dist / timeDiff);
    }

    //Method found here https://github.com/jasonwinn/haversine/blob/master/Haversine.java
    //Transformed to work with jts coordinates
    private static double haversineDistance(Coordinate start, Coordinate end)
    {
        final int EARTH_RADIUS = 6371; // Approx Earth radius in m

        double dLat  = Math.toRadians((end.y - start.y));
        double dLong = Math.toRadians((end.x - start.x));

        start.y = Math.toRadians(start.y);
        end.y   = Math.toRadians(end.y);

        double a = haversin(dLat) + Math.cos(start.y) * Math.cos(end.y) * haversin(dLong);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_RADIUS * c; // <-- d
    }

    private static double haversin(double val) {
        return Math.pow(Math.sin(val / 2), 2);
    }
}
