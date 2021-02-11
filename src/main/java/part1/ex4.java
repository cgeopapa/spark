package part1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class ex4
{
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("ex4").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path = "file:///"+System.getProperty("user.dir")+"/ml-latest/movies.csv";
        JavaRDD<String> movies = jsc.textFile(path, 4);

        final String header = movies.first();
        movies = movies.filter((t) -> {
            return !t.equalsIgnoreCase(header);
        });

        JavaPairRDD<String, Tuple2<Integer, Integer>> moviePairs = movies.mapToPair((String line) -> {
            String[] items = line.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
            int genreCount = items[2].split("\\|").length;
            return new Tuple2<>("key", new Tuple2<>(genreCount, 1));
        });

        JavaPairRDD<String, Tuple2<Integer, Integer>> counts = moviePairs.reduceByKey((v1, v2) -> {
            return new Tuple2<>(v1._1+ v2._1, v1._2+v2._2);
        });

        counts.map((e) -> "Movie genre mean: "+(e._2._1.doubleValue()/e._2._2)).coalesce(1).saveAsTextFile("file:///"+System.getProperty("user.dir")+"/output/part1/ex4");
    }
}
