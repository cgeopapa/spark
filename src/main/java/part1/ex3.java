package part1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.List;

public class ex3
{
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("ex3").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path = "file:///"+System.getProperty("user.dir")+"/ml-latest/ratings.csv";
        JavaRDD<String> ratings = jsc.textFile(path, 4);

        final String header = ratings.first();
        ratings = ratings.filter((t) -> {
            return !t.equalsIgnoreCase(header);
        });

        JavaRDD<String> movies = jsc.textFile("file:///"+System.getProperty("user.dir")+"/ml-latest/movies.csv", 4);

        JavaPairRDD<String, Tuple2<Float, Integer>> ratingPairs = ratings.mapToPair((String line) -> {
            String[] items = line.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
            return new Tuple2<>(items[1], new Tuple2<>(Float.parseFloat(items[2]), 1));
        });

        JavaPairRDD<String, String> moviePairs = movies.mapToPair((String line) -> {
            String[] items = line.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
            return new Tuple2<>(items[0], items[1]);
        });

        JavaPairRDD<String, Tuple2<Tuple2<Float, Integer>, String>> counts = ratingPairs.reduceByKey((v1, v2) -> {
            return new Tuple2<>(v1._1+ v2._1, v1._2+v2._2);
        }).join(moviePairs);

        counts.map((e) -> e._2._2+": "+(e._2._1._1/e._2._1._2)).coalesce(1).saveAsTextFile("file:///"+System.getProperty("user.dir")+"/output/part1/ex3");
    }
}
