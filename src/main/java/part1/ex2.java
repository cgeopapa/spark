package part1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.List;

public class ex2
{
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("ex2").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path = "file:///"+System.getProperty("user.dir")+"/ml-latest/tags.csv";
        JavaRDD<String> tags = jsc.textFile(path, 4);

        JavaRDD<String> movies = jsc.textFile("file:///"+System.getProperty("user.dir")+"/ml-latest/movies.csv", 4);

        JavaPairRDD<String, Integer> tagsPairs = tags.mapToPair((String line) -> {
            String[] items = line.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
            return new Tuple2<>(items[1], 1);
        });

        JavaPairRDD<String, String> moviePairs = movies.mapToPair((String line) -> {
            String[] items = line.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
            return new Tuple2<>(items[0], items[1]);
        });

        JavaPairRDD<String, Tuple2<Integer, String>> counts = tagsPairs.reduceByKey(Integer::sum).join(moviePairs);

        counts.map((e) -> e._2._2+": "+e._2._1).coalesce(1).saveAsTextFile("file:///"+System.getProperty("user.dir")+"/output/part1/ex2");
    }
}
