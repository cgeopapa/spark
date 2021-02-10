package part1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.List;

public class ex1
{
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("ex1").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path = "file:///"+System.getProperty("user.dir")+"/ml-latest/ratings.csv";
        JavaRDD<String> lines = jsc.textFile(path, 4);

        JavaPairRDD<String, Integer> rdd = lines.mapToPair((String line) -> {
            String[] items = line.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
            return new Tuple2<>(items[0], 1);
        });

        JavaPairRDD<String, Integer> counts = rdd.reduceByKey(Integer::sum).filter((v) -> v._2 > 10);
        counts.saveAsTextFile("file:///"+System.getProperty("user.dir")+"/output/part1/ex1");

        jsc.close();
    }
}
