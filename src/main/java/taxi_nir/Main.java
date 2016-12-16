package taxi_nir;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

/**
 * Created by cloudera on 12/16/16.
 */
public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("App01");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> TaxiOrderRDD = sc.textFile("/home/cloudera/IdeaProjects/spark-888/data/taxi/taxi_order.txt");

        // Question 1 - # of trips
        System.out.println(TaxiOrderRDD.count());

        // print RDD one line
        System.out.println(TaxiOrderRDD.collect());

        // print RDD multi lines
        for (String s : TaxiOrderRDD.collect()) {
            System.out.println(s);
        }

        // Question 2 - # of trips to boston where distance > 10km

        JavaRDD<String[]> boston = TaxiOrderRDD.map(t -> t.split(" "))
                .filter(c -> c[1].toLowerCase().equals("boston"))
                .filter(d -> Integer.parseInt(d[2]) > 10);

        System.out.println(boston.count());

        // Question 3 - calculate sum of km trips to boston

        int boston2 = TaxiOrderRDD.map(t -> t.split(" "))
                .filter(c -> c[1].toLowerCase().equals("boston"))
                .map(x -> Integer.parseInt(x[2]))
                .reduce((a, b) -> (a + b));

        System.out.println(boston2);

        // Question 4 - write names of 3 drivers with max km


        JavaPairRDD<String, Integer> Top3DriversRDD  = TaxiOrderRDD.map(t -> t.split(" "))
                .mapToPair(t -> new Tuple2<>(t[0], Integer.parseInt(t[2])))
                .reduceByKey((a, b) -> (a + b))
                .mapToPair(x -> x.swap())
                .sortByKey(false)
                .mapToPair(x -> x.swap())
                .zipWithIndex() // i used this instead of take because take return list, and I wanted to return RDD
                .filter(x -> x._2 <= 2)
                .mapToPair(x -> x._1());

        JavaPairRDD<String, Tuple2<String, Integer>> join = sc.textFile("/home/cloudera/IdeaProjects/spark-888/data/taxi/drivers.txt")
                .map(x -> x.split(","))
                .mapToPair(x -> new Tuple2<>(x[0], x[1]))
                .join(Top3DriversRDD);

        for (Tuple2<String, Tuple2<String, Integer>> i : join.collect()) {
            System.out.println("Driver Name: " + i._2._1 + " , Total km: " + i._2._2.toString());
            System.out.println();

        }
    }
}
