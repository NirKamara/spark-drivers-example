package taxi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by Evegeny on 14/12/2016.
 */
public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("taxi");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile("data/taxi/taxi_order.txt");
        System.out.println("file content");
        lines.collect().forEach(System.out::println);
        System.out.println("end of file");
        JavaRDD<Trip> trips = lines.map(String::toLowerCase)
                .map(Trip::convertLineToTrip);
        System.out.println("trip object");
        trips.collect().forEach(System.out::println);
        System.out.println("end of trip object");

        System.out.println("trips to boston");
        trips.filter(trip->trip.getCity().equals("boston"))
                .collect().forEach(System.out::println);
        System.out.println("end trips to boston");

       /* lines.map(String::toLowerCase)
                .filter(line->line.split(" ")[1].equals("boston"))*/

       /* Trip reduceTrip = trips.filter(Trip::bostonFilter)
                .filter(trip -> trip.getKm() > 10)
                .reduce(Trip::summarizeTrips);
            not good way to to reduce!!!
*/

       /* Double allTripsSum = trips.filter(Trip::bostonFilter)
                .filter(trip -> trip.getKm() > 10)
                .mapToDouble(Trip::getKm).sum();*/

        Integer allKm = trips.filter(Trip::bostonFilter)
                .filter(trip -> trip.getKm() > 10)
                .map(Trip::getKm).reduce(Integer::sum);
        System.out.println("allKm = " + allKm);

        System.out.println("drivers km");

        JavaPairRDD<String, Integer> id2KmRdd =
                trips.mapToPair(trip -> new Tuple2<>(trip.getId(), trip.getKm()));
        id2KmRdd.reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .collect().forEach(System.out::println);
        System.out.println("end drivers km");


    }
}
