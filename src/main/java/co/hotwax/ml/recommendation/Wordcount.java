package co.hotwax.ml.recommendation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
// $example on$
import java.util.Arrays;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.SparkSession;
// $example off$

public class Wordcount {
    public static void main(String[] args) {

//        SparkSession spark = SparkSession
//                .builder()
//                .appName("JavaCorrelationExample").master("local[2]").getOrCreate();
//
//
//
//        // $example on$
//        JavaSparkContext jsc;
//        JavaDoubleRDD seriesX = jsc.parallelizeDoubles(
//                Arrays.asList(1.0, 2.0, 3.0, 3.0, 5.0));  // a series
//
//        // must have the same number of partitions and cardinality as seriesX
//        JavaDoubleRDD seriesY = jsc.parallelizeDoubles(
//                Arrays.asList(11.0, 22.0, 33.0, 33.0, 555.0));
//
//        // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method.
//        // If a method is not specified, Pearson's method will be used by default.
//        double correlation = Statistics.corr(seriesX.srdd(), seriesY.srdd(), "pearson");
//        System.out.println("Correlation is: " + correlation);
//
//        // note that each Vector is a row and not a column
//        JavaRDD<Vector> data = jsc.parallelize(
//                Arrays.asList(
//                        Vectors.dense(1.0, 10.0, 100.0),
//                        Vectors.dense(2.0, 20.0, 200.0),
//                        Vectors.dense(5.0, 33.0, 366.0)
//                )
//        );
//
//        // calculate the correlation matrix using Pearson's method.
//        // Use "spearman" for Spearman's method.
//        // If a method is not specified, Pearson's method will be used by default.
//        Matrix correlMatrix = Statistics.corr(data.rdd(), "pearson");
//        System.out.println(correlMatrix.toString());
//        // $example off$
//
//        jsc.stop();.master("local[*]")
    }
}