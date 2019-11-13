package co.hotwax.ml.clustering;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KMeansClustering {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkSession session = SparkSession.builder().appName("RecommendationJob").getOrCreate();
		
		Dataset<Row> product_price = session.read().format("csv").option("header", "true").load("data/product_price_new.csv");
		
		Dataset<Row> product_desc = session.read().format("csv").option("header", "true").load("data/Product_desc.csv");
		

	}

}
