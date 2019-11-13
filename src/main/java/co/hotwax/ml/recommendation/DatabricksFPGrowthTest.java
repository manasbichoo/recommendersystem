package co.hotwax.ml.recommendation;

import static org.apache.spark.sql.functions.collect_set;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import co.hotwax.ml.recommendation.util.SparkUtil;

public class DatabricksFPGrowthTest {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		LogManager.getLogger("org").setLevel(Level.OFF);
		
		SparkSession spark =  SparkSession.builder().master("local[4]").appName("DatabricksFPGrowthTest").getOrCreate();
		
		//SparkSession spark =  SparkSession.builder().master("local[4]").
		System.out.println("Conf - "+spark.conf().getAll());
		
		

		Dataset<Row> aisles = spark.read().format("com.databricks.spark.csv").option("header", "true")
				.option("inferschema", "true").load("/Users/grv/WS/HWC/spark-example/data/mllib/aisles.csv");
		Dataset<Row> departments = spark.read().format("com.databricks.spark.csv").option("header", "true")
				.option("inferschema", "true").load("/Users/grv/WS/HWC/spark-example/data/mllib/departments.csv");
		Dataset<Row> order_products_prior = spark.read().format("com.databricks.spark.csv").option("header", "true")
				.option("inferschema", "true").load("/Users/grv/WS/HWC/spark-example/data/mllib/order_products__prior.csv");
		Dataset<Row> order_products_train = spark.read().format("com.databricks.spark.csv").option("header", "true")
				.option("inferschema", "true").load("/Users/grv/WS/HWC/spark-example/data/mllib/order_products__train.csv");
		Dataset<Row> orders = spark.read().format("com.databricks.spark.csv").option("header", "true")
				.option("inferschema", "true").load("/Users/grv/WS/HWC/spark-example/data/mllib/orders.csv");
		Dataset<Row> products = spark.read().format("com.databricks.spark.csv").option("header", "true")
				.option("inferschema", "true").load("/Users/grv/WS/HWC/spark-example/data/mllib/products.csv");
		System.out.println("Order line count "+orders.count());
		System.out.println("Product line count "+products.count());

		aisles.createOrReplaceTempView("aisles");
		departments.createOrReplaceTempView("departments");
		order_products_prior.createOrReplaceTempView("order_products_prior");
		order_products_train.createOrReplaceTempView("order_products_train");
		orders.createOrReplaceTempView("orders");
		products.createOrReplaceTempView("products");

		Dataset<Row> rawData = spark.sql(
				"select p.product_name, o.order_id from products p inner join order_products_prior o where o.product_id = p.product_id");
		rawData.show(15);
		Dataset<Row> baskets = rawData.groupBy("order_id").agg(collect_set("product_name").alias("items"));
		baskets.createOrReplaceTempView("baskets");
		System.out.println("Showing basket ");
		baskets.show(20);

		Dataset<Row> baskets_ds = spark.sql("select items from baskets").as("Array[String]").toDF("items");
		// baskets_ds.show(10);

		// Use FPGrowth
		FPGrowth fpGrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.001).setMinConfidence(0);
		long start = System.currentTimeMillis();
		FPGrowthModel model = fpGrowth.fit(baskets_ds);
		System.out.println("Time to do fit " + (System.currentTimeMillis() - start)/1000);
		//System.setProperty("com.amazonaws.services.s3a.enableV4", "false");
		// spark.sparkContext().hadoopConfiguration().set("fs.s3a.impl",
		// "org.apache.hadoop.fs.s3a.S3AFileSystem");
		// spark.sparkContext().hadoopConfiguration().set("com.amazonaws.services.s3n.enableV4",
		// "false");
		// spark.sparkContext().hadoopConfiguration().set("fs.s3n.endpoint","s3.ap-south-1.amazonaws.com");
		System.out.println("Starting uploading model ...");
		
		try {
			//SparkUtil.saveToFileSystem(model, "/Users/grv/development/spark/recommendation/model");
			//SparkUtil.saveToS3(model, "spark/recommendation/model");
			SparkUtil.saveToHDFS(model, "spark/recommendation/model");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		System.out.println("Frequent sets - ");
		// model.freqItemsets().show();

		// Display generated association rules.
		// System.out.println("AssociationRules sets - ");
		// model.associationRules().show();

		// transform examines the input items against all the association rules and
		// summarize the
		// consequents as prediction
		System.out.println("Transform  - ");
		// model.transform(baskets_ds).show();

	}

}
