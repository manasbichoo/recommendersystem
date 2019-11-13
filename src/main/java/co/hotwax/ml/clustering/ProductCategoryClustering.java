package co.hotwax.ml.clustering;

import static org.apache.spark.sql.functions.bround;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.udf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.StringJoiner;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import com.hc.spark.util.DriverConstants;

import co.hotwax.ml.clustering.util.ClusteringUtils;
import co.hotwax.ml.recommendation.DriverConfig;

public class ProductCategoryClustering {

	private static SparkSession session;
	private static SQLContext sqlContext;
	private static String jdbcURL = "jdbc:mysql://" + DriverConfig.getDBHost() + "/" + DriverConfig.getDBName()
			+ "?autoReconnect=true&amp;useSSL=false&amp;characterEncoding=UTF-8";
	private static String CATEGORY = "TOPS";

	private static final String categoryRangeSQL = "(select o.product_id, o.unit_price, p.PRIMARY_PRODUCT_CATEGORY_ID from order_item o inner join product p on p.PRIMARY_PRODUCT_CATEGORY_ID = '"
			+ CATEGORY
			+ "' and o.product_id = p.product_id and o.unit_price != '0.000' order by o.product_id) as CategoryRange";
	private static final String productPriceSQL = "(select product_id, unit_price from order_item where unit_price != '0.000') AS ProductPrice";

	private static Properties sparkSQLProperties;

	static {
		sparkSQLProperties = new Properties();
		sparkSQLProperties.setProperty(DriverConstants.SPARK_SQL_DRIVER, DriverConfig.getDBDriver());
		sparkSQLProperties.setProperty(DriverConstants.SPARK_SQL_USER, DriverConfig.getDBUser());
		sparkSQLProperties.setProperty(DriverConstants.SPARK_SQL_URL, jdbcURL);
		sparkSQLProperties.setProperty(DriverConstants.SPARK_SQL_PASSWORD, DriverConfig.getDBPassword());

	}

	private static SparkSession getSparkSession() {
		if (session == null) {
			session = SparkSession.builder().appName("KMeansClustering").master("local[*]").getOrCreate();

		}
		System.out.println("Spark session - " + session);
		return session;

	}

	private static SQLContext getSQLContext() {
		if (sqlContext == null) {
			sqlContext = new SQLContext(getSparkSession());
		}
		System.out.println("SQL context - " + sqlContext);
		return sqlContext;
	}

	private static void printConf() {
		System.out.println("Using conf  - " + getSparkSession().conf().getAll());
	}

	public static Integer K(String category) {
		// TODO Auto-generated method stub

		LogManager.getLogger("org").setLevel(Level.OFF);

		System.out.println("Making sure driver class can be loaded ...");

		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Err, couldn't load MySQL Driver, exiting ...");
			System.exit(0);
		}

		String categoryRangeSQL = "(select o.product_id, o.unit_price, p.PRIMARY_PRODUCT_CATEGORY_ID from order_item o inner join product p on p.PRIMARY_PRODUCT_CATEGORY_ID = '"
				+ category
				+ "' and o.product_id = p.product_id and o.unit_price != '0.000' order by o.product_id) as CategoryRange";

		System.out.println("Running following query to fetch product prices for the category " + categoryRangeSQL);

		Dataset<Row> dataSetForkMeans = getSQLContext().read().jdbc(jdbcURL, categoryRangeSQL, sparkSQLProperties);
		System.out.println("dataSetForkMeans " + dataSetForkMeans.count());
		if (dataSetForkMeans.count() == 0) {
			return null;
		}

		// dataSetForkMeans.show();

		VectorAssembler assemblerTops = new VectorAssembler().setInputCols(new String[] { "unit_price" })
				.setOutputCol("features");
		Dataset<Row> vectorDataTops = assemblerTops.transform(dataSetForkMeans).drop("unit_price");

		System.out.println("======================Tops data Min and Max ===================");
		vectorDataTops.agg(min("features"), max("features")).show();
		vectorDataTops.show(false);

		return ClusteringUtils.optimalK(vectorDataTops);

	}

	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//
//		LogManager.getLogger("org").setLevel(Level.OFF);
//
//		System.out.println("Making sure driver class can be loaded ...");
//
//		try {
//			Class.forName("com.mysql.jdbc.Driver").newInstance();
//		} catch (InstantiationException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IllegalAccessException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (ClassNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			System.out.println("Err, couldn't load MySQL Driver, exiting ...");
//			System.exit(0);
//		}
//
//		long start = System.currentTimeMillis();
//
//		session = SparkSession.builder().appName("KMeansClustering").config("spark.eventLog.enabled", true)
//				.config("spark.eventLog.dir", "file:/tmp/spark-events").master("local[*]").getOrCreate();
//		System.out.println("Ye dekho " + session.conf().getAll());
//
//		session.conf().set("spark.eventLog.enabled", true);
//		session.conf().set("spark.eventLog.dir", "file:/tmp/spark-events");
//
//		System.out.println("Ye dekho ab " + session.conf().getAll());
//
//		System.out.println("Time to create spark session " + (System.currentTimeMillis() - start) / 1000);
//		sqlContext = new SQLContext(getSparkSession());
//
//		System.out.println("=========== K-Means for Tops category ===========");
//
//		Dataset<Row> dataSetForkMeans = sqlContext.read().jdbc(jdbcURL, categoryRangeSQL, sparkSQLProperties);
//		System.out.println("dataSetForkMeans " + dataSetForkMeans.count());
//
//		dataSetForkMeans.show();
//
//		VectorAssembler assemblerTops = new VectorAssembler().setInputCols(new String[] { "unit_price" })
//				.setOutputCol("features");
//		Dataset<Row> vectorDataTops = assemblerTops.transform(dataSetForkMeans).drop("unit_price");
//
//		System.out.println("======================Tops data Min and Max ===================");
//		vectorDataTops.agg(min("features"), max("features")).show();
//		vectorDataTops.show(false);
//
//		ClusteringUtils.getOptimalK(vectorDataTops);
//
//		int kCount = 5; // Here we will try to determine optimal 'K' and not hard code it.
//
//		KMeans kMeansForTops = new KMeans().setK(kCount).setSeed(1L);
//		KMeansModel kMeansModel = kMeansForTops.fit(vectorDataTops);
//
//		Dataset<Row> kMeansPredictions = kMeansModel.transform(vectorDataTops);
//		System.out.println("====================== KMeans predictions  ===================");
//		kMeansPredictions.orderBy(kMeansPredictions.col("product_id")).show(false);
//
//		// Evaluate clustering by computing Silhouette score
//		ClusteringEvaluator topsEvaluator = new ClusteringEvaluator();
//
//		double TopsSilhouette = topsEvaluator.evaluate(kMeansPredictions);
//
//		System.out.println("================Silhouette with squared euclidean distance " + TopsSilhouette);
//
//		long countZero = kMeansPredictions.where(kMeansPredictions.col("prediction").equalTo(0)).count();
//		System.out.println("countZero " + countZero);
//
//		int i = 0;
//		int clusterNumber = 0;
//		long maxs = 0L;
//
//		for (; i < kCount; i++) {
//			System.out.println("CLuster count " + i);
//			long count = kMeansPredictions.where(kMeansPredictions.col("prediction").equalTo(i)).count();
//			System.out.println("Count " + count);
//
//			if (count > maxs) {
//				maxs = count;
//				clusterNumber = i;
//			}
//
//		}
//
//		System.out.println("Max cluster number " + clusterNumber);
//
//		// Show clusters
//
////		kMeansPredictions.collectAsList().forEach(prediction -> System.out.println(prediction));
//
//		System.out.println("=========== Range of Maximum cluster count ========");
//		kMeansPredictions.where(kMeansPredictions.col("prediction").equalTo(clusterNumber))
//				.agg(min("features"), max("features")).show();
//		;
//		// predictionsTops.where(predictionsTops("prediction") ===
//		// clusterNumber).agg(min("features"), max("features")).show()
//
//		System.out.println("Found the densest cluster, now showing the values - ");
//
//		kMeansPredictions.where(kMeansPredictions.col("prediction").equalTo(clusterNumber)).show();
//
//		// kMeansPredictions.where(predictionsTops("prediction") ===
//		// clusterNumber).agg(min("features"), max("features")).show()

		// generateCategegoryRangeReport();
		LogManager.getLogger("org").setLevel(Level.OFF);
		generateCategegoryRangeReport();
		// getCategoryPriceRange("BOTTOMS");

	}

	private static void generateCategegoryRangeReport() {
		String selectCategories = "select distinct PRIMARY_PRODUCT_CATEGORY_ID from PRODUCT where PRIMARY_PRODUCT_CATEGORY_ID != 'NULL'";
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;

		try {
			conn = DriverManager.getConnection(jdbcURL, DriverConfig.getDBUser(), DriverConfig.getDBPassword());
			stmt = conn.createStatement();
			rs = stmt.executeQuery(selectCategories);
			Dataset<Row> productCategoryPriceRange = null;
			boolean firstCat = true;
			while (rs.next()) {
				String categoryId = rs.getString("PRIMARY_PRODUCT_CATEGORY_ID");

				Dataset<Row> catDs = getCategoryPriceRange(categoryId);
				if (catDs != null) {
					if (firstCat) {
						productCategoryPriceRange = catDs;
						firstCat = false;
					} else {
						productCategoryPriceRange = productCategoryPriceRange.union(catDs);
					}
				}

			}
			System.out.println("Final ...");
			productCategoryPriceRange.show();
			// productCategoryPriceRange.coalesce(1).write().mode(SaveMode.Overwrite).format("csv").option("header",
			// "true")
			// .save(DriverConfig.getPredictionsDumpPath() + "/hc_category_range.csv");

			UDF1 udf = (Object s) -> {

				DenseVector v = (DenseVector) s;
				String vs = s.toString();
				vs = vs.replaceAll("[\\[\\]]", "");

				return vs;
			};

			UserDefinedFunction udfUppercase = udf(udf, DataTypes.StringType);

			productCategoryPriceRange = productCategoryPriceRange
					.withColumn("min_price", udfUppercase.apply(productCategoryPriceRange.col("min_price")))
					.withColumn("max_price", udfUppercase.apply(productCategoryPriceRange.col("max_price")));
			// .drop("MinPrice")
			// .drop("MaxPrice");

			productCategoryPriceRange.show(2000);

			System.out.println("JSON : " + productCategoryPriceRange.toJSON().toString());

			System.out.println(productCategoryPriceRange.toJSON().collect());

			String arr[] = (String[]) productCategoryPriceRange.toJSON().collect();

			StringJoiner joiner = new StringJoiner(",", "[", "]");
			for (String ar : arr) {
				System.out.println("arr " + ar);
				joiner.add(ar);
			}

			System.out.println("joiner " + joiner.toString());

			productCategoryPriceRange.coalesce(1).write().mode(SaveMode.Overwrite).format("csv")
					.option("header", "true").save(DriverConfig.getPredictionsDumpPath() + "/hc_category_range.csv");

			productCategoryPriceRange.coalesce(1).write().mode(SaveMode.Overwrite).format("json")
					.option("header", "true").save(DriverConfig.getPredictionsDumpPath() + "/hc_predictions.json");

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					System.err.println("Failed to close rs " + e.getMessage());
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					System.err.println("Failed to close stmt " + e.getMessage());
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					System.err.println("Failed to close conns " + e.getMessage());
				}
			}
		}
	}

	public static Dataset<Row> getCategoryPriceRange(String categoryId) {
		System.out.println("Thread Id - " + Thread.currentThread().getId());

		Dataset<Row> catDs = null;

		String categoryRangeSQL = "(select o.product_id, o.unit_price, p.PRIMARY_PRODUCT_CATEGORY_ID from order_item o inner join product p on p.PRIMARY_PRODUCT_CATEGORY_ID = '"
				+ categoryId
				+ "' and o.product_id = p.product_id and o.unit_price != '0.000' order by o.product_id) as CategoryRange";

		System.out.println("Running following query to fetch product prices for the category " + categoryRangeSQL);

		printConf();

		Dataset<Row> dataSetForkMeans = getSQLContext().read().jdbc(jdbcURL, categoryRangeSQL, sparkSQLProperties);
		long totalTransactionCount = dataSetForkMeans.count();
		System.out.println("dataSetForkMeans " + dataSetForkMeans.count());
		if (dataSetForkMeans.count() == 0) {
			return null;
		}
		System.out.println("=========== K-Means for Tops category ===========");

		VectorAssembler assemblerTops = new VectorAssembler().setInputCols(new String[] { "unit_price" })
				.setOutputCol("features");
		Dataset<Row> vectorDataTops = assemblerTops.transform(dataSetForkMeans).drop("unit_price");

		System.out.println("======================Tops data Min and Max ===================");
		vectorDataTops.agg(min("features"), max("features")).show();
		vectorDataTops.show(false);

		int kCount = 5;

		KMeans kMeansForTops = new KMeans().setK(kCount).setSeed(1L);
		KMeansModel kMeansModel = kMeansForTops.fit(vectorDataTops);

		Dataset<Row> kMeansPredictions = kMeansModel.transform(vectorDataTops);
		System.out.println("====================== KMeans predictions  ===================");
		kMeansPredictions.orderBy(kMeansPredictions.col("product_id")).show(false);

		int i = 0;
		int clusterNumber = 0;
		long maxs = 0L;

		for (; i < kCount; i++) {
			System.out.println("Cluster number in iterator " + i);
			long count = kMeansPredictions.where(kMeansPredictions.col("prediction").equalTo(i)).count();
			System.out.println("Count for  " + i + " " + count);

			if (count > maxs) {
				maxs = count;
				clusterNumber = i;
			}

		}

		System.out.println("Max cluster number " + clusterNumber);

		System.out.println("=========== Range of Maximum cluster count ========");
		kMeansPredictions.where(kMeansPredictions.col("prediction").equalTo(clusterNumber))
				.agg(min("features"), max("features")).show();

		System.out.println("ONly 2 ");
		kMeansPredictions.show(2);

		// kMeansPredictions.where(kMeansPredictions.col("prediction").equalTo(clusterNumber)).drop("product_id","features");

//		kMeansPredictions.where(kMeansPredictions.col("prediction").equalTo(clusterNumber))
//				.agg(lit(categoryId).as("Category"), min("features").as("Min Price"), max("features").as("Max Price"),
//						count("product_id").as("Transactions"), lit(totalTransactionCount).as("Total Transactions"))
//				.show();

//		System.out.println("Found the densest cluster, now showing the values - ");
//
//		kMeansPredictions.where(kMeansPredictions.col("prediction").equalTo(clusterNumber)).show();

		catDs = kMeansPredictions.where(kMeansPredictions.col("prediction").equalTo(clusterNumber)).agg(
				lit(categoryId).alias("category"), min("features").alias("min_price"),
				max("features").alias("max_price"), count("product_id").alias("transactions"),
				lit(totalTransactionCount).alias("total_transactions"),
				lit(bround(count("product_id").cast("integer").multiply(100).divide(totalTransactionCount), 2))
						.alias("prcentage"));
		catDs.show();

		UDF1 udf = (Object s) -> {

			DenseVector v = (DenseVector) s;
			String vs = s.toString();
			vs = vs.replaceAll("[\\[\\]]", "");

			return vs;
		};

		UserDefinedFunction udfUppercase = udf(udf, DataTypes.StringType);

		catDs = catDs.withColumn("min_price", udfUppercase.apply(catDs.col("min_price"))).withColumn("max_price",
				udfUppercase.apply(catDs.col("max_price")));

		
		return catDs;
		
	}

}
