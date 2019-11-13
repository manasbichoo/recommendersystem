package co.hotwax.ml.spark.job;

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import com.hc.spark.util.DriverConstants;

import co.hotwax.kafka.KafkaMessage;
import co.hotwax.kafka.producer.KafkaMessageProducer;
import co.hotwax.message.GenericMessage;
import co.hotwax.message.MessageProducer;
import co.hotwax.ml.recommendation.DriverConfig;

/**
 * The class represents spark job to determine the densest cluster for each
 * product category. Furthermore, it calculates the maximum and minimum price
 * range for each product category. Eventually it stores the price range for the
 * categories in respective Kafka topics dedicated for each category.
 * 
 * @author grv
 */
public class ProductCategoryClusteringJob {

	private static final Logger LOGGER = LogManager.getLogger(ProductCategoryClusteringJob.class.getName());

	/*
	 * Spark session to be used for a particular run.
	 */
	private static SparkSession session;

	/*
	 * SQL Context to be used for a particular session.
	 */
	private static SQLContext sqlContext;

	private static String jdbcURL = "jdbc:mysql://" + DriverConfig.getDBHost() + "/" + DriverConfig.getDBName()
			+ "?autoReconnect=true&amp;useSSL=false&amp;characterEncoding=UTF-8";
	private static String CATEGORY = "TOPS";

	private static final String categoryRangeSQL = "(select o.product_id, o.unit_price, p.PRIMARY_PRODUCT_CATEGORY_ID from order_item o inner join product p on p.PRIMARY_PRODUCT_CATEGORY_ID = '"
			+ CATEGORY
			+ "' and o.product_id = p.product_id and o.unit_price != '0.000' order by o.product_id) as CategoryRange";
	private static final String productPriceSQL = "(select product_id, unit_price from order_item where unit_price != '0.000') AS ProductPrice";

	/*
	 * Properties reference to feed to Spark SQL context
	 */
	private static Properties sparkSQLProperties;

	static {
		sparkSQLProperties = new Properties();
		sparkSQLProperties.setProperty(DriverConstants.SPARK_SQL_DRIVER, DriverConfig.getDBDriver());
		sparkSQLProperties.setProperty(DriverConstants.SPARK_SQL_USER, DriverConfig.getDBUser());
		sparkSQLProperties.setProperty(DriverConstants.SPARK_SQL_URL, jdbcURL);
		sparkSQLProperties.setProperty(DriverConstants.SPARK_SQL_PASSWORD, DriverConfig.getDBPassword());

	}

	/**
	 * Get the spark session for a particular run.
	 * 
	 * @return
	 */
	private static SparkSession getSparkSession() {
		if (session == null) {
			session = SparkSession.builder().appName(ProductCategoryClusteringJob.class.getName()).getOrCreate();

		}
		System.out.println("Spark session - " + session);
		return session;

	}

	/**
	 * Gets SQLContext for the session.
	 * 
	 * @return
	 */
	private static SQLContext getSQLContext() {
		if (sqlContext == null) {
			sqlContext = new SQLContext(getSparkSession());
		}
		System.out.println("SQL context - " + sqlContext);
		return sqlContext;
	}

	/**
	 * Prints the spark configuration for this session.
	 */
	private static void printConf() {
		System.out.println("Using conf  - " + getSparkSession().conf().getAll());
	}

	/**
	 * spark-submit will invoke main method of this job
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		LOGGER.debug("Debug Message Logged !!!");
		LOGGER.info("Info Message Logged !!!");
		LOGGER.error("Error Message Logged !!!", new NullPointerException("NullError"));
		runJob(args);

	}

	private static void runJob(String[] args) {
		// Get logger for spark packages and turn it off.
		org.apache.log4j.LogManager.getLogger("org").setLevel(Level.OFF);
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
					MessageProducer producer = KafkaMessageProducer.getInstance();
					String arr[] = (String[]) catDs.toJSON().collect();
					System.out.println("Category JSON " + arr[0]);
					KafkaMessage message = new KafkaMessage();
					message.setKey(categoryId);
					message.setValue(arr[0]);
					message.setToTopic(categoryId + "_kmeans");
					producer.produce(message);

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

			UDF1<?, ?> udf = (Object s) -> {
				String vs = s.toString();
				vs = vs.replaceAll("[\\[\\]]", "");

				return vs;
			};

			UserDefinedFunction udfUppercase = udf(udf, DataTypes.StringType);

			productCategoryPriceRange = productCategoryPriceRange
					.withColumn("min_price", udfUppercase.apply(productCategoryPriceRange.col("min_price")))
					.withColumn("max_price", udfUppercase.apply(productCategoryPriceRange.col("max_price")));

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

	private static Dataset<Row> getCategoryPriceRange(String categoryId) {
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

		kMeansPredictions.show(2);

		catDs = kMeansPredictions.where(kMeansPredictions.col("prediction").equalTo(clusterNumber)).agg(
				lit(categoryId).alias("category"), min("features").alias("min_price"),
				max("features").alias("max_price"), count("product_id").alias("transactions"),
				lit(totalTransactionCount).alias("total_transactions"),
				lit(bround(count("product_id").cast("integer").multiply(100).divide(totalTransactionCount), 2))
						.alias("percentage"));
		catDs.show();

		UDF1<?, ?> udf = (Object s) -> {

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
