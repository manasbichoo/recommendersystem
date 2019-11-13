package co.hotwax.ml.recommendation;

import static org.apache.spark.sql.functions.collect_set;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.hc.spark.util.DriverConstants;

import scala.collection.mutable.WrappedArray;

public class HCRecommendationJob {

	private static SparkSession session;
	private static SQLContext sqlContext;
	private static String jdbcURL = "jdbc:mysql://" + DriverConfig.getDBHost() + "/" + DriverConfig.getDBName()
			+ "?autoReconnect=true&amp;useSSL=false&amp;characterEncoding=UTF-8";

	private static String user = DriverConfig.getDBUser();
	private static String password = DriverConfig.getDBPassword();
	private static final boolean updateDB = false;

	private static FPGrowthModel model;
	private static Properties sparkSQLProperties;
	static {
		sparkSQLProperties = new Properties();
		sparkSQLProperties.setProperty(DriverConstants.SPARK_SQL_DRIVER, DriverConfig.getDBDriver());
		sparkSQLProperties.setProperty(DriverConstants.SPARK_SQL_USER, DriverConfig.getDBUser());
		sparkSQLProperties.setProperty(DriverConstants.SPARK_SQL_URL, jdbcURL);
		sparkSQLProperties.setProperty(DriverConstants.SPARK_SQL_PASSWORD, DriverConfig.getDBPassword());

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long start = System.currentTimeMillis();
		System.out.println();
		LogManager.getLogger("org").setLevel(Level.OFF);

		File dumpPath = new File(DriverConfig.getPredictionsDumpPath());
		dumpPath.setReadable(true, false);
		dumpPath.setWritable(true, false);

		if (!dumpPath.exists()) {
			dumpPath.mkdirs();
		}

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

		 session = SparkSession.builder().appName("RecommendationJob").getOrCreate();

		sqlContext = new SQLContext(session);

		System.out.println("SparkContext created successfully ...");

		printConf();

		Dataset<Row> dataSet = sqlContext.read().jdbc(jdbcURL, "order_item", sparkSQLProperties);

		dataSet.createOrReplaceTempView("orderItem");

		Dataset<Row> rawData = session.sql("select order_id, product_id from orderItem");

		System.out.println("OrderItem count " + rawData.count());

		Dataset<Row> baskets = rawData.groupBy("order_id").agg(collect_set("product_id").alias("items"));
		baskets.createOrReplaceTempView("baskets");

		Dataset<Row> baskets_ds = session.sql("select items from baskets").as("Array[String]").toDF("items");

		FPGrowth fpGrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.001).setMinConfidence(0)
				.setNumPartitions(10);

		trainModel(fpGrowth, baskets_ds);

		showFrequentItemSets();

		showAssociationRules();

		System.out.println("Transforming ...");

		Dataset<Row> predictions = transformAndPredict();
		exportPredictionsToJSON(predictions);
		// exportPredictionsToCSV(predictionsRows);
		try {
			exportProductAssocForOFBizConsumption(predictions);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			session.close();
		}

		session.close();
		System.out.println("Recommendation engine ran successfully, exiting now !! "
				+ (System.currentTimeMillis() - start) / 1000 / 60);

	}

	private static void printConf() {
		System.out.println("Using conf  - " + session.conf().getAll());
	}

	private static void trainModel(FPGrowth fpGrowth, Dataset<Row> baskets_ds) {
		long start = System.currentTimeMillis();
		model = fpGrowth.fit(baskets_ds);
		System.out.println("Time to train model " + (System.currentTimeMillis() - start) / 1000 + " seconds ");
	}

	private static void showFrequentItemSets() {
		System.out.println("Top-10 frequent itemsets ...");
		model.freqItemsets().show(10, false);

	}

	private static void showAssociationRules() {
		System.out.println("Top-10 associationRules sets ...");
		model.associationRules().show(10, false);
	}

	private static Dataset<Row> transformAndPredict() {

		StructType schema = new StructType(new StructField[] {
				new StructField("items", new ArrayType(DataTypes.StringType, true), false, Metadata.empty()) });

		Dataset<Row> productSet = sqlContext.read().jdbc(jdbcURL, "product", sparkSQLProperties);
		productSet.createOrReplaceTempView("product_view");

		Dataset<Row> productViewSet = session.sql("select product_id from product_view");
		List<Row> rowData = productViewSet.collectAsList();
		List<Row> data = new ArrayList<>();
		rowData.forEach(row -> {
			String productId = (String) row.get(0);
			data.add(RowFactory.create(Arrays.asList(productId)));
		});

		Dataset<Row> itemsDF = session.createDataFrame(data, schema);
		long start = System.currentTimeMillis();
		Dataset<Row> predictions = model.transform(itemsDF);
		System.out.println("Time to make predictions " + (System.currentTimeMillis() - start) / 1000 + " seconds ");

		return predictions;
	}

	private static void exportPredictionsToJSON(Dataset<Row> predictionsRows) {

		predictionsRows.filter((row) -> {
			// System.out.println("Row length filter " + row.length());
			String recommendations = ((WrappedArray) row.get(1)).mkString(",");
			return recommendations.length() > 0;

		}).coalesce(1).write().mode(SaveMode.Overwrite).format("json").option("header", "true")
				.save(DriverConfig.getPredictionsDumpPath() + "/hc_predictions.json");

	}

	private static void exportPredictionsToCSV(Dataset<Row> predictionsRows) throws SQLException {

		List<Row> predictionsList = predictionsRows.collectAsList();
		List<Row> productAssoc = new ArrayList<Row>();

		StructField[] structFields = new StructField[] {
				new StructField("product_id", DataTypes.StringType, true, Metadata.empty()),
				new StructField("recommended_product_id", DataTypes.StringType, true, Metadata.empty()) };

		StructType rootSchema = new StructType(structFields);

		Connection conn = null;
		Statement stmt = null;
		conn = DriverManager.getConnection(jdbcURL, user, password);
		stmt = conn.createStatement();

		for (Row row : predictionsList) {

			String product_id = ((WrappedArray) row.get(0)).mkString();
			String recommendations = ((WrappedArray) row.get(1)).mkString(",");
			Timestamp createTs = new java.sql.Timestamp(System.currentTimeMillis());

			if (recommendations.length() != 0) {
				for (String recommended_product : recommendations.split(",")) {

					if (updateDB) {
						String sql = "INSERT INTO product_assoc (PRODUCT_ID,PRODUCT_ID_TO,PRODUCT_ASSOC_TYPE_ID,FROM_DATE, LAST_UPDATED_STAMP, LAST_UPDATED_TX_STAMP, CREATED_STAMP, CREATED_TX_STAMP, REASON) "
								+ "VALUES ('" + product_id + "','" + recommended_product + "','ALSO_BOUGHT','"
								+ createTs + "','" + createTs + "','" + createTs + "','" + createTs + "','" + createTs
								+ "', 'Product Association Recommendation');";

						System.out.println("SQL - " + sql);
						stmt.executeUpdate(sql);
					}

					productAssoc.add(RowFactory.create(product_id, recommended_product));

				}
			}

		}

		Dataset<Row> productAssocDataset = session.createDataFrame(productAssoc, rootSchema);
		productAssocDataset.coalesce(1).write().mode(SaveMode.Overwrite).format("csv").option("header", "true")
				.save("model/hc_predictions.csv");

	}

	private static void exportProductAssocForOFBizConsumption(Dataset<Row> predictionsRows) throws SQLException {

		List<Row> predictionsList = predictionsRows.collectAsList();
		List<Row> productAssoc = new ArrayList<Row>();

		StructField[] structFields = new StructField[] {
				new StructField("product-id", DataTypes.StringType, true, Metadata.empty()),
				new StructField("product-id-to", DataTypes.StringType, true, Metadata.empty()),
				new StructField("product-assoc-type-id", DataTypes.StringType, true, Metadata.empty()),
				new StructField("start-date", DataTypes.StringType, true, Metadata.empty()) };

		StructType rootSchema = new StructType(structFields);

		Connection conn = null;
		Statement stmt = null;
		conn = DriverManager.getConnection(jdbcURL, user, password);
		stmt = conn.createStatement();
		String currentDate = new SimpleDateFormat("MM/dd/yyyy").format(new Date());

		for (Row row : predictionsList) {

			String product_id = ((WrappedArray) row.get(0)).mkString();
			String recommendations = ((WrappedArray) row.get(1)).mkString(",");
			Timestamp createTs = new java.sql.Timestamp(System.currentTimeMillis());

			if (recommendations.length() != 0) {
				for (String recommended_product : recommendations.split(",")) {

					if (updateDB) {
						String sql = "INSERT INTO product_assoc (PRODUCT_ID,PRODUCT_ID_TO,PRODUCT_ASSOC_TYPE_ID,FROM_DATE, LAST_UPDATED_STAMP, LAST_UPDATED_TX_STAMP, CREATED_STAMP, CREATED_TX_STAMP, REASON) "
								+ "VALUES ('" + product_id + "','" + recommended_product + "','ALSO_BOUGHT','"
								+ createTs + "','" + createTs + "','" + createTs + "','" + createTs + "','" + createTs
								+ "', 'Product Association Recommendation');";

						System.out.println("SQL - " + sql);
						stmt.executeUpdate(sql);
					}

					productAssoc.add(RowFactory.create(product_id, recommended_product, "ALSO_BOUGHT", currentDate));

				}
			}

		}

		Dataset<Row> productAssocDataset = session.createDataFrame(productAssoc, rootSchema);

		productAssocDataset.coalesce(1).write().mode(SaveMode.Overwrite).format("csv").option("header", "true")
				.save(DriverConfig.getPredictionsDumpPath() + "/hc_predictions.csv");

	}

}
