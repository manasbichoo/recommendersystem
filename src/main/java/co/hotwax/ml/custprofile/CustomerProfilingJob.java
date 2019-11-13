package co.hotwax.ml.custprofile;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;

public class CustomerProfilingJob {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		LogManager.getLogger("org").setLevel(Level.OFF);
		SparkConf sparkConf = new SparkConf().setAppName("WebStore India App").setMaster("local[2]")
				.set("spark.executor.memory", "1g").set("spark.sql.crossJoin.enabled", "true");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sparkContext);

		sparkContext.setLogLevel("ERROR");

		Dataset<Row> userVisites = sqlContext.read().format("jdbc")
				.option("url", "jdbc:mysql://localhost:3306/ofbizstats").option("driver", "com.mysql.jdbc.Driver")
				.option("dbtable", "visit").option("user", "grv").option("password", "gurudutt").load();

		Dataset<Row> userAgent = sqlContext.read().format("jdbc")
				.option("url", "jdbc:mysql://localhost:3306/ofbizstats").option("dbtable", "user_agent")
				.option("user", "grv").option("password", "gurudutt").load()
				.select("USER_AGENT_ID", "PLATFORM_TYPE_ID");

		Dataset<Row> userPlatformType = sqlContext.read().format("jdbc")
				.option("url", "jdbc:mysql://localhost:3306/ofbizstats").option("dbtable", "platform_type")
				.option("user", "grv").option("password", "gurudutt").load();

		Dataset<Row> serverHitData = sqlContext.read().format("jdbc")
				.option("url", "jdbc:mysql://localhost:3306/ofbizstats").option("dbtable", "server_hit")
				.option("user", "grv").option("password", "gurudutt").load()
				.select("PRODUCT_ID", "VISIT_ID", "PRODUCT_CATEGORY_ID");

		// System.out.println(userVisites.count());

		Dataset<Row> filteredUsers = userVisites.where(userVisites.col("USER_LOGIN_ID").isNotNull())
				.where("WEBAPP_NAME = 'WEBSTORE'").select("VISIT_ID", "VISITOR_ID", "WEBAPP_NAME", "INITIAL_USER_AGENT",
						"USER_AGENT_ID", "USER_LOGIN_ID");

		// System.out.println(filteredUsers.count() + " : Filtered Count");
		// filteredUsers.select("USER_LOGIN_ID").distinct().show();

		// ************ platform of particular user *****************
		Dataset<Row> userVisitAndAgent = filteredUsers
				.join(userAgent, filteredUsers.col("USER_AGENT_ID").equalTo(filteredUsers.col("USER_AGENT_ID")))
				.drop(filteredUsers.col("USER_AGENT_ID"));

		Dataset<Row> userVisitAgentAndPlatform = userVisitAndAgent
				.join(userPlatformType,
						userVisitAndAgent.col("PLATFORM_TYPE_ID").equalTo(userPlatformType.col("PLATFORM_TYPE_ID")))
				.drop(userVisitAndAgent.col("PLATFORM_TYPE_ID"));

		Dataset<Row> platformOfVisitedUsers = userVisitAgentAndPlatform.groupBy("VISIT_ID", "PLATFORM_NAME")
				.agg(functions.count("PLATFORM_NAME"));

		// final user_id with platforms and their counts
		platformOfVisitedUsers
				.join(filteredUsers, platformOfVisitedUsers.col("VISIT_ID").equalTo(filteredUsers.col("VISIT_ID")))
				.drop(platformOfVisitedUsers.col("VISIT_ID"))
				.select("VISIT_ID", "USER_LOGIN_ID", "PLATFORM_NAME", "count(PLATFORM_NAME)").show(20);

		// ************ categories of particular user visited **************

		Dataset<Row> newServerHitData = serverHitData.where(serverHitData.col("VISIT_ID").isNotNull());

		// newServerHitData.show();

		Dataset<Row> newFilteredUsers = filteredUsers.select("VISIT_ID", "USER_LOGIN_ID").distinct();

		Dataset<Row> userWithProducts = newFilteredUsers
				.join(newServerHitData, newFilteredUsers.col("VISIT_ID").equalTo(newServerHitData.col("VISIT_ID")))
				.drop(newFilteredUsers.col("VISIT_ID"));

		// userWithProducts.show(20);

		// ********** Category Transactions ***********
		Dataset<Row> userCategoryTransactions = userWithProducts.groupBy("VISIT_ID", "PRODUCT_CATEGORY_ID")
				.agg(functions.count(userWithProducts.col("PRODUCT_CATEGORY_ID").as("CATEGORY_COUNT")));

		// VISIT ids with PRODUCT_ID and its count
		System.out.println("===================VISIT ids with PRODUCT_CATEGORY_ID and its count ===================");

		// Not null PRODUCT_CATEGORY_ID
		// count -> 148
		userCategoryTransactions.where(userCategoryTransactions.col("PRODUCT_CATEGORY_ID").isNotNull()).show();

		// ********** Product Transactions ************
		Dataset<Row> userProductTransactions = userWithProducts.groupBy("VISIT_ID", "PRODUCT_ID")
				.agg(functions.count(userWithProducts.col("PRODUCT_ID").as("CATEGORY_COUNT")));

		userProductTransactions.where(userProductTransactions.col("PRODUCT_ID").isNotNull()).show();

	}

}
