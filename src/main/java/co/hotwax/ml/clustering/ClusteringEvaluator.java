package co.hotwax.ml.clustering;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hc.spark.util.DriverConstants;

import co.hotwax.app.config.ApplicationCatalog;
import co.hotwax.app.config.auth.AuthConfig;
import co.hotwax.app.config.auth.DBConfig;
import co.hotwax.app.config.ml.ClusteringConfig;
import co.hotwax.app.config.ml.MLConfig;
import co.hotwax.app.config.ml.ProductCategory;
import co.hotwax.app.config.ml.ProductCategoryConfig;
import co.hotwax.ml.recommendation.DriverConfig;

/**
 * This class is to be used to generate optimal number of K for K-Means
 * algorithm. Initially it is supposed to be reading all Primary Product
 * Categories from OFBiz DB and generate price data set for all the products
 * falling under the category and then evaluate Silhouette or Elbow score for
 * the data set. This score will be used to determine optimal K value and the K
 * value will be stored for the corresponding category in a KV store.
 * 
 * @author grv
 *
 */
public class ClusteringEvaluator {

	private static String jdbcURL = "jdbc:mysql://" + DriverConfig.getDBHost() + "/" + DriverConfig.getDBName()
			+ "?autoReconnect=true&amp;useSSL=false&amp;characterEncoding=UTF-8";
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
		LogManager.getLogger("org").setLevel(Level.OFF);
		String selectCategories = "select distinct PRIMARY_PRODUCT_CATEGORY_ID from PRODUCT where PRIMARY_PRODUCT_CATEGORY_ID != 'NULL'";
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;

		try {
			conn = DriverManager.getConnection(jdbcURL, DriverConfig.getDBUser(), DriverConfig.getDBPassword());
			stmt = conn.createStatement();
			rs = stmt.executeQuery(selectCategories);

			while (rs.next()) {
				String categoryId = rs.getString("PRIMARY_PRODUCT_CATEGORY_ID");
				try {
					evaluateK(categoryId);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

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

	/**
	 * Evaluates K for the product category
	 * 
	 * @param category
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	private static void evaluateK(String category) throws JsonParseException, JsonMappingException, IOException {
		Integer k = ProductCategoryClustering.K(category);
		if (k != null) {
			saveK(category, k);
			System.out.println("Optimal K for Category " + category + " = " + k);
		}

	}

	/**
	 * Saves the optimal K for the category in the application configuration YAML file.
	 * @param category
	 * @param k
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	private static void saveK(String category, Integer k) throws JsonParseException, JsonMappingException, IOException {

		ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
		yamlMapper.configure(SerializationFeature.WRAP_ROOT_VALUE, true);
		yamlMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

		yamlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		yamlMapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);

		ApplicationCatalog app = yamlMapper.readValue(new File("application.yaml"), ApplicationCatalog.class);

		AuthConfig authConfig = app.getAuthentication() != null ? app.getAuthentication() : new AuthConfig();

		DBConfig dbConf = authConfig.getDbConfig() != null ? authConfig.getDbConfig() : new DBConfig();
		dbConf.setName("ofbiz");
		dbConf.setPassword("gurudutt");
		String url = "jdbc:mysql://" + DriverConfig.getDBHost() + "/" + DriverConfig.getDBName()
				+ "?autoReconnect=true&amp;useSSL=false&amp;characterEncoding=UTF-8";
		dbConf.setUrl(url);
		dbConf.setHost("localhost");
		dbConf.setUser("grv");

		authConfig.setDbConfig(dbConf);
		app.setAuthentication(authConfig);

		MLConfig mlConf = app.getMlConfig() != null ? app.getMlConfig() : new MLConfig();
		app.setMlConfig(mlConf);

		ClusteringConfig clusterConf = mlConf.getClusterConfig() != null ? mlConf.getClusterConfig()
				: new ClusteringConfig();
		mlConf.setClusterConfig(clusterConf);

		ProductCategoryConfig pConf = clusterConf.getProductCategoryConfig() != null
				? clusterConf.getProductCategoryConfig()
				: new ProductCategoryConfig();
		ProductCategory cat = new ProductCategory();
		cat.setCategoryName(category);
		cat.setOptimalK(k);
		pConf.getProductCategories().add(cat);

		clusterConf.setProductCategoryConfig(pConf);

		try {
			yamlMapper.writeValue(new File("application.yaml"), app);
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
