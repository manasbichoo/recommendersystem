package co.hc.ml.recommendation.launcher;

import java.io.IOException;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

public class RecommendationLauncher {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		SparkAppHandle handle = new SparkLauncher()
				.setAppResource("/Users/grv/WS/HWC/ProductRecommendations/build/libs/ProductRecommendations.jar").addJar("")
				.setMainClass("co.hotwax.ml.recommendation.DatabricksFPGrowthTest").setMaster("local")
				.setConf(SparkLauncher.DRIVER_MEMORY, "2g").setSparkHome("/usr/local/spark").startApplication();
		while (true) {
			Thread.sleep(5000);
			System.out.println("State : " + handle.getState());
		}

	}

}
