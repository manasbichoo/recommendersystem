package co.hotwax.ml.recommendation.util;

import java.io.IOException;

import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.ml.util.MLWritable;

public class SparkUtil {

	public static void saveToFileSystem(MLWritable model, String path) throws IOException {
		long start;
		if (model instanceof FPGrowthModel) {
			start = System.currentTimeMillis();
			((FPGrowthModel) model).save(path);
			//((FPGrowthModel) model).write().overwrite().save(path);
			System.out.println(
					"Time to save model to file system - " + (System.currentTimeMillis() - start) / 1000 + " seconds");
		}

	}

	public static void saveToS3(MLWritable model, String path) throws IOException {
		long start;
		System.out.println("This is path - "+"s3a://hws-bucket/" + path);
		if (model instanceof FPGrowthModel) {
			start = System.currentTimeMillis();
			((FPGrowthModel) model).save("s3a://hws-bucket/" + path);
			//((FPGrowthModel) model).write().overwrite().save("s3a://hws-bucket/" + path);
			System.out
					.println("Time to save model to S3 - " + (System.currentTimeMillis() - start) / 1000 + " seconds");
		}

	}

	public static void saveToHDFS(MLWritable model, String path) throws IOException {
		long start;
		if (model instanceof FPGrowthModel) {
			start = System.currentTimeMillis();
			((FPGrowthModel) model).save("hdfs://0.0.0.0:9000/" + path);
			
			System.out.println(
					"Time to save model to HDFS - " + (System.currentTimeMillis() - start) / 1000 + " seconds");
		}
	}

}
