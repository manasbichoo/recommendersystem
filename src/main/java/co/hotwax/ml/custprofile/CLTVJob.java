package co.hotwax.ml.custprofile;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionSummary;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CLTVJob {

	public static void main(String[] args) {

		LogManager.getLogger("org").setLevel(Level.OFF);

		SparkSession spark = SparkSession.builder().appName("JavaLinearRegressionWithElasticNetExample")
				.master("local[*]").getOrCreate();

		VectorAssembler assembler = new VectorAssembler();
		assembler.setInputCols(new String[] { "MONTH_1", "MONTH_2", "MONTH_3", "MONTH_4", "MONTH_5", "MONTH_6" })
				.setOutputCol("features");

		Dataset<Row> training = spark.read().format("csv").option("header", "true").option("inferSchema", "true")
				.load("data/history.csv");
		training.show();
		training.printSchema();

		// org.apache.spark.mllib.stat.Statistics.corr(training.toJavaRDD());

		Dataset<Row> vectorDataTops = assembler.transform(training).drop("CUST_ID");
		vectorDataTops.show();

		LinearRegression lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
				.setFeaturesCol("features").setLabelCol("CLV");
		lr.setPredictionCol("predictioneeee");

		// Fit the model.
		LinearRegressionModel lrModel = lr.fit(vectorDataTops);

		// Print the coefficients and intercept for linear regression.
		System.out.println("Coefficients: " + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

		// Summarize the model over the training set and print out some metrics.
		LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
		System.out.println("numIterations: " + trainingSummary.totalIterations());
		System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
		trainingSummary.residuals().show();
		System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
		System.out.println("r2: " + trainingSummary.r2());
		// $example off$

		LinearRegressionSummary summary = lrModel.evaluate(vectorDataTops);
		System.out.println("RMSE, summary: " + summary.rootMeanSquaredError());
		System.out.println("r2, summary: " + summary.r2());

		lrModel.transform(vectorDataTops).show();

		spark.stop();
	}
}
