package co.hotwax.ml.clustering.util;

import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.github.sh0nk.matplotlib4j.Plot;
import com.github.sh0nk.matplotlib4j.PythonExecutionException;

public class ClusteringUtils {

	public static int optimalK(Dataset<Row> vectorData) {

		// We'll start with Silhouette score first

		HashMap<Double, Integer> scoreKMap = new HashMap<Double, Integer>();
		double maxSilhouetteScore = -1;
		int optimalK = 0;

		for (int i = 2; i < 10; i++) {

			KMeans kMeans = new KMeans().setK(i).setSeed(1L).setPredictionCol("prediction");
			KMeansModel kMeansModel = kMeans.fit(vectorData);
			Dataset<Row> kMeansPredictions = kMeansModel.transform(vectorData);
			int maxAssignedClusterNumber = kMeansPredictions.agg(min("prediction"), max("prediction")).head().getInt(1);
			int minAssignedClusterNumber = kMeansPredictions.agg(min("prediction"), max("prediction")).head().getInt(0);
			if (maxAssignedClusterNumber == minAssignedClusterNumber) {
				optimalK = 1;
				break;
			}

			ClusteringEvaluator topsEvaluator = new ClusteringEvaluator();
			double silhouetteScore = topsEvaluator.evaluate(kMeansPredictions);
			scoreKMap.put(silhouetteScore, i);
			if (silhouetteScore > maxSilhouetteScore) {
				maxSilhouetteScore = silhouetteScore;
				optimalK = i;
			}

			System.out.println("Silhouette score when K = " + i + " is " + silhouetteScore);
		}

		System.out.println("Optimal K turned out to be " + optimalK);
		return optimalK;

	}

	public static int getOptimalK(Dataset<Row> vectorData) {
		// Integer K[] = new Integer[9];
		// Double computeCost[] = new Double[9];
		List<Integer> K = new ArrayList<Integer>();
		List<Double> computeCost = new ArrayList<Double>();

		for (int i = 2; i < 10; i++) {

			KMeans kMeansForTops = new KMeans().setK(i).setSeed(1L);
			long start = System.currentTimeMillis();
			KMeansModel kMeansModel = kMeansForTops.fit(vectorData);
			System.out.println("Time to FIT " + (System.currentTimeMillis() - start) / 1000);
			double cost = kMeansModel.computeCost(vectorData); // WCSS
			// KMeansSummary
			K.add(i);
			computeCost.add(cost);
			// System.out.println("================ Compite cost for K == " + i + " is == "
			// + cost);

			Dataset<Row> kMeansPredictions = kMeansModel.transform(vectorData);

			for (int j = 0; j < i; j++) {

				// System.out.println("====================== Running it for " + j + "th cluster
				// when the K is " + i);
				// System.out.println("====================== Range for the cluster count " + i
				// + " ===================");
				// kMeansPredictions.where(kMeansPredictions.col("prediction").equalTo(j)).agg(min("features"),
				// max("features")).show();

			}

			// kMeansPredictions.agg(min("features"), max("features")).show();
			// System.out.println("====================== Calculating Silhouette
			// ===================");

			// Evaluate clustering by computing Silhouette score
			ClusteringEvaluator topsEvaluator = new ClusteringEvaluator();

			double TopsSilhouette = topsEvaluator.evaluate(kMeansPredictions);

			// System.out.println("================ Silhouette score for K == " + i + " is
			// == " + TopsSilhouette);

		}

		Plot plt = Plot.create();
		// plt.plot().add(K, computeCost).label("MyLabel").linestyle("bx-");
		// plt.plot().

		System.out.println("K List " + K);
		System.out.println("Cost  List " + computeCost);
		plt.plot().add(K, computeCost).label("Elbow").linestyle("--");
		plt.xlabel("K");
		plt.ylabel("Cost");
		plt.title("Compute Cost for K-Means !");
		plt.legend();
		try {
			plt.show();
		} catch (IOException | PythonExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;

	}

	public static void showDensestClusterPlot() {

	}

}
