package co.hotwax.ml.custprofile;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class ShopperChallenge {

	public SparkContext sc;
	public SQLContext sqlContext;
	public SparkSession spark;
	String dataPath = "data/";
	String exdataPath = "/Users/grv/Downloads/";
	public Dataset<Row> trainHistory, offers, sampleSubmission, chain, offer, market, repeattrips, offerdate,
			allFeatures;

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		ShopperChallenge.getInstance().init();
	}

	public void init() {
		LogManager.getLogger("org").setLevel(Level.OFF);

		spark = SparkSession.builder().appName("Clustering_POC").master("local[*]").getOrCreate();

		sc = spark.sparkContext();
		sqlContext = spark.sqlContext();

		loadData();
		// clusterChain();
		// clusterOffer();
		// clusterMarket();
		// clusterTrips();
		// clusterOfferDate();
		clusterAll();
	}

	public static ShopperChallenge getInstance() {
		return new ShopperChallenge();
	}

	private void loadData() {
		trainHistory = spark.read().format("csv").option("header", "true").option("inferSchema", "true")
				.load(exdataPath + "trainHistory.csv.gz");
		// trainHistory.show(20, false);
		System.out.println("Training data count : " + trainHistory.count());

		// offers = spark.read().format("csv").option("header",
		// "true").option("inferSchema", "true").load(exdataPath + "offers.csv.gz");
		// offers.show(20, false);

		// sampleSubmission = spark.read().format("csv").option("header",
		// "true").option("inferSchema", "true").load(exdataPath +
		// "sampleSubmission.csv.gz");
		// sampleSubmission.show(20, false);

		// Dataset<Row> transactions = spark.read().format("csv").option("header",
		// "true").option("inferSchema", "true").load(exdataPath +
		// "transactions.csv.gz");
		// transactions.show(20, false);

	}

	private void clusterChain() {
		chain = trainHistory.select("id", "chain").na().drop();
		// chain.show(20);

		int k = 6;
		int iterations = 10;

		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] { "chain" }).setOutputCol("features");
		Dataset finalChain = vectorAssembler.transform(chain);

		KMeansModel kMeansModel = new KMeans().setK(k).setMaxIter(iterations).setSeed(1L).fit(finalChain);

		Dataset<Row> predictions = kMeansModel.transform(finalChain);
		predictions.show(false);
	}

	private void clusterOffer() {
		offer = trainHistory.select("id", "offer").na().drop();
		// offer.show(20);

		int k = 6;
		int iterations = 10;

		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] { "offer" }).setOutputCol("features");
		Dataset finalOffer = vectorAssembler.transform(offer);

		KMeansModel kMeansModel = new KMeans().setK(k).setMaxIter(iterations).setSeed(1L).fit(finalOffer);

		Dataset<Row> predictions = kMeansModel.transform(finalOffer);
		predictions.show(false);
	}

	private void clusterMarket() {
		market = trainHistory.select("id", "market").na().drop();
		// market.show(20);

		int k = 6;
		int iterations = 10;

		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] { "market" }).setOutputCol("features");
		Dataset finalMarket = vectorAssembler.transform(market);

		KMeansModel kMeansModel = new KMeans().setK(k).setMaxIter(iterations).setSeed(1L).fit(finalMarket);

		Dataset<Row> predictions = kMeansModel.transform(finalMarket);
		predictions.show(false);
	}

	private void clusterTrips() {
		repeattrips = trainHistory.select("id", "repeattrips").na().drop();
		// market.show(20);

		int k = 6;
		int iterations = 10;

		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] { "repeattrips" }).setOutputCol("features");
		Dataset finalRepeatTrips = vectorAssembler.transform(repeattrips);

		KMeansModel kMeansModel = new KMeans().setK(k).setMaxIter(iterations).setSeed(1L).fit(finalRepeatTrips);

		Dataset<Row> predictions = kMeansModel.transform(finalRepeatTrips);
		predictions.show(false);
	}

	private void clusterOfferDate() {
		offerdate = trainHistory.withColumn("newOfferDate", trainHistory.col("offerdate").cast("String"))
				.select("id", "newOfferDate").na().drop();
		// offerdate.show(20);

		int k = 6;
		int iterations = 10;

		Dataset<Row> indexedOfferDate = new StringIndexer().setInputCol("newOfferDate").setOutputCol("newOfferDates")
				.fit(offerdate).transform(offerdate);

		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] { "newOfferDates" }).setOutputCol("features");
		Dataset<Row> finalOfferDate = vectorAssembler.transform(indexedOfferDate);

		KMeansModel kMeansModel = new KMeans().setK(k).setMaxIter(iterations).setSeed(1L).fit(finalOfferDate);

		Dataset<Row> predictions = kMeansModel.transform(finalOfferDate);
		predictions.show(false);
	}

	private void clusterAll() {
		int k = 6;
		int itr = 10;
		allFeatures = trainHistory.withColumn("newOfferDate", trainHistory.col("offerdate").cast("String"))
				.select("id", "chain", "offer", "market", "repeattrips", "newOfferDate").na().drop();

		Dataset<Row> indexedData = new StringIndexer().setInputCol("newOfferDate").setOutputCol("newOfferDates")
				.fit(allFeatures).transform(allFeatures);

		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] { "chain", "offer", "market", "repeattrips", "newOfferDates" })
				.setOutputCol("features");
		Dataset<Row> finalData = vectorAssembler.transform(indexedData);

		KMeansModel kMeansModel = new KMeans().setK(k).setMaxIter(itr).setSeed(1L).fit(finalData);

		Dataset<Row> predictions = kMeansModel.transform(finalData);
		predictions.show(false);
	}

}
