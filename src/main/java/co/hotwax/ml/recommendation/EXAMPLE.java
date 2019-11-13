package co.hotwax.ml.recommendation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;



public class EXAMPLE {

    public static class Rating implements Serializable {
        private String user;
        private int product;
        private int count;

        public Rating() {}

        public Rating(String user, int product, int count) {
            this.user = user;
            this.product = product;
            this.count = count;
        }

        public String getUser() {
            return user;
        }

        public int getProduct() {
            return product;
        }

        public int getCount() {
            return count;
        }

        public static Rating parseRating(String str) {
            String[] fields = str.split(",");
            if (fields.length != 3) {
                System.out.println("str "+str+"length "+fields.length);
                throw new IllegalArgumentException("Each line must contain 3 fields");
            }
            String user = fields[0];
            int product = Integer.parseInt(fields[1]);
            int count = Integer.parseInt(fields[2]);

            return new Rating(user, product, count);
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder().master("local[*]")
                .appName("ALS")
                .getOrCreate();

        JavaRDD<Rating> ratingsRDD = spark
                .read().textFile("/home/manas/Desktop/1.txt").javaRDD()
                .map(Rating::parseRating);
        Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);


        StringIndexer model1 = new StringIndexer()
                .setInputCol("user")
                .setOutputCol("userNumber");
        Dataset<Row> indexed = model1.fit(ratings).transform(ratings);
        indexed.show();
         Dataset<Row>[] splits = indexed.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];
        // Build the recommendation model using ALS on the training data
        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("userNumber")
                .setItemCol("product")
                .setRatingCol("count");
        ALSModel model = als.fit(training);

        // Evaluate the model by computing the RMSE on the test data
        // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
        model.setColdStartStrategy("drop");
        Dataset<Row> predictions = model.transform(test);

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("count")
                .setPredictionCol("prediction");
        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root-mean-square error = " + rmse);

        // Generate top 10 recommendations for each user
        Dataset<Row> userRecs = model.recommendForAllUsers(10);
        // Generate top 10 user recommendations for each product
        Dataset<Row> productRecs = model.recommendForAllItems(10);

        // Generate top 10 product recommendations for a specified set of users
        Dataset<Row> users = indexed.select(als.getUserCol()).distinct().limit(3);
        Dataset<Row> userSubsetRecs = model.recommendForUserSubset(users, 10);
        // Generate top 10 user recommendations for a specified set of products
        Dataset<Row> products = indexed.select(als.getItemCol()).distinct().limit(3);
        Dataset<Row> productsSubSetRecs = model.recommendForItemSubset(products, 10);
        // $example off$
        userRecs.show();
        productRecs.show();
        userSubsetRecs.show();
        productsSubSetRecs.show();

        spark.stop();
    }
}


