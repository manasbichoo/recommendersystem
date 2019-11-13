#!/usr/bin/env bash

echo $SPARK_HOME

/usr/local/spark/bin/spark-submit \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///home/manas/sandbox/recommendation-engine/src/main/resources/executor_log4j.properties" \
  --driver-java-options "-Dlog4j.configuration=file:///home/manas/sandbox/recommendation-engine/src/main/resources/driver_log4j.properties" \
  --class co.hotwax.ml.recommendation.HCRecommendationJob \
  --master local[*] \
  --deploy-mode client \
  --jars /home/manas/sandbox/recommendation-engine/runtime/lib/mysql-connector-java-5.1.6-bin.jar \
  /home/manas/sandbox/recommendation-engine/build/libs/recommendation-engine.jar > /home/manas/sparkoutput/predictions/recommendations/cronjob.log 2>&1
