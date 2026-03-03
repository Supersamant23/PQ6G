import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, IndexToString}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

/**
 * PQG6 — Spark MLlib Model Training
 *
 * Reads labeled ground-truth CSV from HDFS (produced by NS-3 simulator),
 * trains a Random Forest classifier for multiclass attack detection
 * (Normal / FLOOD / STEALTH / BURST), evaluates on a holdout set,
 * and saves the trained model + metrics to HDFS.
 */
object TrainModel {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PQG6-ML-Training")
      .getOrCreate()

    import spark.implicits._

    val hdfsBase = sys.env.getOrElse("HDFS_NAMENODE", "hdfs://namenode:9000")
    val trainingPath = s"$hdfsBase/pqg6/training/ground-truth.csv"
    val modelPath = s"$hdfsBase/pqg6/models/rf-model"
    val metricsPath = s"$hdfsBase/pqg6/metrics"

    println("=" * 80)
    println("  PQG6 — SPARK ML MODEL TRAINING")
    println("  Post-Quantum 6G Attack Classification")
    println("=" * 80)
    println(s"Training data: $trainingPath")
    println(s"Model output:  $modelPath")

    // ---- Load labeled data ----
    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(trainingPath)

    println(s"Loaded ${rawDf.count()} flow records")
    println(s"Schema: ${rawDf.columns.mkString(", ")}")

    // Show class distribution
    rawDf.groupBy("Attack_Type").count().show()

    // ---- Feature engineering ----
    val featureCols = Array(
      "Flow_Bytes_s", "Flow_Packets_s",
      "Flow_IAT_Mean", "Flow_IAT_Std", "Flow_IAT_Max", "Flow_IAT_Min",
      "Fwd_Pkt_Len_Mean", "Fwd_Pkt_Len_Std", "Fwd_Pkt_Len_Max", "Fwd_Pkt_Len_Min",
      "Flow_Duration", "Total_Fwd_Packets",
      "KEM_Handshake_ms", "SIG_Sign_ms", "SIG_Verify_ms"
    )

    // Index Attack_Type string → numeric label
    val labelIndexer = new StringIndexer()
      .setInputCol("Attack_Type")
      .setOutputCol("label")
      .setHandleInvalid("keep")

    // Assemble feature vector
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
      .setHandleInvalid("skip")

    // ---- Random Forest ----
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(100)
      .setMaxDepth(10)
      .setSeed(42)

    // Label converter for readable predictions
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predicted_attack")

    // ---- Pipeline ----
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, assembler, rf, labelConverter))

    // ---- Train/Test split ----
    val Array(trainDf, testDf) = rawDf.randomSplit(Array(0.8, 0.2), seed = 42)
    println(s"Training set: ${trainDf.count()} rows")
    println(s"Test set:     ${testDf.count()} rows")

    // ---- Fit model ----
    println("Training Random Forest model...")
    val model = pipeline.fit(trainDf)

    // ---- Evaluate ----
    val predictions = model.transform(testDf)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val accuracy  = evaluator.setMetricName("accuracy").evaluate(predictions)
    val precision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
    val recall    = evaluator.setMetricName("weightedRecall").evaluate(predictions)
    val f1        = evaluator.setMetricName("f1").evaluate(predictions)

    println()
    println("=" * 40)
    println("  MODEL EVALUATION RESULTS")
    println("=" * 40)
    println(f"  Accuracy:  $accuracy%.4f")
    println(f"  Precision: $precision%.4f")
    println(f"  Recall:    $recall%.4f")
    println(f"  F1 Score:  $f1%.4f")
    println("=" * 40)

    // Show confusion matrix
    println("\nPrediction breakdown:")
    predictions.groupBy("Attack_Type", "predicted_attack").count().show()

    // ---- Save model ----
    println(s"Saving model to $modelPath ...")
    model.write.overwrite().save(modelPath)

    // ---- Save metrics as JSON ----
    val metricsJson = Seq(
      s"""{"accuracy":$accuracy,"precision":$precision,"recall":$recall,"f1":$f1,"numTrees":100,"maxDepth":10}"""
    ).toDF("metrics")
    metricsJson.write.mode("overwrite").text(s"$metricsPath/evaluation.json")

    // Save feature importance
    val rfModel = model.stages(2).asInstanceOf[org.apache.spark.ml.classification.RandomForestClassificationModel]
    val importances = rfModel.featureImportances.toArray.zip(featureCols)
      .sortBy(-_._1)
      .map { case (imp, name) => s"""{"feature":"$name","importance":$imp}""" }
    spark.createDataset(importances).write.mode("overwrite").text(s"$metricsPath/feature-importance.json")

    println(s"Metrics saved to $metricsPath")
    println("Training complete!")

    spark.stop()
  }
}
