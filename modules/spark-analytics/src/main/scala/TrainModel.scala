import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, IndexToString}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

/**
 * PQG6 — Spark MLlib Model Training
 *
 * Reads per-packet CSV from HDFS (29 GB, produced by NS-3 simulator),
 * aggregates packets into flow-level features using Spark SQL,
 * then trains a Random Forest classifier for multiclass attack detection
 * (Normal / FLOOD / STEALTH / BURST).
 *
 * Designed for distributed execution across a Swarm cluster.
 */
object TrainModel {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PQG6-ML-Training")
      .getOrCreate()

    import spark.implicits._

    val hdfsBase = sys.env.getOrElse("HDFS_NAMENODE", "hdfs://namenode:9000")
    // Accept command-line arg or env var for training data path
    val trainingPath = if (args.length > 0) args(0)
      else sys.env.getOrElse("TRAINING_DATA",
        s"$hdfsBase/pqg6/training/ground-truth.csv")
    val modelPath = s"$hdfsBase/pqg6/models/rf-model"
    val metricsPath = s"$hdfsBase/pqg6/metrics"

    println("=" * 80)
    println("  PQG6 — SPARK ML MODEL TRAINING")
    println("  Post-Quantum 6G Attack Classification")
    println("=" * 80)
    println(s"Training data: $trainingPath")
    println(s"Model output:  $modelPath")

    // ---- Load data ----
    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(trainingPath)

    println(s"Loaded schema: ${rawDf.columns.mkString(", ")}")

    // ---- Detect format: per-packet vs pre-aggregated ----
    val isPerPacket = rawDf.columns.contains("packet_size_bytes")

    val flowDf = if (isPerPacket) {
      println("Detected PER-PACKET format — aggregating to flow-level features...")
      println(s"Raw packet count: ${rawDf.count()}")

      // Window for computing inter-arrival times per flow
      val w = Window.partitionBy("flow_id").orderBy("timestamp_s")

      val withIAT = rawDf
        .withColumn("prev_ts", lag("timestamp_s", 1).over(w))
        .withColumn("iat", $"timestamp_s" - $"prev_ts")

      // Aggregate per flow_id → flow-level features
      val aggregated = withIAT.groupBy("flow_id").agg(
        // Duration
        (max("timestamp_s") - min("timestamp_s")).as("Flow_Duration"),
        // Packet counts
        count("*").as("Total_Fwd_Packets"),
        // Throughput
        (sum("packet_size_bytes") /
          greatest(max("timestamp_s") - min("timestamp_s"), lit(0.001))).as("Flow_Bytes_s"),
        (count("*") /
          greatest(max("timestamp_s") - min("timestamp_s"), lit(0.001))).as("Flow_Packets_s"),
        // Inter-arrival time statistics
        avg("iat").as("Flow_IAT_Mean"),
        stddev("iat").as("Flow_IAT_Std"),
        max("iat").as("Flow_IAT_Max"),
        min("iat").as("Flow_IAT_Min"),
        // Forward packet length statistics
        avg("packet_size_bytes").as("Fwd_Pkt_Len_Mean"),
        stddev("packet_size_bytes").as("Fwd_Pkt_Len_Std"),
        max("packet_size_bytes").as("Fwd_Pkt_Len_Max"),
        min("packet_size_bytes").as("Fwd_Pkt_Len_Min"),
        // Label and PQC — take first per flow (same for all packets in a flow)
        first("attack_type").as("Attack_Type"),
        first("pqc_enabled").as("PQC_Enabled"),
        first("kem_handshake_ms").as("KEM_Handshake_ms"),
        first("sig_sign_ms").as("SIG_Sign_ms"),
        first("sig_verify_ms").as("SIG_Verify_ms")
      ).na.fill(0.0)

      println(s"Aggregated to ${aggregated.count()} flows")
      aggregated

    } else {
      println("Detected PRE-AGGREGATED flow format (ground-truth.csv)")
      println(s"Flow count: ${rawDf.count()}")
      rawDf
    }

    // Show class distribution
    println("\nAttack type distribution:")
    flowDf.groupBy("Attack_Type").count().orderBy(desc("count")).show()

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

    val labelIndexerModel = labelIndexer.fit(flowDf)
    val labels = labelIndexerModel.labelsArray(0)
    println(s"Attack type labels: ${labels.mkString(", ")}")

    val indexedDf = labelIndexerModel.transform(flowDf)
      .na.fill(0.0, featureCols)

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

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predicted_attack")
      .setLabels(labels)

    val pipeline = new Pipeline()
      .setStages(Array(assembler, rf, labelConverter))

    // ---- Train/Test split ----
    val Array(trainDf, testDf) = indexedDf.randomSplit(Array(0.8, 0.2), seed = 42)
    println(s"Training set: ${trainDf.count()} rows")
    println(s"Test set:     ${testDf.count()} rows")

    // ---- Fit model ----
    println("Training Random Forest model...")
    val startTime = System.currentTimeMillis()
    val model = pipeline.fit(trainDf)
    val trainingTime = (System.currentTimeMillis() - startTime) / 1000.0
    println(f"Training completed in $trainingTime%.1f seconds")

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
    println(f"  Training:  $trainingTime%.1f s")
    println("=" * 40)

    // Show confusion matrix
    println("\nPrediction breakdown:")
    predictions.groupBy("Attack_Type", "predicted_attack").count().show()

    // ---- Save model ----
    println(s"Saving model to $modelPath ...")
    model.write.overwrite().save(modelPath)

    // ---- Save metrics as JSON ----
    val metricsJson = Seq(
      s"""{"accuracy":$accuracy,"precision":$precision,"recall":$recall,"f1":$f1,"numTrees":100,"maxDepth":10,"trainingTimeSec":$trainingTime}"""
    ).toDF("metrics")
    metricsJson.write.mode("overwrite").text(s"$metricsPath/evaluation.json")

    // Save feature importance
    val rfModel = model.stages(1).asInstanceOf[org.apache.spark.ml.classification.RandomForestClassificationModel]
    val importances = rfModel.featureImportances.toArray.zip(featureCols)
      .sortBy(-_._1)
      .map { case (imp, name) => s"""{"feature":"$name","importance":$imp}""" }
    spark.createDataset(importances).write.mode("overwrite").text(s"$metricsPath/feature-importance.txt")

    println(s"Metrics saved to $metricsPath")
    println("Training complete!")

    println("\n>>> Spark job finished. The UI is still available at http://localhost:4040")
    println(">>> Press [ENTER] to stop the Spark context and exit...")
    scala.io.StdIn.readLine()

    spark.stop()
  }
}
