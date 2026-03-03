import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.PipelineModel

/**
 * PQG6 — Spark Structured Streaming Flow Classifier
 *
 * Loads the trained Random Forest model from HDFS, reads new flows
 * from Kafka in real-time, classifies them, and writes predictions
 * to HDFS for the dashboard to consume.
 */
object ClassifyFlows {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PQG6-ML-Classify")
      .getOrCreate()

    import spark.implicits._

    val hdfsBase = sys.env.getOrElse("HDFS_NAMENODE", "hdfs://namenode:9000")
    val kafkaServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    val modelPath = s"$hdfsBase/pqg6/models/rf-model"
    val outputPath = s"$hdfsBase/pqg6/predictions"

    println("=" * 80)
    println("  PQG6 — SPARK STREAMING FLOW CLASSIFIER")
    println("  Real-time ML-based Attack Detection")
    println("=" * 80)
    println(s"Model:  $modelPath")
    println(s"Kafka:  $kafkaServers")
    println(s"Output: $outputPath")

    // ---- Load trained model ----
    println("Loading trained model...")
    val model = PipelineModel.load(modelPath)

    // ---- Define the schema for NS-3 JSON flows ----
    val flowSchema = new StructType()
      .add("flow_id", IntegerType)
      .add("src_ip", StringType)
      .add("dst_ip", StringType)
      .add("src_port", IntegerType)
      .add("dst_port", IntegerType)
      .add("flow_duration", DoubleType)
      .add("total_fwd_packets", IntegerType)
      .add("total_bwd_packets", IntegerType)
      .add("flow_bytes_s", DoubleType)
      .add("flow_packets_s", DoubleType)
      .add("flow_iat_mean", DoubleType)
      .add("flow_iat_std", DoubleType)
      .add("flow_iat_max", DoubleType)
      .add("flow_iat_min", DoubleType)
      .add("fwd_pkt_len_mean", DoubleType)
      .add("fwd_pkt_len_std", DoubleType)
      .add("fwd_pkt_len_max", DoubleType)
      .add("fwd_pkt_len_min", DoubleType)
      .add("pqc_enabled", BooleanType)
      .add("kem_handshake_ms", DoubleType)
      .add("kem_pubkey_bytes", IntegerType)
      .add("kem_ciphertext_bytes", IntegerType)
      .add("sig_sign_ms", DoubleType)
      .add("sig_verify_ms", DoubleType)
      .add("sig_bytes", IntegerType)
      .add("timestamp", StringType)

    // ---- Read from Kafka ----
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", "network-flows")
      .option("startingOffsets", "latest")
      .load()

    // Parse JSON from Kafka value
    val flowStream = kafkaStream
      .selectExpr("CAST(value AS STRING) as json_str")
      .select(from_json($"json_str", flowSchema).as("flow"))
      .select("flow.*")
      // Rename columns to match the training schema
      .withColumnRenamed("flow_bytes_s", "Flow_Bytes_s")
      .withColumnRenamed("flow_packets_s", "Flow_Packets_s")
      .withColumnRenamed("flow_iat_mean", "Flow_IAT_Mean")
      .withColumnRenamed("flow_iat_std", "Flow_IAT_Std")
      .withColumnRenamed("flow_iat_max", "Flow_IAT_Max")
      .withColumnRenamed("flow_iat_min", "Flow_IAT_Min")
      .withColumnRenamed("fwd_pkt_len_mean", "Fwd_Pkt_Len_Mean")
      .withColumnRenamed("fwd_pkt_len_std", "Fwd_Pkt_Len_Std")
      .withColumnRenamed("fwd_pkt_len_max", "Fwd_Pkt_Len_Max")
      .withColumnRenamed("fwd_pkt_len_min", "Fwd_Pkt_Len_Min")
      .withColumnRenamed("flow_duration", "Flow_Duration")
      .withColumnRenamed("total_fwd_packets", "Total_Fwd_Packets")
      .withColumnRenamed("kem_handshake_ms", "KEM_Handshake_ms")
      .withColumnRenamed("sig_sign_ms", "SIG_Sign_ms")
      .withColumnRenamed("sig_verify_ms", "SIG_Verify_ms")

    // ---- Apply model ----
    val predictions = model.transform(flowStream)
      .select(
        $"flow_id", $"src_ip", $"dst_ip",
        $"Flow_Bytes_s", $"Flow_Packets_s",
        $"predicted_attack", $"prediction",
        current_timestamp().as("classified_at")
      )

    // ---- Write predictions to HDFS as JSON ----
    val query = predictions.writeStream
      .format("json")
      .option("path", outputPath)
      .option("checkpointLocation", s"$hdfsBase/pqg6/checkpoints/classify")
      .outputMode("append")
      .start()

    println("Streaming classifier running... Press Ctrl-C to stop.")
    query.awaitTermination()

    spark.stop()
  }
}
