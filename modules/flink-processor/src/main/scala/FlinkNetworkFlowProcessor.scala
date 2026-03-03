import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.sink.{KafkaSink, KafkaRecordSerializationSchema}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.configuration.MemorySize
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.time.{Duration, Instant}

// ============================================================================
// DATA MODELS — Matches NS-3 pqg6-sim JSON output
// ============================================================================

@JsonIgnoreProperties(ignoreUnknown = true)
case class NetworkFlow(
    flow_id: Int = 0,
    src_ip: String = "",
    dst_ip: String = "",
    src_port: Int = 0,
    dst_port: Int = 0,
    flow_duration: Double = 0.0,
    total_fwd_packets: Int = 0,
    total_bwd_packets: Int = 0,
    flow_bytes_s: Double = 0.0,
    flow_packets_s: Double = 0.0,
    flow_iat_mean: Double = 0.0,
    flow_iat_std: Double = 0.0,
    flow_iat_max: Double = 0.0,
    flow_iat_min: Double = 0.0,
    fwd_pkt_len_mean: Double = 0.0,
    fwd_pkt_len_std: Double = 0.0,
    fwd_pkt_len_max: Double = 0.0,
    fwd_pkt_len_min: Double = 0.0,
    pqc_enabled: Boolean = false,
    kem_handshake_ms: Double = 0.0,
    kem_pubkey_bytes: Int = 0,
    kem_ciphertext_bytes: Int = 0,
    sig_sign_ms: Double = 0.0,
    sig_verify_ms: Double = 0.0,
    sig_bytes: Int = 0,
    timestamp: String = ""
) {
  // Rule-based attack classification calibrated to NS-3 simulation output:
  //   FLOOD:   OnOff 50 Mbps constant → ~6,250,000 B/s, ~6100 pkt/s
  //   BURST:   OnOff 100 Mbps (0.5s on / 3s off) → ~1,800,000 B/s avg, high IAT std
  //   STEALTH: OnOff 100 Kbps (0.1s on / ~2s off) → ~600-800 B/s, 64-byte pkts
  //   NORMAL:  OnOff 5 Mbps (1s on / ~1s off)   → ~300,000 B/s, 512-byte pkts
  def detectAttackType: String = {
    if (flow_bytes_s > 2000000 || flow_packets_s > 4000) "FLOOD"
    else if (flow_bytes_s > 500000 && flow_iat_std > flow_iat_mean * 0.3) "BURST"
    else if (flow_bytes_s < 5000 && flow_packets_s < 100 && fwd_pkt_len_mean < 100) "STEALTH"
    else "Normal"
  }
  def isAttack: Boolean = detectAttackType != "Normal"
}

case class AttackAlert(
    flowId: Int,
    attackType: String,
    timestamp: Long,
    severity: String,
    srcIp: String,
    dstIp: String,
    flowBytesPerSec: Double,
    flowPacketsPerSec: Double,
    pqcEnabled: Boolean,
    kemHandshakeMs: Double,
    message: String
) {
  def toJson: String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(this)
  }
}

case class FlowStatistics(
    windowStart: Long,
    windowEnd: Long,
    totalFlows: Long,
    attackFlows: Long,
    normalFlows: Long,
    avgBytesPerSec: Double,
    avgPacketsPerSec: Double,
    maxBytesPerSec: Double,
    attackRate: Double,
    avgKemHandshakeMs: Double,
    avgSigSignMs: Double,
    attackTypes: Map[String, Long]
) {
  def toJson: String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(this)
  }
}

// ============================================================================
// JSON PARSING
// ============================================================================

object JsonParser {
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def parseFlow(json: String): Option[NetworkFlow] = {
    try {
      Some(mapper.readValue(json, classOf[NetworkFlow]))
    } catch {
      case _: Exception => None
    }
  }
}

// ============================================================================
// ATTACK DETECTION — PQC-aware thresholds
// ============================================================================

class AttackDetector extends ProcessFunction[NetworkFlow, AttackAlert] {

  override def processElement(
      flow: NetworkFlow,
      ctx: ProcessFunction[NetworkFlow, AttackAlert]#Context,
      out: Collector[AttackAlert]
  ): Unit = {
    val attackType = flow.detectAttackType
    if (attackType != "Normal") {
      val severity = determineSeverity(flow)
      val alert = AttackAlert(
        flowId = flow.flow_id,
        attackType = attackType,
        timestamp = System.currentTimeMillis(),
        severity = severity,
        srcIp = flow.src_ip,
        dstIp = flow.dst_ip,
        flowBytesPerSec = flow.flow_bytes_s,
        flowPacketsPerSec = flow.flow_packets_s,
        pqcEnabled = flow.pqc_enabled,
        kemHandshakeMs = flow.kem_handshake_ms,
        message = s"$attackType attack detected from ${flow.src_ip}→${flow.dst_ip}! " +
          s"Severity: $severity, Bytes/s: ${"%.0f".format(flow.flow_bytes_s)}, " +
          s"PQC: ${if (flow.pqc_enabled) "ON" else "OFF"}"
      )
      out.collect(alert)
    }
  }

  private def determineSeverity(flow: NetworkFlow): String = {
    if (flow.flow_bytes_s > 50000000 || flow.flow_packets_s > 50000) "CRITICAL"
    else if (flow.flow_bytes_s > 10000000 || flow.flow_packets_s > 20000) "HIGH"
    else if (flow.flow_bytes_s > 1000000 || flow.flow_packets_s > 5000) "MEDIUM"
    else "LOW"
  }
}

// ============================================================================
// FLOW STATISTICS AGGREGATOR — with PQC metrics
// ============================================================================

case class FlowStatsAccumulator(
    windowStart: Long = System.currentTimeMillis(),
    windowEnd: Long = System.currentTimeMillis(),
    totalFlows: Long = 0,
    attackFlows: Long = 0,
    normalFlows: Long = 0,
    totalBytes: Double = 0.0,
    totalPackets: Double = 0.0,
    maxBytesPerSec: Double = 0.0,
    totalKemMs: Double = 0.0,
    totalSigMs: Double = 0.0,
    attackTypes: Map[String, Long] = Map.empty
)

class FlowStatsAggregator
    extends AggregateFunction[NetworkFlow, FlowStatsAccumulator, FlowStatistics] {

  override def createAccumulator(): FlowStatsAccumulator = FlowStatsAccumulator()

  override def add(flow: NetworkFlow, acc: FlowStatsAccumulator): FlowStatsAccumulator = {
    val atype = flow.detectAttackType
    acc.copy(
      totalFlows = acc.totalFlows + 1,
      attackFlows = if (flow.isAttack) acc.attackFlows + 1 else acc.attackFlows,
      normalFlows = if (!flow.isAttack) acc.normalFlows + 1 else acc.normalFlows,
      totalBytes = acc.totalBytes + flow.flow_bytes_s,
      totalPackets = acc.totalPackets + flow.flow_packets_s,
      maxBytesPerSec = Math.max(acc.maxBytesPerSec, flow.flow_bytes_s),
      totalKemMs = acc.totalKemMs + flow.kem_handshake_ms,
      totalSigMs = acc.totalSigMs + flow.sig_sign_ms,
      attackTypes = if (flow.isAttack)
        acc.attackTypes.updated(atype, acc.attackTypes.getOrElse(atype, 0L) + 1)
      else acc.attackTypes
    )
  }

  override def getResult(acc: FlowStatsAccumulator): FlowStatistics = {
    val n = if (acc.totalFlows > 0) acc.totalFlows.toDouble else 1.0
    FlowStatistics(
      windowStart = acc.windowStart,
      windowEnd = System.currentTimeMillis(),
      totalFlows = acc.totalFlows,
      attackFlows = acc.attackFlows,
      normalFlows = acc.normalFlows,
      avgBytesPerSec = acc.totalBytes / n,
      avgPacketsPerSec = acc.totalPackets / n,
      maxBytesPerSec = acc.maxBytesPerSec,
      attackRate = if (acc.totalFlows > 0) (acc.attackFlows.toDouble / acc.totalFlows) * 100 else 0.0,
      avgKemHandshakeMs = acc.totalKemMs / n,
      avgSigSignMs = acc.totalSigMs / n,
      attackTypes = acc.attackTypes
    )
  }

  override def merge(a: FlowStatsAccumulator, b: FlowStatsAccumulator): FlowStatsAccumulator = {
    a.copy(
      totalFlows = a.totalFlows + b.totalFlows,
      attackFlows = a.attackFlows + b.attackFlows,
      normalFlows = a.normalFlows + b.normalFlows,
      totalBytes = a.totalBytes + b.totalBytes,
      totalPackets = a.totalPackets + b.totalPackets,
      maxBytesPerSec = Math.max(a.maxBytesPerSec, b.maxBytesPerSec),
      totalKemMs = a.totalKemMs + b.totalKemMs,
      totalSigMs = a.totalSigMs + b.totalSigMs,
      attackTypes = b.attackTypes.foldLeft(a.attackTypes) {
        case (map, (t, c)) => map.updated(t, map.getOrElse(t, 0L) + c)
      }
    )
  }
}

// ============================================================================
// CONFIGURATION — reads from environment variables
// ============================================================================

object KafkaConfig {
  val BOOTSTRAP_SERVERS: String = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
  val GROUP_ID = "pqg6-flink-processor"
  val TOPIC_NETWORK_FLOWS = "network-flows"
  val TOPIC_ALERTS = "security-alerts"
  val TOPIC_STATISTICS = "flow-statistics"
}

object OutputConfig {
  val BASE_PATH: String = sys.env.getOrElse("FLINK_OUTPUT_PATH", "hdfs://namenode:9000/flink-output")
  val ALERTS_PATH: String = s"$BASE_PATH/alerts"
  val STATISTICS_PATH: String = s"$BASE_PATH/statistics"
}

// ============================================================================
// MAIN FLINK APPLICATION
// ============================================================================

object FlinkNetworkFlowProcessor {

  def main(args: Array[String]): Unit = {
    println("=" * 80)
    println("  PQG6 — FLINK REAL-TIME NETWORK FLOW PROCESSOR")
    println("  Post-Quantum 6G Network Security Analytics")
    println("=" * 80)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.enableCheckpointing(10000)

    // ---- Kafka Source ----
    val kafkaSource = KafkaSource
      .builder[String]()
      .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
      .setTopics(KafkaConfig.TOPIC_NETWORK_FLOWS)
      .setGroupId(KafkaConfig.GROUP_ID)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val rawStream = env.fromSource(
      kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka: network-flows"
    )

    // ---- Parse JSON ----
    val flowStream = rawStream
      .flatMap(json => JsonParser.parseFlow(json))
      .name("Parse JSON Flows")

    // ---- Attack Detection Pipeline ----
    val attackAlerts = flowStream
      .filter(_.isAttack)
      .process(new AttackDetector())
      .name("Attack Detector")

    // Print alerts
    attackAlerts.map(a => s"[ALERT] ${a.message}").print().name("Print Alerts")

    // Alerts → Kafka
    val alertSink = KafkaSink
      .builder[String]()
      .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
          .setTopic(KafkaConfig.TOPIC_ALERTS)
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      ).build()

    attackAlerts.map(_.toJson).sinkTo(alertSink).name("Kafka Sink: alerts")

    // Alerts → HDFS/File
    val alertFileSink = FileSink
      .forRowFormat(new Path(OutputConfig.ALERTS_PATH), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(Duration.ofMinutes(15))
          .withInactivityInterval(Duration.ofMinutes(5))
          .withMaxPartSize(MemorySize.ofMebiBytes(128))
          .build()
      ).build()

    attackAlerts.map(_.toJson).sinkTo(alertFileSink).name("File Sink: alerts")

    // ---- Statistics Pipeline (10s tumbling windows) ----
    val statistics = flowStream
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .aggregate(new FlowStatsAggregator())
      .name("Flow Stats (10s windows)")

    statistics.map(s => s.toJson).print().name("Print Stats")

    // Stats → Kafka
    val statsSink = KafkaSink
      .builder[String]()
      .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
          .setTopic(KafkaConfig.TOPIC_STATISTICS)
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      ).build()

    statistics.map(_.toJson).sinkTo(statsSink).name("Kafka Sink: statistics")

    // Stats → HDFS/File
    val statsFileSink = FileSink
      .forRowFormat(new Path(OutputConfig.STATISTICS_PATH), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(Duration.ofMinutes(15))
          .withInactivityInterval(Duration.ofMinutes(5))
          .withMaxPartSize(MemorySize.ofMebiBytes(128))
          .build()
      ).build()

    statistics.map(_.toJson).sinkTo(statsFileSink).name("File Sink: statistics")

    // ---- Launch ----
    println(s"Kafka: ${KafkaConfig.BOOTSTRAP_SERVERS}")
    println(s"Output: ${OutputConfig.BASE_PATH}")
    println("Starting Flink job...")

    env.execute("PQG6 — Quantum-Resistant 6G Flow Processor")
  }
}
