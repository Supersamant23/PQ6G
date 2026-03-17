/* ============================================
 * PQG6 — Post-Quantum 6G Network Simulator
 * ============================================
 * NS-3 simulation that models a 6G point-to-point network with
 * post-quantum key exchange (Kyber) and digital signatures (Dilithium)
 * from liboqs. Generates flow metrics for downstream analytics.
 *
 * Outputs:
 *   - Flow metrics CSV (for Spark ML training — labeled ground truth)
 *   - Flow metrics JSON (one per flow, for Kafka real-time export)
 *
 * Build: Compiled as part of NS-3 scratch module via CMake
 */

#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/applications-module.h"
#include "ns3/flow-monitor-module.h"
#include "ns3/traffic-control-module.h"

#include <oqs/oqs.h>

#include <fstream>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <chrono>
#include <random>
#include <cstring>
#include <vector>
#include <map>
#include <filesystem>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("PQG6Sim");

// ---- PQ Crypto Helpers ----

struct PQKeyExchangeResult {
    double handshake_time_ms;
    uint32_t public_key_bytes;
    uint32_t ciphertext_bytes;
    uint32_t shared_secret_bytes;
    bool success;
};

struct PQSignResult {
    double sign_time_ms;
    double verify_time_ms;
    uint32_t signature_bytes;
    bool success;
};

/**
 * Perform a Kyber key exchange (KEM) and measure overhead.
 */
PQKeyExchangeResult PerformKyberKEX()
{
    PQKeyExchangeResult result = {};
    OQS_KEM *kem = OQS_KEM_new(OQS_KEM_alg_kyber_768);
    if (!kem) {
        NS_LOG_ERROR("Failed to initialize Kyber-768");
        result.success = false;
        return result;
    }

    uint8_t *public_key = (uint8_t *)malloc(kem->length_public_key);
    uint8_t *secret_key = (uint8_t *)malloc(kem->length_secret_key);
    uint8_t *ciphertext = (uint8_t *)malloc(kem->length_ciphertext);
    uint8_t *shared_secret_enc = (uint8_t *)malloc(kem->length_shared_secret);
    uint8_t *shared_secret_dec = (uint8_t *)malloc(kem->length_shared_secret);

    auto start = std::chrono::high_resolution_clock::now();

    // Key generation (server side)
    OQS_KEM_keypair(kem, public_key, secret_key);

    // Encapsulation (client side)
    OQS_KEM_encaps(kem, ciphertext, shared_secret_enc, public_key);

    // Decapsulation (server side)
    OQS_KEM_decaps(kem, shared_secret_dec, ciphertext, secret_key);

    auto end = std::chrono::high_resolution_clock::now();

    result.handshake_time_ms = std::chrono::duration<double, std::milli>(end - start).count();
    result.public_key_bytes = kem->length_public_key;
    result.ciphertext_bytes = kem->length_ciphertext;
    result.shared_secret_bytes = kem->length_shared_secret;
    result.success = (memcmp(shared_secret_enc, shared_secret_dec, kem->length_shared_secret) == 0);

    free(public_key); free(secret_key); free(ciphertext);
    free(shared_secret_enc); free(shared_secret_dec);
    OQS_KEM_free(kem);

    return result;
}

/**
 * Perform a Dilithium signature and verification, measure overhead.
 */
PQSignResult PerformDilithiumSign(const uint8_t *message, size_t msg_len)
{
    PQSignResult result = {};
    OQS_SIG *sig = OQS_SIG_new(OQS_SIG_alg_dilithium_3);
    if (!sig) {
        NS_LOG_ERROR("Failed to initialize Dilithium3");
        result.success = false;
        return result;
    }

    uint8_t *public_key = (uint8_t *)malloc(sig->length_public_key);
    uint8_t *secret_key = (uint8_t *)malloc(sig->length_secret_key);
    uint8_t *signature = (uint8_t *)malloc(sig->length_signature);
    size_t sig_len = 0;

    OQS_SIG_keypair(sig, public_key, secret_key);

    // Sign
    auto t1 = std::chrono::high_resolution_clock::now();
    OQS_SIG_sign(sig, signature, &sig_len, message, msg_len, secret_key);
    auto t2 = std::chrono::high_resolution_clock::now();

    // Verify
    auto t3 = std::chrono::high_resolution_clock::now();
    OQS_STATUS verify_status = OQS_SIG_verify(sig, message, msg_len, signature, sig_len, public_key);
    auto t4 = std::chrono::high_resolution_clock::now();

    result.sign_time_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    result.verify_time_ms = std::chrono::duration<double, std::milli>(t4 - t3).count();
    result.signature_bytes = sig_len;
    result.success = (verify_status == OQS_SUCCESS);

    free(public_key); free(secret_key); free(signature);
    OQS_SIG_free(sig);

    return result;
}

// ---- Attack Traffic Generators ----

enum AttackType { NORMAL = 0, FLOOD, STEALTH, BURST };

std::string AttackTypeToString(AttackType t) {
    switch(t) {
        case FLOOD: return "FLOOD";
        case STEALTH: return "STEALTH";
        case BURST: return "BURST";
        default: return "Normal";
    }
}

// ---- Per-Packet Logging ----

struct PacketLogContext {
    uint32_t flowId;
    std::string srcIp;
    std::string dstIp;
    uint16_t port;
    std::string attackType;
    bool pqcEnabled;
    double kemMs;
    double signMs;
    double verifyMs;
};

std::ofstream g_packetLog;
std::vector<PacketLogContext> g_contexts;

void OnTxPacket(PacketLogContext *ctx, Ptr<const Packet> packet)
{
    g_packetLog << std::fixed << std::setprecision(6)
                << Simulator::Now().GetSeconds() << ","
                << ctx->flowId << ","
                << ctx->srcIp << "," << ctx->dstIp << ","
                << ctx->port << ","
                << packet->GetSize() << ",tx,"
                << ctx->attackType << ","
                << (ctx->pqcEnabled ? 1 : 0) << ","
                << ctx->kemMs << "," << ctx->signMs << "," << ctx->verifyMs
                << "\n";
}

void OnRxPacket(PacketLogContext *ctx, Ptr<const Packet> packet, const Address &)
{
    g_packetLog << std::fixed << std::setprecision(6)
                << Simulator::Now().GetSeconds() << ","
                << ctx->flowId << ","
                << ctx->srcIp << "," << ctx->dstIp << ","
                << ctx->port << ","
                << packet->GetSize() << ",rx,"
                << ctx->attackType << ","
                << (ctx->pqcEnabled ? 1 : 0) << ","
                << ctx->kemMs << "," << ctx->signMs << "," << ctx->verifyMs
                << "\n";
}

/**
 * Install application traffic on a flow with given attack profile.
 */
void InstallTraffic(Ptr<Node> src, Ptr<Node> dst,
                    Ipv4Address dstAddr, uint16_t port,
                    AttackType attack, double startTime, double duration)
{
    switch(attack) {
        case FLOOD: {
            // High-rate constant flood
            OnOffHelper onoff("ns3::UdpSocketFactory",
                              InetSocketAddress(dstAddr, port));
            onoff.SetAttribute("DataRate", DataRateValue(DataRate("50Mbps")));
            onoff.SetAttribute("PacketSize", UintegerValue(1024));
            onoff.SetAttribute("OnTime", StringValue("ns3::ConstantRandomVariable[Constant=1.0]"));
            onoff.SetAttribute("OffTime", StringValue("ns3::ConstantRandomVariable[Constant=0.0]"));
            auto apps = onoff.Install(src);
            apps.Start(Seconds(startTime));
            apps.Stop(Seconds(startTime + duration));
            break;
        }
        case STEALTH: {
            // Low-rate, small packets — hard to detect by rules
            OnOffHelper onoff("ns3::UdpSocketFactory",
                              InetSocketAddress(dstAddr, port));
            onoff.SetAttribute("DataRate", DataRateValue(DataRate("100Kbps")));
            onoff.SetAttribute("PacketSize", UintegerValue(64));
            onoff.SetAttribute("OnTime", StringValue("ns3::ConstantRandomVariable[Constant=0.1]"));
            onoff.SetAttribute("OffTime", StringValue("ns3::ExponentialRandomVariable[Mean=2.0]"));
            auto apps = onoff.Install(src);
            apps.Start(Seconds(startTime));
            apps.Stop(Seconds(startTime + duration));
            break;
        }
        case BURST: {
            // Periodic high-rate bursts
            OnOffHelper onoff("ns3::UdpSocketFactory",
                              InetSocketAddress(dstAddr, port));
            onoff.SetAttribute("DataRate", DataRateValue(DataRate("100Mbps")));
            onoff.SetAttribute("PacketSize", UintegerValue(1400));
            onoff.SetAttribute("OnTime", StringValue("ns3::ConstantRandomVariable[Constant=0.5]"));
            onoff.SetAttribute("OffTime", StringValue("ns3::ConstantRandomVariable[Constant=3.0]"));
            auto apps = onoff.Install(src);
            apps.Start(Seconds(startTime));
            apps.Stop(Seconds(startTime + duration));
            break;
        }
        default: {
            // Normal background traffic
            OnOffHelper onoff("ns3::UdpSocketFactory",
                              InetSocketAddress(dstAddr, port));
            onoff.SetAttribute("DataRate", DataRateValue(DataRate("5Mbps")));
            onoff.SetAttribute("PacketSize", UintegerValue(512));
            onoff.SetAttribute("OnTime", StringValue("ns3::ConstantRandomVariable[Constant=1.0]"));
            onoff.SetAttribute("OffTime", StringValue("ns3::ExponentialRandomVariable[Mean=1.0]"));
            auto apps = onoff.Install(src);
            apps.Start(Seconds(startTime));
            apps.Stop(Seconds(startTime + duration));

            // Sink on destination
            PacketSinkHelper sink("ns3::UdpSocketFactory",
                                  InetSocketAddress(Ipv4Address::GetAny(), port));
            auto sinkApps = sink.Install(dst);
            sinkApps.Start(Seconds(0.0));
            break;
        }
    }

    // Always install a sink on the destination for attack traffic too
    if (attack != NORMAL) {
        PacketSinkHelper sink("ns3::UdpSocketFactory",
                              InetSocketAddress(Ipv4Address::GetAny(), port));
        auto sinkApps = sink.Install(dst);
        sinkApps.Start(Seconds(0.0));
    }
}

// ---- Main Simulation ----

int main(int argc, char *argv[])
{
    // Command-line parameters
    double simDuration = 30.0;    // seconds
    uint32_t numNormalFlows = 20;
    uint32_t numFloodFlows = 3;
    uint32_t numStealthFlows = 3;
    uint32_t numBurstFlows = 2;
    uint32_t flowMultiplier = 2;
    bool enablePQC = true;
    std::string outputDir = "/opt/pqg6/output";
    std::string packetLogPath = "";

    CommandLine cmd;
    cmd.AddValue("duration", "Simulation duration (s)", simDuration);
    cmd.AddValue("normalFlows", "Number of normal flows", numNormalFlows);
    cmd.AddValue("floodFlows", "Number of FLOOD attack flows", numFloodFlows);
    cmd.AddValue("stealthFlows", "Number of STEALTH attack flows", numStealthFlows);
    cmd.AddValue("burstFlows", "Number of BURST attack flows", numBurstFlows);
    cmd.AddValue("multiplier", "Global flow count multiplier", flowMultiplier);
    cmd.AddValue("pqc", "Enable post-quantum crypto", enablePQC);
    cmd.AddValue("outputDir", "Output directory", outputDir);
    cmd.AddValue("packetLog", "Per-packet CSV output path (empty=disabled)", packetLogPath);
    cmd.Parse(argc, argv);
    
    numNormalFlows *= flowMultiplier;
    numFloodFlows *= flowMultiplier;
    numStealthFlows *= flowMultiplier;
    numBurstFlows *= flowMultiplier;

    uint32_t totalFlows = numNormalFlows + numFloodFlows + numStealthFlows + numBurstFlows;

    NS_LOG_INFO("PQG6 Simulation: " << totalFlows << " flows, duration=" << simDuration
                << "s, PQC=" << (enablePQC ? "ON" : "OFF"));

    // ---- Create topology: star with central router ----
    NodeContainer router;
    router.Create(1);

    NodeContainer sources;
    sources.Create(totalFlows);

    NodeContainer destinations;
    destinations.Create(totalFlows);

    InternetStackHelper internet;
    internet.Install(router);
    internet.Install(sources);
    internet.Install(destinations);

    PointToPointHelper p2p;
    p2p.SetDeviceAttribute("DataRate", StringValue("10Gbps"));  // 6G speeds
    p2p.SetChannelAttribute("Delay", StringValue("0.1ms"));     // Sub-ms latency

    Ipv4AddressHelper address;

    // PQ crypto results (per-flow)
    struct FlowCryptoInfo {
        PQKeyExchangeResult kex;
        PQSignResult sign;
        AttackType attackType;
    };
    std::vector<FlowCryptoInfo> cryptoInfo(totalFlows);

    // ---- Assign flows and install traffic ----
    std::mt19937 rng(42);  // Deterministic seed for reproducibility
    uint16_t basePort = 9000;

    // Build attack type list for each flow
    std::vector<AttackType> flowTypes;
    for (uint32_t i = 0; i < numNormalFlows; i++) flowTypes.push_back(NORMAL);
    for (uint32_t i = 0; i < numFloodFlows; i++) flowTypes.push_back(FLOOD);
    for (uint32_t i = 0; i < numStealthFlows; i++) flowTypes.push_back(STEALTH);
    for (uint32_t i = 0; i < numBurstFlows; i++) flowTypes.push_back(BURST);

    // Shuffle to mix attack and normal traffic
    std::shuffle(flowTypes.begin(), flowTypes.end(), rng);

    for (uint32_t i = 0; i < totalFlows; i++) {
        // Source -> Router link
        std::ostringstream srcSubnet;
        srcSubnet << "10.1." << (i * 2 + 1) << ".0";
        address.SetBase(Ipv4Address(srcSubnet.str().c_str()), "255.255.255.0");
        NetDeviceContainer srcLink = p2p.Install(sources.Get(i), router.Get(0));
        Ipv4InterfaceContainer srcIface = address.Assign(srcLink);

        // Router -> Destination link
        std::ostringstream dstSubnet;
        dstSubnet << "10.1." << (i * 2 + 2) << ".0";
        address.SetBase(Ipv4Address(dstSubnet.str().c_str()), "255.255.255.0");
        NetDeviceContainer dstLink = p2p.Install(router.Get(0), destinations.Get(i));
        Ipv4InterfaceContainer dstIface = address.Assign(dstLink);

        // Perform PQ crypto handshake per flow
        if (enablePQC) {
            cryptoInfo[i].kex = PerformKyberKEX();
            // Sign a dummy "handshake confirmation" message
            std::string msg = "flow-" + std::to_string(i) + "-handshake";
            cryptoInfo[i].sign = PerformDilithiumSign(
                (const uint8_t*)msg.c_str(), msg.size());
        }
        cryptoInfo[i].attackType = flowTypes[i];

        // Stagger start times slightly for realism
        double startOffset = 1.0 + (i * 0.1);
        InstallTraffic(sources.Get(i), destinations.Get(i),
                       dstIface.GetAddress(1), basePort + i,
                       flowTypes[i], startOffset, simDuration - startOffset - 1.0);
    }

    // Enable global routing
    Ipv4GlobalRoutingHelper::PopulateRoutingTables();

    // ---- Per-packet logging (if enabled) ----
    bool packetLogEnabled = !packetLogPath.empty();
    if (packetLogEnabled) {
        if (packetLogPath == "auto") {
            packetLogPath = outputDir + "/packets.csv";
        }
        g_packetLog.open(packetLogPath);
        g_packetLog << "timestamp_s,flow_id,src_ip,dst_ip,port,packet_size_bytes,"
                    << "direction,attack_type,pqc_enabled,kem_handshake_ms,"
                    << "sig_sign_ms,sig_verify_ms" << std::endl;

        // Build per-flow contexts
        g_contexts.resize(totalFlows);
        for (uint32_t i = 0; i < totalFlows; i++) {
            auto &ci = cryptoInfo[i];
            std::ostringstream srcAddr, dstAddr;
            srcAddr << "10.1." << (i * 2 + 1) << ".1";
            dstAddr << "10.1." << (i * 2 + 2) << ".2";

            g_contexts[i] = {
                i,
                srcAddr.str(),
                dstAddr.str(),
                static_cast<uint16_t>(basePort + i),
                AttackTypeToString(ci.attackType),
                enablePQC,
                ci.kex.handshake_time_ms,
                ci.sign.sign_time_ms,
                ci.sign.verify_time_ms
            };

            // Connect to OnOffApplication Tx trace on source node
            uint32_t srcNodeId = sources.Get(i)->GetId();
            std::string txPath = "/NodeList/" + std::to_string(srcNodeId)
                + "/ApplicationList/0/$ns3::OnOffApplication/Tx";
            Config::ConnectWithoutContext(txPath,
                MakeBoundCallback(&OnTxPacket, &g_contexts[i]));

            // Connect to PacketSink Rx trace on destination node
            uint32_t dstNodeId = destinations.Get(i)->GetId();
            std::string rxPath = "/NodeList/" + std::to_string(dstNodeId)
                + "/ApplicationList/0/$ns3::PacketSink/Rx";
            Config::ConnectWithoutContext(rxPath,
                MakeBoundCallback(&OnRxPacket, &g_contexts[i]));
        }

        NS_LOG_INFO("Per-packet logging enabled: " << packetLogPath);
    }

    // ---- Install FlowMonitor ----
    FlowMonitorHelper flowMonHelper;
    Ptr<FlowMonitor> flowMon = flowMonHelper.InstallAll();

    // ---- Run simulation ----
    Simulator::Stop(Seconds(simDuration));
    Simulator::Run();

    // ---- Collect flow metrics ----
    flowMon->CheckForLostPackets();
    Ptr<Ipv4FlowClassifier> classifier =
        DynamicCast<Ipv4FlowClassifier>(flowMonHelper.GetClassifier());
    FlowMonitor::FlowStatsContainer stats = flowMon->GetFlowStats();

    // Open CSV output (ground truth with labels for Spark ML training)
    std::string csvPath = outputDir + "/ground-truth.csv";
    std::ofstream csvFile(csvPath);
    csvFile << "Flow_ID,Src_IP,Dst_IP,Src_Port,Dst_Port,"
            << "Flow_Duration,Total_Fwd_Packets,Total_Bwd_Packets,"
            << "Flow_Bytes_s,Flow_Packets_s,"
            << "Flow_IAT_Mean,Flow_IAT_Std,Flow_IAT_Max,Flow_IAT_Min,"
            << "Fwd_Pkt_Len_Mean,Fwd_Pkt_Len_Std,Fwd_Pkt_Len_Max,Fwd_Pkt_Len_Min,"
            << "PQC_Enabled,KEM_Handshake_ms,KEM_PubKey_Bytes,KEM_Ciphertext_Bytes,"
            << "SIG_Sign_ms,SIG_Verify_ms,SIG_Bytes,"
            << "Attack_Type" << std::endl;

    // Open JSON output (for Kafka streaming — unlabeled for real-time pipeline)
    std::string jsonDir = outputDir + "/flows";
    std::filesystem::create_directories(jsonDir);

    uint32_t flowIndex = 0;
    for (auto &entry : stats) {
        Ipv4FlowClassifier::FiveTuple ft = classifier->FindFlow(entry.first);
        FlowMonitor::FlowStats &fs = entry.second;

        // Calculate derived metrics
        double duration_s = (fs.timeLastRxPacket - fs.timeFirstTxPacket).GetSeconds();
        if (duration_s <= 0) duration_s = 0.001;

        uint64_t totalBytes = fs.rxBytes;
        uint32_t totalPackets = fs.rxPackets + fs.txPackets;
        double bytes_per_s = totalBytes / duration_s;
        double packets_per_s = totalPackets / duration_s;

        // Calculate IAT (Inter-Arrival Time) statistics
        double iat_mean = 0, iat_std = 0, iat_max = 0, iat_min = 0;
        if (fs.rxPackets > 1) {
            iat_mean = duration_s / (fs.rxPackets - 1) * 1000.0; // ms
            iat_max = fs.delaySum.GetSeconds() / fs.rxPackets * 1000.0 * 2.0; // Approximation
            iat_min = iat_mean * 0.5; // Approximation
            iat_std = iat_mean * 0.3; // Approximation
        }

        // Packet length stats (approximated from total bytes / total packets)
        double pkt_len_mean = (fs.txPackets > 0) ? (double)fs.txBytes / fs.txPackets : 0;
        double pkt_len_std = pkt_len_mean * 0.1;
        double pkt_len_max = pkt_len_mean * 1.2;
        double pkt_len_min = pkt_len_mean * 0.8;

        // Get attack type label (only for flows we created)
        AttackType atype = NORMAL;
        if (flowIndex < totalFlows) {
            atype = cryptoInfo[flowIndex].attackType;
        }

        // Get PQ crypto info
        auto &ci = (flowIndex < totalFlows) ? cryptoInfo[flowIndex] : cryptoInfo[0];

        // Write CSV row (labeled ground truth)
        csvFile << entry.first << ","
                << ft.sourceAddress << "," << ft.destinationAddress << ","
                << ft.sourcePort << "," << ft.destinationPort << ","
                << std::fixed << std::setprecision(6)
                << duration_s << "," << fs.txPackets << "," << fs.rxPackets << ","
                << bytes_per_s << "," << packets_per_s << ","
                << iat_mean << "," << iat_std << "," << iat_max << "," << iat_min << ","
                << pkt_len_mean << "," << pkt_len_std << "," << pkt_len_max << "," << pkt_len_min << ","
                << (enablePQC ? 1 : 0) << ","
                << ci.kex.handshake_time_ms << "," << ci.kex.public_key_bytes << "," << ci.kex.ciphertext_bytes << ","
                << ci.sign.sign_time_ms << "," << ci.sign.verify_time_ms << "," << ci.sign.signature_bytes << ","
                << AttackTypeToString(atype) << std::endl;

        // Write JSON file (unlabeled — for Kafka real-time export)
        std::ostringstream jsonPath;
        jsonPath << jsonDir << "/flow-" << entry.first << ".json";
        std::ofstream jsonFile(jsonPath.str());
        jsonFile << "{" << std::endl
                 << "  \"flow_id\": " << entry.first << "," << std::endl
                 << "  \"src_ip\": \"" << ft.sourceAddress << "\"," << std::endl
                 << "  \"dst_ip\": \"" << ft.destinationAddress << "\"," << std::endl
                 << "  \"src_port\": " << ft.sourcePort << "," << std::endl
                 << "  \"dst_port\": " << ft.destinationPort << "," << std::endl
                 << "  \"flow_duration\": " << duration_s << "," << std::endl
                 << "  \"total_fwd_packets\": " << fs.txPackets << "," << std::endl
                 << "  \"total_bwd_packets\": " << fs.rxPackets << "," << std::endl
                 << "  \"flow_bytes_s\": " << bytes_per_s << "," << std::endl
                 << "  \"flow_packets_s\": " << packets_per_s << "," << std::endl
                 << "  \"flow_iat_mean\": " << iat_mean << "," << std::endl
                 << "  \"flow_iat_std\": " << iat_std << "," << std::endl
                 << "  \"flow_iat_max\": " << iat_max << "," << std::endl
                 << "  \"flow_iat_min\": " << iat_min << "," << std::endl
                 << "  \"fwd_pkt_len_mean\": " << pkt_len_mean << "," << std::endl
                 << "  \"fwd_pkt_len_std\": " << pkt_len_std << "," << std::endl
                 << "  \"fwd_pkt_len_max\": " << pkt_len_max << "," << std::endl
                 << "  \"fwd_pkt_len_min\": " << pkt_len_min << "," << std::endl
                 << "  \"pqc_enabled\": " << (enablePQC ? "true" : "false") << "," << std::endl
                 << "  \"kem_handshake_ms\": " << ci.kex.handshake_time_ms << "," << std::endl
                 << "  \"kem_pubkey_bytes\": " << ci.kex.public_key_bytes << "," << std::endl
                 << "  \"kem_ciphertext_bytes\": " << ci.kex.ciphertext_bytes << "," << std::endl
                 << "  \"sig_sign_ms\": " << ci.sign.sign_time_ms << "," << std::endl
                 << "  \"sig_verify_ms\": " << ci.sign.verify_time_ms << "," << std::endl
                 << "  \"sig_bytes\": " << ci.sign.signature_bytes << "," << std::endl
                 << "  \"timestamp\": \"" << std::chrono::system_clock::now().time_since_epoch().count() << "\"" << std::endl
                 << "}" << std::endl;
        jsonFile.close();

        flowIndex++;
    }

    csvFile.close();

    NS_LOG_INFO("Simulation complete. Generated " << flowIndex << " flow records.");
    NS_LOG_INFO("CSV output: " << csvPath);
    NS_LOG_INFO("JSON output: " << jsonDir);

    if (packetLogEnabled) {
        g_packetLog.close();
        NS_LOG_INFO("Per-packet log: " << packetLogPath);
    }

    Simulator::Destroy();
    return 0;
}
