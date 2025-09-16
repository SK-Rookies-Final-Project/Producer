package Kafka.Flink;

import Kafka.Flink.SecurityAlertData.*;
import org.apache.avro.Schema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;

public class Flink_certified {
    // --- 환경 설정 및 상수 ---
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("BOOTSTRAP", "3.143.42.149:29092,3.143.42.149:39092,3.143.42.149:49092");
    private static final String SOURCE_TOPIC = "login-auth-raw";
    private static final String SOURCE_GROUP_ID = "flink-brokerlog-alert-detector-v3";

    // --- JSON Sink 토픽 ---
    private static final String FREQUENT_FAILURES_JSON_TOPIC = "certified-2time";
    private static final String INACTIVITY_JSON_TOPIC = "certified-notMove";

    // --- AVRO Sink 관련 상수 ---
    private static final String SR_URL = "http://3.143.42.149:18081";
    private static final String FREQUENT_FAILURES_AVRO_TOPIC = "certified-2time-avro";
    private static final String INACTIVITY_AVRO_TOPIC = "certified-notMove-avro";
    private static final String KEY_SCHEMA_STR = SecurityAlertData.KEY_SCHEMA_STR;
    private static final String VALUE_SCHEMA_STR = SecurityAlertData.VALUE_SCHEMA_STR;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId(SOURCE_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .setProperties(getKafkaAuthProps())
                .build();

        DataStream<String> jsonStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaRawLogSource");

        DataStream<BrokerLogEvent> parsedBrokerLogs = jsonStream
                .map((MapFunction<String, BrokerLogEvent>) value -> {
                    try {
                        return new ObjectMapper().readValue(value, BrokerLogEvent.class);
                    } catch (Exception e) {
                        return null; // 파싱 실패 시 null 반환
                    }
                })
                .filter(Objects::nonNull);

        DataStream<SecurityAlert> allAlertsStream = parsedBrokerLogs
                .flatMap(new LogExtractor())
                .keyBy((KeySelector<AuthFailureEvent, String>) event -> event.clientIp)
                .process(new AdvancedFailureDetector())
                .name("AdvancedFailureDetectionLogic");

        // --- 경고 스트림 분기 ---
        DataStream<SecurityAlert> frequentFailuresAlerts = allAlertsStream
                .filter(alert -> "FREQUENT_FAILURES".equals(alert.alertType));

        DataStream<SecurityAlert> inactivityAlerts = allAlertsStream
                .filter(alert -> "INACTIVITY_AFTER_FAILURE".equals(alert.alertType));

        // --- JSON Sinks ---
        KafkaSink<SecurityAlert> frequentJsonSink = createKafkaJsonSink(FREQUENT_FAILURES_JSON_TOPIC, BOOTSTRAP_SERVERS);
        frequentFailuresAlerts.sinkTo(frequentJsonSink).name("FrequentFailuresSink_JSON");

        KafkaSink<SecurityAlert> inactivityJsonSink = createKafkaJsonSink(INACTIVITY_JSON_TOPIC, BOOTSTRAP_SERVERS);
        inactivityAlerts.sinkTo(inactivityJsonSink).name("InactivitySink_JSON");

        // --- AVRO Sinks ---
        final Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_STR);
        final Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_STR);

        KafkaSink<SecurityAlert> frequentAvroSink = createKafkaAvroSink(FREQUENT_FAILURES_AVRO_TOPIC, BOOTSTRAP_SERVERS, keySchema, valueSchema);
        frequentFailuresAlerts.sinkTo(frequentAvroSink).name("FrequentFailuresSink_AVRO");

        KafkaSink<SecurityAlert> inactivityAvroSink = createKafkaAvroSink(INACTIVITY_AVRO_TOPIC, BOOTSTRAP_SERVERS, keySchema, valueSchema);
        inactivityAlerts.sinkTo(inactivityAvroSink).name("InactivitySink_AVRO");

        env.execute("Broker Log Authentication Failure Detector Job (JSON + Avro)");
    }

    private static Properties getKafkaAuthProps() {
        Properties props = new Properties();
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"admin-secret\";");
        return props;
    }

    /**
     * JSON Sink의 Key를 'id' 필드를 사용하도록 수정
     */
    private static KafkaSink<SecurityAlert> createKafkaJsonSink(String topic, String bootstrapServers) {
        return KafkaSink.<SecurityAlert>builder()
                .setBootstrapServers(bootstrapServers)
                .setKafkaProducerConfig(getKafkaAuthProps())
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setKeySerializationSchema((SerializationSchema<SecurityAlert>) alert -> {
                                    if (alert.id != null) {
                                        return alert.id.getBytes(StandardCharsets.UTF_8);
                                    }
                                    return null;
                                })
                                .setValueSerializationSchema(new SecurityAlertSerializer())
                                .build()
                )
                .build();
    }

    /**
     * Avro Sink를 생성합니다.
     */
    private static KafkaSink<SecurityAlert> createKafkaAvroSink(String topic, String bootstrapServers, Schema keySchema, Schema valueSchema) {
        Properties avroProps = getKafkaAuthProps();
        avroProps.put("schema.registry.url", SR_URL);
        return KafkaSink.<SecurityAlert>builder()
                .setBootstrapServers(bootstrapServers)
                .setKafkaProducerConfig(avroProps)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setKeySerializationSchema(new SecurityAlertToAvroSerializer(keySchema, topic + "-key", SR_URL, true))
                                .setValueSerializationSchema(new SecurityAlertToAvroSerializer(valueSchema, topic + "-value", SR_URL, false))
                                .build()
                )
                .build();
    }
}

