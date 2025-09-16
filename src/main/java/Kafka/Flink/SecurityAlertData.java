package Kafka.Flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

public class SecurityAlertData {

    // --- AVRO 스키마 정의 (수정됨) ---
    public static final String KEY_SCHEMA_STR = "{"
            + "\"type\":\"record\", \"name\":\"AlertKey\", \"namespace\":\"com.example.alert\","
            + "\"fields\":[ {\"name\":\"id\",\"type\":\"string\"} ]"
            + "}";

    public static final String VALUE_SCHEMA_STR = "{"
            + "\"type\":\"record\", \"name\":\"SecurityAlertValue\", \"namespace\":\"com.example.alert\","
            + "\"fields\":["
            + " {\"name\":\"id\", \"type\":\"string\"},"
            + " {\"name\":\"client_ip\", \"type\":\"string\"},"
            + " {\"name\":\"alert_time_kst\", \"type\":\"string\"},"
            + " {\"name\":\"alert_type\", \"type\":\"string\"},"
            + " {\"name\":\"description\", \"type\":\"string\"},"
            + " {\"name\":\"failure_count\", \"type\":\"long\"}"
            + "]}";

    // --- 데이터 모델 (POJO) ---
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BrokerLogEvent implements Serializable {
        @JsonProperty("@timestamp")
        public double timestamp;
        public String log;
    }

    public static class AuthFailureEvent implements Serializable {
        public String clientIp;
        public long eventTimestamp;
    }


    public static class SecurityAlert implements Serializable {
        @JsonProperty("id")
        public String id;
        @JsonProperty("client_ip")
        public String clientIp;
        @JsonProperty("alert_time_kst")
        public String alertTimeKST;
        @JsonProperty("alert_type")
        public String alertType;
        @JsonProperty("description")
        public String description;
        @JsonProperty("failure_count")
        public long failureCount;
    }

    // --- 직렬화 클래스 ---
    public static class SecurityAlertSerializer implements SerializationSchema<SecurityAlert> {
        private static final ObjectMapper objectMapper = new ObjectMapper();
        @Override
        public byte[] serialize(SecurityAlert element) {
            try { return objectMapper.writeValueAsBytes(element); }
            catch (Exception e) { throw new RuntimeException(e); }
        }
    }

    // ✅ 변경점: Avro 직렬화 로직을 새로운 스키마와 POJO에 맞게 수정
    public static class SecurityAlertToAvroSerializer implements SerializationSchema<SecurityAlert> {
        private final Schema schema;
        private final String subject;
        private final String srUrl;
        private final boolean isKey;
        private transient SerializationSchema<GenericRecord> delegate;

        public SecurityAlertToAvroSerializer(Schema schema, String subject, String srUrl, boolean isKey) {
            this.schema = schema;
            this.subject = subject;
            this.srUrl = srUrl;
            this.isKey = isKey;
        }

        @Override
        public void open(InitializationContext context) {
            this.delegate = ConfluentRegistryAvroSerializationSchema.forGeneric(subject, schema, srUrl);
        }

        @Override
        public byte[] serialize(SecurityAlert alert) {
            GenericRecord record = new GenericData.Record(schema);
            if (isKey) {
                record.put("id", alert.id);
            } else {
                record.put("id", alert.id);
                record.put("client_ip", alert.clientIp);
                record.put("alert_time_kst", alert.alertTimeKST);
                record.put("alert_type", alert.alertType);
                record.put("description", alert.description);
                record.put("failure_count", alert.failureCount);
            }
            return delegate.serialize(record);
        }
    }

    // --- Flink Function 클래스 ---
    public static class LogExtractor extends RichFlatMapFunction<BrokerLogEvent, AuthFailureEvent> {
        private static final Pattern IP_PATTERN = Pattern.compile("with /([\\d.]+)");

        @Override
        public void flatMap(BrokerLogEvent value, Collector<AuthFailureEvent> out) throws Exception {
            String logMessage = value.log;
            if (logMessage != null && logMessage.contains("Failed authentication") && logMessage.contains("invalid credentials")) {
                java.util.regex.Matcher matcher = IP_PATTERN.matcher(logMessage);
                if (matcher.find()) {
                    String ip = matcher.group(1);
                    long timestamp = (long) (value.timestamp * 1000);
                    AuthFailureEvent failureEvent = new AuthFailureEvent();
                    failureEvent.clientIp = ip;
                    failureEvent.eventTimestamp = timestamp;
                    out.collect(failureEvent);
                }
            }
        }
    }

    public static class AdvancedFailureDetector extends KeyedProcessFunction<String, AuthFailureEvent, SecurityAlert> {
        private ListState<AuthFailureEvent> failureEventsState;
        private ValueState<Long> timerTimestampState;

        @Override
        public void open(Configuration parameters) {
            failureEventsState = getRuntimeContext().getListState(new ListStateDescriptor<>("failureEvents", AuthFailureEvent.class));
            timerTimestampState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerTimestamp", Long.class));
        }

        @Override
        public void processElement(AuthFailureEvent event, Context ctx, Collector<SecurityAlert> out) throws Exception {
            failureEventsState.add(event);
            if (timerTimestampState.value() == null) {
                long timerTimestamp = System.currentTimeMillis() + 10000; // 10초 타이머
                ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
                timerTimestampState.update(timerTimestamp);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SecurityAlert> out) throws Exception {
            Long savedTimer = timerTimestampState.value();
            if (savedTimer != null && savedTimer == timestamp) {
                List<AuthFailureEvent> failures = new ArrayList<>();
                failureEventsState.get().forEach(failures::add);

                String clientIp = ctx.getCurrentKey();
                long failureCount = failures.size();

                if (failureCount >= 2) {
                    out.collect(createAlert(clientIp, "FREQUENT_FAILURES",
                            String.format("IP %s has failed authentication %d times within 10 seconds.", clientIp, failureCount),
                            failureCount));
                } else if (failureCount == 1) {
                    out.collect(createAlert(clientIp, "INACTIVITY_AFTER_FAILURE",
                            String.format("IP %s showed no activity for 10 seconds after a single authentication failure.", clientIp),
                            failureCount));
                }
                failureEventsState.clear();
                timerTimestampState.clear();
            }
        }

        // ✅ 변경점: 경고 생성 시 자동 증가 ID 대신 UUID를 생성하여 할당
        private SecurityAlert createAlert(String clientIp, String alertType, String message, long failureCount) {
            SecurityAlert alert = new SecurityAlert();
            alert.id = UUID.randomUUID().toString(); // UUID로 고유 ID 생성
            alert.clientIp = clientIp;
            alert.alertType = alertType;
            alert.description = message;
            alert.failureCount = failureCount;
            alert.alertTimeKST = ZonedDateTime.now(ZoneId.of("Asia/Seoul"))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS 'KST'"));
            return alert;
        }
    }
}

