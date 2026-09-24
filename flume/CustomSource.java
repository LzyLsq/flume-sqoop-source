//package com.wenchen.flume;
//
//import com.google.gson.JsonArray;
//import com.google.gson.JsonObject;
//import com.google.gson.JsonParser;
//import org.apache.flume.*;
//import org.apache.flume.conf.Configurable;
//import org.apache.flume.event.SimpleEvent;
//import org.apache.flume.source.AbstractSource;
//import org.apache.flume.PollableSource;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//
//import java.nio.charset.StandardCharsets;
//import java.time.Duration;
//import java.util.Collections;
//import java.util.Properties;
//
//public class CustomSource extends AbstractSource implements Configurable, PollableSource {
//
//    private String kafkaTopic;
//    private String kafkaBroker;
//    private KafkaConsumer<String, String> consumer;
//
//    @Override
//    public void configure(Context context) {
//        kafkaTopic = context.getString("kafka.topic", "users");
//
//        // 配置 Kafka Consumer
//        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flumeGroup");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("auto.offset.reset", "earliest");  // 确保从最早的消息开始读取
//
//        consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(Collections.singletonList(kafkaTopic));
//    }
//
//    @Override
//    public void start() {
//        super.start();
//    }
//
//    @Override
//    public void stop() {
//        if (consumer != null) {
//            consumer.close();
//        }
//        super.stop();
//    }
//
//    @Override
//    public Status process() throws EventDeliveryException {
//        // 拉取 Kafka 消息
//        ConsumerRecords<String, String> records = consumer.poll(1000L);
//        for (org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record : records) {
//            String rawMessage = record.value();
//            try {
//                // 解析 JSON 数据
//                JsonObject root = JsonParser.parseString(rawMessage).getAsJsonObject();
//                String userId = root.get("user_id").getAsString();
//                String host = root.get("host").getAsString();
//                JsonArray items = root.getAsJsonArray("items");
//
//                // 处理每个 item，构建扁平化的 JSON
//                for (int i = 0; i < items.size(); i++) {
//                    JsonObject item = items.get(i).getAsJsonObject();
//                    JsonObject flatJson = new JsonObject();
//                    flatJson.addProperty("active_time", item.get("active_time").getAsInt());
//                    flatJson.addProperty("user_id", userId);
//                    flatJson.addProperty("item_type", item.get("item_type").getAsString());
//                    flatJson.addProperty("host", host);
//
//                    // 将每条拆解后的数据作为 Flume Event 传递
//                    Event event = new SimpleEvent();
//                    event.setBody(flatJson.toString().getBytes(StandardCharsets.UTF_8));
//
//                    // 设置事件时间戳，添加到事件头部
//                    event.setHeaders(Collections.singletonMap("timestamp", String.valueOf(System.currentTimeMillis())));
//
//                    // 处理事件
//                    getChannelProcessor().processEvent(event);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        return Status.READY;
//    }
//
//    @Override
//    public long getBackOffSleepIncrement() {
//        return 1000;
//    }
//
//    @Override
//    public long getMaxBackOffSleepInterval() {
//        return 5000;
//    }
//}



import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.PollableSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Properties;

/**
 * 自定义 Flume Source：从 Kafka 订阅订单流数据并写入 Flume Channel。
 * 支持格式：
 * {"order_category":"Toys","order_name":"1005","order_quantity":24,
 *  "order_date":"2025-06-14 12:29:07","is_valid":"Y","user_id":20,"rating":100}
 */
public class CustomSource extends AbstractSource implements Configurable, PollableSource {

    private String kafkaTopic;
    private String kafkaBroker;
    private KafkaConsumer<String, String> consumer;

    @Override
    public void configure(Context context) {
        kafkaTopic = context.getString("kafka.topic", "users");
        // Kafka 地址不再硬编码兜底，统一由 flume.conf 的 kafka.broker 提供；
        // 缺配置时直接失败，避免把内网地址泄露到仓库中
        kafkaBroker = context.getString("kafka.broker");
        if (kafkaBroker == null || kafkaBroker.trim().isEmpty()) {
            throw new IllegalStateException(
                    "缺少 Flume 配置项 kafka.broker，请在 flume.conf 中指定 Kafka Broker 地址");
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flumeGroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(kafkaTopic));
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        if (consumer != null) {
            consumer.close();
        }
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        // 修改为兼容 Java7 的 poll 方法（不使用 Duration）
        ConsumerRecords<String, String> records = consumer.poll(1000L);

        for (org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record : records) {
            String rawMessage = record.value();
            try {
                JsonObject root = JsonParser.parseString(rawMessage).getAsJsonObject();

                JsonObject flatJson = new JsonObject();
                flatJson.addProperty("order_category", root.get("order_category").getAsString());
                flatJson.addProperty("order_name", root.get("order_name").getAsString());
                flatJson.addProperty("order_quantity", root.get("order_quantity").getAsInt());
                flatJson.addProperty("order_date", root.get("order_date").getAsString());
                flatJson.addProperty("is_valid", root.get("is_valid").getAsString());
                flatJson.addProperty("user_id", root.get("user_id").getAsInt());
                flatJson.addProperty("rating", root.get("rating").getAsInt());

                Event event = new SimpleEvent();
                event.setBody(flatJson.toString().getBytes(StandardCharsets.UTF_8));
                event.setHeaders(Collections.singletonMap("timestamp", String.valueOf(System.currentTimeMillis())));

                getChannelProcessor().processEvent(event);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return Status.READY;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 1000;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 5000;
    }
    //将以上代码打包，然后执行
}
