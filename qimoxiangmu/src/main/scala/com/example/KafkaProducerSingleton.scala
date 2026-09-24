// src/main/scala/com/example/KafkaProducerSingleton.scala
package com.example

// 导入 KafkaProducer 所需的类和配置项
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties // 用于存储 Kafka 配置的键值对

/**
 * KafkaProducerSingleton 对象
 * 提供一个线程安全的 KafkaProducer 单例，用于整个应用中复用 KafkaProducer 实例。
 */
object KafkaProducerSingleton {

  // Step 1: 定义一个私有的易变变量，存储 KafkaProducer 实例
  @volatile private var instance: KafkaProducer[String, String] = _

  /**
   * Step 2: 获取 KafkaProducer 实例
   * 通过双重检查锁和懒加载机制，确保线程安全并避免重复初始化 KafkaProducer。
   *
   * @param bootstrapServers Kafka 集群地址
   * @return KafkaProducer 实例
   */
  def getInstance(bootstrapServers: String): KafkaProducer[String, String] = {
    // 检查 KafkaProducer 实例是否已经初始化
    if (instance == null) {
      // 使用 synchronized 确保多线程下的线程安全性
      synchronized {
        // 第二次检查，防止多个线程同时初始化实例
        if (instance == null) {
          // Step 2.1: 创建 KafkaProducer 配置
          val props = new Properties()
          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers) // 配置 Kafka 集群地址
          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName) // 消息键序列化器
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName) // 消息值序列化器
          props.put(ProducerConfig.ACKS_CONFIG, "all") // 配置消息确认级别为 "all"（确保消息可靠性）
          props.put(ProducerConfig.RETRIES_CONFIG, "3") // 配置失败时重试次数为 3
          props.put(ProducerConfig.LINGER_MS_CONFIG, "1") // 配置延迟发送时间为 1 毫秒（优化吞吐量）

          // Step 2.2: 初始化 KafkaProducer 实例
          instance = new KafkaProducer[String, String](props)
        }
      }
    }
    // 返回单例 KafkaProducer 实例
    instance
  }

  /**
   * Step 3: 关闭 KafkaProducer 实例
   * 释放资源并清理 KafkaProducer 实例。
   */
  def close(): Unit = {
    // 检查 KafkaProducer 实例是否存在
    if (instance != null) {
      // 使用 synchronized 确保线程安全
      synchronized {
        // 再次检查，避免多线程导致的问题
        if (instance != null) {
          instance.close() // 调用 close 方法释放资源
          instance = null // 将实例置为 null，方便重新初始化
        }
      }
    }
  }
}
