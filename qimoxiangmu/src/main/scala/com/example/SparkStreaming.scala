// src/main/scala/com/example/SparkStreaming.scala
package com.example

// 导入所需的库
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import org.apache.log4j.Logger

import java.util.Properties

// Step 1: SparkStreaming - 主对象，包含 Spark Streaming 的主要逻辑
object SparkStreaming {
  // 初始化 Logger，用于记录日志
  val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    // Step 1.1: Spark 配置和 StreamingContext 初始化
    val conf = new SparkConf().setAppName("SparkKafkaStreamingApp").setMaster("local[*]")

    // 创建 StreamingContext，设置批处理间隔为 5 秒
    val ssc = new StreamingContext(conf, Seconds(5)) // 批处理间隔为5秒

    // Step 1.2: Kafka 消费者配置
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> sys.env.getOrElse("KAFKA_BROKER", "niit:9092"),  // Kafka 集群地址（可用环境变量覆盖）
      "key.deserializer" -> classOf[StringDeserializer],  // 键的反序列化类
      "value.deserializer" -> classOf[StringDeserializer],  // 值的反序列化类
      "group.id" -> "niit",  // 消费者组 ID
      "auto.offset.reset" -> "latest",  // 如果没有偏移量，从最新的消息开始读取
      "enable.auto.commit" -> (false: java.lang.Boolean)  // 禁用自动提交偏移量
    )

    // Step 1.3: 指定要订阅的 Kafka 主题
    val topics = Array("orders")  // 输入主题

    // Step 1.4: 创建 Kafka 直接数据流
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    /*
      Step 2: Kafka 数据流的时间分配
      数据是从 Kafka 中按批次消费的
      由于 StreamingContext 的批处理间隔设置为 5 秒
      所以 Kafka 消费的数据量将根据每个批次的时间窗口（5 秒）进行拉取和处理
      每次 Spark Streaming 被触发时，Kafka 消费者会从 Kafka 中拉取消息并形成一个新的 RDD
      然后对这个 RDD 进行操作
    */

    // Step 2.1: JSON 解析配置
    // 使用 json4s 库进行 JSON 解析，定义隐式格式
    implicit val formats: DefaultFormats.type = DefaultFormats

    // Step 3: 数据处理
    // 对 Kafka 流中的每一条记录进行处理
    stream.map(record => record.value).foreachRDD { rdd =>
      if (!rdd.isEmpty()) { // 检查 RDD 是否为空
        // Step 3.1: 解析 JSON 数据
        // 将每条 JSON 字符串解析为对应的字段，并封装成元组
        val data = rdd.flatMap { record =>
          try {
            val parsed = parse(record) // 解析 JSON (producer.py推送json数据)
            val orderCategory = (parsed \ "order_category").extract[String]
            val orderName = (parsed \ "order_name").extract[String]
            val isValid = (parsed \ "is_valid").extract[String]
            val quantity = (parsed \ "order_quantity").extract[Int]
            Some((orderCategory, orderName, isValid, quantity))  // 返回封装后的元组
          } catch {
            case e: Exception =>
              logger.error(s"解析错误: ${e.getMessage}")   // 记录解析错误
              None  // 解析失败时，过滤掉该记录
          }
        }

        // Step 3.2: 聚合统计 - 订单有效性统计
        // 根据 isValid 字段（Y/N）统计订单数量
        val orderStats = data.map {
          case (_, _, isValid, quantity) => (isValid, quantity)
        }.reduceByKey(_ + _)  // 按 isValid 聚合数量
          .collectAsMap()  // 将结果收集为 Map

        // Step 3.3: 聚合统计 - 按订单名称统计有效和无效订单数量
        val orderNumberStatsRDD = data.map {
          case (_, orderName, isValid, quantity) => ((orderName, isValid), quantity)
        }.reduceByKey(_ + _)  // 按 (orderName, isValid) 聚合数量

        // 将聚合结果转换为 Map 格式，按订单名称分组
        val orderNumberStats = orderNumberStatsRDD.collect().groupBy(_._1._1).mapValues { list =>
          val y = list.filter(_._1._2 == "Y").map(_._2).sum  // 统计有效订单数量
          val n = list.filter(_._1._2 == "N").map(_._2).sum  // 统计无效订单数量
          Map("Y" -> y, "N" -> n)  // 封装为 Map(y可当作Y的值)
        }

        // Step 3.4: 聚合统计 - 按订单类别统计有效、无效订单数量及总数量
        val orderCategoryStatsRDD = data.map {
          case (category, _, isValid, quantity) => ((category, isValid), quantity)
        }.reduceByKey(_ + _)  // 按 (category, isValid) 聚合数量

        // 将聚合结果转换为 Map 格式，按订单类别分组
        val orderCategoryStats = orderCategoryStatsRDD.collect().groupBy(_._1._1).mapValues { list =>
          val y = list.filter(_._1._2 == "Y").map(_._2).sum
          val n = list.filter(_._1._2 == "N").map(_._2).sum
          val total = y + n  // 计算总数量
          Map("Y" -> y, "N" -> n, "total_quantity" -> total)  // 封装为 Map
        }

        // Step 3.5: 构建最终结果 Map
        val result = Map(
          "order_stats" -> Map(
            "valid" -> orderStats.getOrElse("Y", 0),  // 有效订单数量
            "invalid" -> orderStats.getOrElse("N", 0)  // 无效订单数量
          ),
          "order_category_stats" -> orderCategoryStats,  // 类别统计
          "order_number_stats" -> orderNumberStats  // 订单名称统计
        )

        // Step 3.6: 将结果转换为 JSON 字符串
        val jsonResult = write(result)  // 使用 json4s 将 Map 转换为 JSON

        // Step 4: Kafka 生产者推送结果
        // 获取 KafkaProducer 实例
        val producer = KafkaProducerSingleton.getInstance(
      sys.env.getOrElse("KAFKA_BROKER", "niit:9092")
    )

        try {
          // 发送到 Kafka 的 processed_orders 主题
          val recordToSend = new ProducerRecord[String, String]("processed_orders", null, jsonResult)
          producer.send(recordToSend)  // 异步发送消息
          logger.info(s"发送到 processed_orders 的数据: $jsonResult")  // 记录发送日志
        } catch {
          case e: Exception =>
            logger.error("发送到 Kafka 失败", e)  // 记录发送失败的错误
        }
      }
    }

    // Step 5: 启动流处理，并进行异常处理与资源管理
    try {
      ssc.start()  // 启动 StreamingContext，开始流处理
      ssc.awaitTermination()  // 等待流处理终止
    } catch {
      case e: Exception =>
        logger.error("StreamingContext 发生异常", e)  // 捕获并记录异常
    } finally {
       // 无论是否异常，最终都会执行以下代码
      KafkaProducerSingleton.close()  // 关闭 KafkaProducer，释放资源
      logger.info("关闭 StreamingContext")  // 记录关闭日志
    }
  }

