package com.atguigu.gmall.realtime.util

import java.util.Properties

import com.fasterxml.jackson.databind.deser.std.StringArrayDeserializer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 * @Author iRuiX
 * @Date 2021/3/7 14:41
 * @Version 1.0
 * @Desc 读取kafka的工具类
 */
object MyKafkaUtil {

  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  private val broker_list: String = properties.getProperty("kafka.broker.list")

  //kafka消费者配置
  private val kafkaParam = collection.mutable.Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> "gmall0523_group",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
  )

  // 创建 DStream，返回接收到的输入数据
  def getKafkaStream(topic: String,ssc:StreamingContext ): InputDStream[ConsumerRecord[String,String]]={
    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParam )
    )
    dStream
  }

  def getKafkaStream(topic: String,ssc:StreamingContext,groupId:String): InputDStream[ConsumerRecord[String,String]]={
    kafkaParam(ConsumerConfig.GROUP_ID_CONFIG)=groupId
    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam ))
    dStream
  }

  def getKafkaStream(topic: String,ssc:StreamingContext,offsets:Map[TopicPartition,Long],groupId:String): InputDStream[ConsumerRecord[String,String]]={
    kafkaParam("group.id")=groupId
    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam,offsets))
    dStream
  }
}
