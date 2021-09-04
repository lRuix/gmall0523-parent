package com.atguigu.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @Author iRuiX
 * @Date 2021/3/7 15:30
 * @Version 1.0
 * @Desc 获取Jedis客户端的工具类
 */
object MyRedisUtil {

  //定义一个连接池对象
  private var jedisPool: JedisPool = null

  //创建JedisPool连接池对象
  def build(): Unit = {
    val prop = MyPropertiesUtil.load("config.properties")
    val host = prop.getProperty("redis.host")
    val port = prop.getProperty("redis.port")

    val jedisPoolConfig = new JedisPoolConfig
    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMaxIdle(20) //最大空闲
    jedisPoolConfig.setMinIdle(20) //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接时进行测试

    jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
  }

  //获取Jedis客户端
  def getJedisClient(): Jedis = {
    if (jedisPool == null) {
      build()
    }
    jedisPool.getResource
  }
/**
  def main(args: Array[String]): Unit = {
    val jedis = getJedisClient()
    println(jedis.ping())
  }
 */
}

