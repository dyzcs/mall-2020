package com.dyzcs.handler

import com.dyzcs.bean.StartupLog
import com.dyzcs.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by Administrator on 2020/10/16.
 */
object DauHandler {
    /**
     * 将两次过滤后的数据集中的Mid写入Redis
     *
     * @param filterByMidGroupLogDStream 两次过滤后的数据集
     */
    def saveMidToRedis(filterByMidGroupLogDStream: DStream[StartupLog]): Unit = {
        // 将数据转换为RDD进行操作
        filterByMidGroupLogDStream.foreachRDD(rdd => {
            // 对每个分区单独写库
            rdd.foreachPartition(iter => {
                // a.获取Redis连接
                val jedisClient = RedisUtil.getJedisClient

                // b.写库操作
                iter.foreach(log => jedisClient.sadd(s"dau:${log.logDate}", log.mid))

                // c.释放连接
                jedisClient.close()
            })
        })
    }

    /**
     * 对经过Redis去重之后的数据集，按照Mid分组进行去重
     *
     * @param filterByRedisLogDStream redis过滤后数据
     * @return 过滤后数据集
     */
    def filterByMidGroup(filterByRedisLogDStream: DStream[StartupLog]): DStream[StartupLog] = {
        // 转换数据结构 log => (mid, log)
        val midToLogDStream = filterByRedisLogDStream.map(log => (log.mid, log))

        // 按照mid分组
        val midToLogIterDStream = midToLogDStream.groupByKey()

        // 组内取时间第一条
        val filterByMidGroupLogDStream = midToLogIterDStream.flatMapValues(iter => {
            // 按照时间戳排序并取第一条
            iter.toList.sortWith(_.ts < _.ts).take(1)
        })

        // 返回值
        filterByMidGroupLogDStream.map(_._2)
    }

    /**
     * 根据Redis中数据进行去重
     *
     * @param startupLogDStream 原始数据集
     * @return 过滤后数据集
     */
    def filterByRedis(startupLogDStream: DStream[StartupLog]): DStream[StartupLog] = {
        // 将数据转换为RDD
        val filterByRedisLogDStream = startupLogDStream.transform(rdd => {
            // 对RDD的每个分区单独处理，减少连接的创建
            val filterRDD = rdd.mapPartitions(iter => {
                // a.获取redis连接
                val jedisClient = RedisUtil.getJedisClient
                // b.过滤
                val logs = iter.filter(log => !jedisClient.sismember(s"dau:${log.logDate}", log.mid))
                // c.关闭
                jedisClient.close()
                // d.返回过滤后的数据
                logs
            })
            // 返回过滤后的数据
            filterRDD
        })
        // 当前根据redis过滤方法的返回值
        filterByRedisLogDStream
    }

}
