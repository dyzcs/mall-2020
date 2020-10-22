package com.dyzcs.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.dyzcs.bean.{CouponAlertInfo, EventLog}
import com.dyzcs.constants.MallConstant
import com.dyzcs.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
 * Created by Administrator on 2020/10/22.
 */
object AlertApp {
    def main(args: Array[String]): Unit = {
        // 1.创建SparkConf
        val sparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

        // 2.创建StreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // 创建时间转换对象
        val sdf = new SimpleDateFormat("yyyy-MM-dd")

        // 3.读取Kafka数据转换流
        val kafkaDStream = MyKafkaUtil.getKafkaStream(ssc, Set(MallConstant.MALL_EVENT))

        // 4.将每一条数据转换为样例类对象
        val eventLogDStream = kafkaDStream.map(record => {
            // a.将record转换为样例类对象
            val eventLog = JSON.parseObject(record.value(), classOf[EventLog])

            // b.处理日期和时间
            val dateHourStr = sdf.format(new Date(eventLog.ts))
            val dateHourArr = dateHourStr.split(" ")
            eventLog.logDate = dateHourArr(0)
            eventLog.logHour = dateHourArr(1)

            // c.返回结果
            eventLog
        })

        // 5.开窗
        val eventLogWindowDStream = eventLogDStream.window(Seconds(30))

        // 6.转换数据结构并按照mid分组
        val midToLogIterDStream: DStream[(String, Iterable[EventLog])] =
            eventLogWindowDStream.map(log => (log.mid, log)).groupByKey()

        // 7.根据登陆用户个数以及操作行为进行筛选
        val boolToCouponAlertInfo: DStream[(Boolean, CouponAlertInfo)] =
            midToLogIterDStream.map { case (mid, logIter) =>
                // a.存放三个集合用于存放结果数据
                val uids = new util.HashSet[String]()
                val itemIds = new util.HashSet[String]()
                val events = new util.ArrayList[String]()

                // 定义一个标签，用于判断是否有点击数据行为
                var noClick: Boolean = true

                // b.遍历迭代器
                breakable {
                    logIter.foreach(eventLog => {
                        // 提取事件类型
                        val evid = eventLog.evid

                        // 向事件集合放入数据
                        events.add(evid)

                        // 判断是否为领劵行为
                        if ("coupon".equals(evid)) {
                            uids.add(eventLog.uid)
                            itemIds.add(eventLog.itemid)
                        } else if ("clickItem".equals(evid)) {
                            noClick = false
                            break
                        }
                    })
                }

                // c.返回结果
                (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
            }

        // 过滤出需要预警的日志信息
        val result = boolToCouponAlertInfo
                .filter(_._1)
                .map { case (_, alertLog) =>
                    // 获取时间戳
                    val ts = alertLog.ts
                    val min = ts / 1000 / 60

                    // 返回结果
                    (s"${alertLog.mid}_$min", alertLog)
                }

        result.print()

        // 8.写入ES
//        result.foreachRDD(rdd => {
//            // 按照分区写入
//            rdd.foreachPartition(iter => {
//                MyEsUtil.insertBulk(MallConstant.MALL_COUPON_ALERT, iter.toList)
//            })
//        })

        // 9.启动任务
        ssc.start()
        ssc.awaitTermination()
    }
}
