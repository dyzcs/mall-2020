package com.dyzcs.app

import com.alibaba.fastjson.JSON
import com.dyzcs.bean.OrderInfo
import com.dyzcs.constants.MallConstant
import com.dyzcs.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Administrator on 2020/10/21.
 */
object OrderApp {
    def main(args: Array[String]): Unit = {
        // 1.创建SparkConf
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
        // 创建StreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // 3.读取kafka订单主题数据
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtil.getKafkaStream(ssc, Set(MallConstant.MALL_ORDER_INFO))

        // 4.转换为样例类对象
        val orderInfoDStream = kafkaDStream.map(record => {
            // a.转换为样例类对象
            val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

            // b.手机号脱敏
            val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
            orderInfo.consignee_tel = telTuple._1 + "*******"

            // c.处理订单生成的日期
            val create_time = orderInfo.create_time
            val timeArr = create_time.split(" ")
            orderInfo.create_date = timeArr(0)
            orderInfo.create_hour = timeArr(1).split(":")(0)

            // d.返回结果
            orderInfo
        })

        // 5.将数据斜土phoenix
        orderInfoDStream.foreachRDD(rdd => {
            import org.apache.phoenix.spark._
            rdd.saveToPhoenix("MALL_ORDER_INFO",
                Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT",
                    "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID",
                    "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS",
                    "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID",
                    "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
                new Configuration,
                Some("s183:2181"))
        })

        // 6.启动任务
        ssc.start()
        ssc.awaitTermination()
    }
}
