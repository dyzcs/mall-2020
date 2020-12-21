package com.dyzcs.app

import com.alibaba.fastjson.JSON
import com.dyzcs.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.dyzcs.constants.MallConstant
import com.dyzcs.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats

import scala.collection.mutable.ListBuffer

/**
 * Created by Administrator on 2020/12/20.
 */
object SaleApp {
    def main(args: Array[String]): Unit = {
        //1.创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("SaleApp").setMaster("local[*]")

        //2.创建StreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        //3.消费3个主题的数据
        val orderInfoKafkaDStream = MyKafkaUtil.getKafkaStream(ssc, Set(MallConstant.MALL_ORDER_INFO))
        val orderDetailKafkaDStream = MyKafkaUtil.getKafkaStream(ssc, Set(MallConstant.MALL_ORDER_DETAIL))
        val userInfoKafkaDStream = MyKafkaUtil.getKafkaStream(ssc, Set(MallConstant.MALL_USER_INFO))

        //4.将OrderInfo以及OrderDetail数据转换为样例类对象,同时将数据转换为KV结构
        val orderInfoDStream = orderInfoKafkaDStream.map(record => {
            //a.转换为样例类对象
            val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
            //b.手机号脱敏
            val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
            orderInfo.consignee_tel = telTuple._1 + "*******"
            //c.处理订单生成的日期和时间
            val create_time: String = orderInfo.create_time
            val timeArr: Array[String] = create_time.split(" ")
            orderInfo.create_date = timeArr(0)
            orderInfo.create_hour = timeArr(1).split(":")(0)
            //d.返回数据
            (orderInfo.id, orderInfo)
        })

        val orderDetailDStream = orderDetailKafkaDStream.map(record => {
            val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
            (detail.order_id, detail)
        })

        //将用户信息写入Redis做缓存
        userInfoKafkaDStream.foreachRDD(rdd => {
            rdd.foreachPartition(iter => {
                //a.获取Redis连接
                val jedisClient = RedisUtil.getJedisClient

                //b.对于分区中每一条数据进行写入
                iter.foreach(record => {
                    //转换为样例类对象
                    val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
                    //RedisKey
                    val userKey = s"user:${userInfo.id}"
                    //写入Redis
                    jedisClient.set(userKey, record.value())
                })

                //c.关闭连接
                jedisClient.close()
            })
        })

        //5.将OrderInfo和OrderDetail做fullOuterJoin
        //    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDStream.join(orderDetailDStream)
        //    val value1: DStream[(String, (OrderInfo, Option[OrderDetail]))] = orderInfoDStream.leftOuterJoin(orderDetailDStream)
        //    val value2: DStream[(String, (Option[OrderInfo], OrderDetail))] = orderInfoDStream.rightOuterJoin(orderDetailDStream)
        //        val joinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)
        val joinDStream = orderInfoDStream.fullOuterJoin(orderDetailDStream)

        //6.对JOIN的结果进行解析,封装为SaleDetail(没有user信息)
        val noUserSaleDetailDStream = joinDStream.mapPartitions(iter => {

            //a.获取Redis连接
            val jedisClient = RedisUtil.getJedisClient
            //b.创建集合用于存放集合上的数据集
            val listBuffer = new ListBuffer[SaleDetail]()
            import org.json4s.native.Serialization
            implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

            //c.遍历一个分区的内容,进行单条数据处理
            iter.foreach { case (orderId, (orderInfoOpt, orderDetailOpt)) =>

                //定义OrderInfo以及OrderDetail的RedisKey
                val orderKey = s"order:$orderId"
                val detailKey = s"detail:$orderId"

                //一、orderInfoOpt不为空
                if (orderInfoOpt.isDefined) {
                    val orderInfo: OrderInfo = orderInfoOpt.get
                    //1.orderDetailOpt不为空
                    if (orderDetailOpt.isDefined) {
                        val orderDetail: OrderDetail = orderDetailOpt.get
                        listBuffer += new SaleDetail(orderInfo, orderDetail)
                    }

                    //2.由于info数据对detail数据为一对多的关系,则无条件保存至Redis  JsonStr
                    // val str: String = JSON.toJSONString(orderInfo)
                    val orderStr: String = Serialization.write(orderInfo)
                    jedisClient.set(orderKey, orderStr)
                    jedisClient.expire(orderKey, 300)

                    //3.由于info数据对detail数据为一对多的关系,则无条件查询Detail缓存
                    val details = jedisClient.smembers(detailKey)
                    import scala.collection.JavaConversions._
                    details.foreach(orderDetailStr => {
                        //将orderDetailStr转换为OrderDetail对象
                        val orderDetail: OrderDetail = JSON.parseObject(orderDetailStr, classOf[OrderDetail])
                        listBuffer += new SaleDetail(orderInfo, orderDetail)
                    })

                } else {
                    //二、orderInfoOpt为空,则orderDetailOpt必定不为空
                    val orderDetail: OrderDetail = orderDetailOpt.get

                    //查询OrderInfo缓存中是否有对应数据,如果有,则JOIN写出,如果没有,将自身写入缓存
                    if (jedisClient.exists(orderKey)) {
                        //OrderInfo缓存数据存在
                        val orderInfoStr: String = jedisClient.get(orderKey)
                        val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
                        listBuffer += new SaleDetail(orderInfo, orderDetail)

                    } else {
                        //OrderInfo缓存数据不存在
                        val orderDetailStr: String = Serialization.write(orderDetail)
                        jedisClient.sadd(detailKey, orderDetailStr)
                        jedisClient.expire(detailKey, 300)
                    }
                }
            }

            //d.关闭Redis连接
            jedisClient.close()
            //e.返回结果
            listBuffer.toIterator
        })

        //7.查询Redis中的用户信息,将当前的SaleDetail补充完整
        val saleDetailDStream = noUserSaleDetailDStream.mapPartitions(iter => {
            //a.获取Redis连接
            val jedisClient = RedisUtil.getJedisClient
            //b.创建集合,用于存放添加了用户信息的数据
            val details = new ListBuffer[SaleDetail]()
            //c.遍历分区数据,给每一条数据添加用户信息
            iter.foreach(noUserSaleDetail => {
                val userInfoStr: String = jedisClient.get(s"user:${noUserSaleDetail.user_id}")
                val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
                noUserSaleDetail.mergeUserInfo(userInfo)
                details += noUserSaleDetail
            })
            //d.关闭连接
            jedisClient.close()
            //e.返回结果
            details.toIterator
        })

        //8.测试打印,写入ES
        saleDetailDStream.cache()
        saleDetailDStream.print(100)
        val idToSaleDetailDStream = saleDetailDStream.map(saleDetail => (s"${saleDetail.user_id}-${saleDetail.order_id}-${saleDetail.order_detail_id}", saleDetail))

        idToSaleDetailDStream.foreachRDD(rdd => {
            //按照分区进行数据写入
            rdd.foreachPartition(iter =>
                MyEsUtil.insertBulk(MallConstant.MALL_SALE_DETAIL, iter.toList)
            )
        })

        //9.启动
        ssc.start()
        ssc.awaitTermination()
    }
}
