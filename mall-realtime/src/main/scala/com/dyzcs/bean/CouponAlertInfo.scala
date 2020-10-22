package com.dyzcs.bean

/**
 * Created by Administrator on 2020/10/22.
 */
case class CouponAlertInfo(mid: String,
                           uids: java.util.HashSet[String],
                           itemIds: java.util.HashSet[String],
                           events: java.util.List[String],
                           ts: Long)