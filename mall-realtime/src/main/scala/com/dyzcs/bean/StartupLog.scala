package com.dyzcs.bean

/**
 * Created by Administrator on 2020/10/16.
 */
case class StartupLog(mid: String,
                      uid: String,
                      appid: String,
                      area: String,
                      os: String,
                      ch: String,
                      `type`: String,
                      vs: String,
                      var logDate: String,
                      var logHour: String,
                      var ts: Long)
