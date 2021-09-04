package com.atguigu.gmall.realtime.bean

/**
 * @Author iRuiX
 * @Date 2021/3/7 20:31
 * @Version 1.0
 * @Desc xxxxx
 */
case class DauInfo(
                    mid:String,//设备 id
                    uid:String,//用户 id
                    ar:String,//地区
                    ch:String,//渠道
                    vc:String,//版本
                    var dt:String,//日期
                    var hr:String,//小时
                    var mi:String,//分钟
                    ts:Long //时间戳
                  ) {}
