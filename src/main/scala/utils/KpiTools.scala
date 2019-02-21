package utils

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object KpiTools {

  /**
    * 数据处理
    * @param rdd
    * @return
    */
  def baseDataRDD(rdd: RDD[ConsumerRecord[String, String]]) = {
    rdd.map(cr => JSON.parseObject(cr.value()))
      .filter(obj => obj.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
      .map(obj => {
        //判断这条日志是否是充值成功的日志
        val result = obj.getString("bussinessRst")
        //获取充值金额
        val fee = obj.getDouble("chargefee")

        //充值发起时间和结束时间
        val requestId = obj.getString("requestId")
        //数据当前时间
        val day = requestId.substring(0, 8)
        val hour = requestId.substring(8, 10)
        val minute = requestId.substring(10, 12)
        val receiveTime = obj.getString("receiveNotifyTime")
        //省份
        val province = obj.getString("provinceCode")

        //取得充值时长
        val costTime = CaculateTools.caculateTime(requestId, receiveTime)
        val succAndFeeAndTime: (Double, Double, Double) = if (result.equals("0000")) (1, fee, costTime) else (0, 0, 0)

        //日期，小时，List（订单数，成功订单，充值金额，充值时长）
        (day, hour, List[Double](1, succAndFeeAndTime._1, succAndFeeAndTime._2, succAndFeeAndTime._3), province, minute)
      }).cache()
  }

  /**
    * 业务概述-每小时充值情况
    */
  def kpi_general_hour(baseData: RDD[(String, String, List[Double], String, String)]) = {
    baseData.map(tp => ((tp._1, tp._2), List(tp._3(0), tp._3(1))))
      .reduceByKey((list1, list2) => {
      list1.zip(list2).map(tp => tp._1 + tp._2)
    }).foreachPartition(partition => {
      val jedis = Jpools.getJedis
      partition.foreach(tp => {
        //总的充值成功和失败的数量
        jedis.hincrBy("B-" + tp._1._1, "total" + tp._1._2, tp._2(0).toLong)
        //充值成功的数量
        jedis.hincrBy("B-" + tp._1._1, "success" + tp._1._2, tp._2(1).toLong)
        //设置key的过期时间
        jedis.expire("B-" + tp._1._1, 60 * 60 * 48)
      })
      jedis.close()
    })
  }

  /**
    * 业务概述（总订单量，充值订单量，总金额，业务时间）
    *
    * @param baseData
    */
  def kpi_general(baseData: RDD[(String, String, List[Double], String, String)]) = {
    baseData.map(tp => (tp._1, tp._3)).reduceByKey((list1, list2) => {
      list1.zip(list2).map(tp => tp._1 + tp._2)
    }).foreachPartition(partition => {
      val jedis = Jpools.getJedis
      partition.foreach(tp => {
        jedis.hincrBy("A-" + tp._1, "total", tp._2(0).toLong)
        jedis.hincrBy("A-" + tp._1, "success", tp._2(1).toLong)
        jedis.hincrByFloat("A-" + tp._1, "money", tp._2(2))
        jedis.hincrBy("A-" + tp._1, "cost", tp._2(3).toLong)

        //设置key的过期时间
        jedis.expire("A-" + tp._1, 60 * 60 * 48)
      })
      jedis.close()
    })
  }

  /**
    *业务质量
    */
  def kpi_quality(baseData: RDD[(String, String, List[Double], String, String)], p2p: Broadcast[Map[String, AnyRef]]) = {
    baseData.map(tp => ((tp._1, tp._4), tp._3(1))).reduceByKey(_+_).foreachPartition(partition => {
      val jedis = Jpools.getJedis
      partition.foreach(tp => {
        //总的充值成功和失败的数量
        jedis.hincrBy("C-" + tp._1._1, p2p.value.getOrElse(tp._1._2, tp._1._2).toString, tp._2.toLong)
       //设置key的过期时间
        jedis.expire("C-" + tp._1._1, 60 * 60 * 48)
      })
      jedis.close()
    })
  }

  /**
    * 实时统计每分钟的充值金额和订单量
    * @param baseData
    * @return
    */
  def kpi_realtime_minute(baseData: RDD[(String, String, List[Double], String, String)]) = {
    baseData.map(tp =>
      ((tp._1, tp._2, tp._5), List(tp._3(1), tp._3(2)))).reduceByKey((list1, list2) => {
      list1.zip(list2).map(tp => tp._1 + tp._2)
    }).foreachPartition(partition => {
      val jedis = Jpools.getJedis
      partition.foreach(tp => {
        //每分钟充值成功笔数和金额
        jedis.hincrBy("D-" + tp._1._1,  "C:"+ tp._1._2 + tp._1._3, tp._2(0).toLong)
        jedis.hincrByFloat("D-" + tp._1._1,  "M:"+ tp._1._2 + tp._1._3, tp._2(1))
        //设置key的过期时间
        jedis.expire("D-" + tp._1._1, 60 * 60 * 48)
      })
      jedis.close()
    })
  }

}
