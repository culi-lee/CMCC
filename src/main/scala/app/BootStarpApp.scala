package app

/**
  * bin/flume-ng agent -c conf -f myconf/avro-logger.conf -n a1 -Dflume.root.logger=INFO,console
  */

import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import utils.{AppParams, Jpools}

/**
  * 读取kafka中的数据
  */
object BootStarpApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName("中国移动运营实时监控平台-Monitor")
    //如果在集群上运行的话，需要去掉：sparkConf.setMaster（“local[*]”）
    sparkConf.setMaster("local[*]")
    //将rdd以序列化格式来保存以减少内存的占用
    //默认采用org.apache.spark.serializer.JavaSerializer
    //这是最基本的优化  对于优化<网络性能>极为重要，将RDD以序列化格式来保存减少内存占用
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //rdd压缩
    sparkConf.set("spark.rdd.compress", "true")
    //batchSize = partitionNum * 100(分区数量) * 采样时间    参与控制吞吐量
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100")
    //优雅的停止
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(2))

    //获取kafka的数据
    //定期地从kafka的topic+partition中查询最新的偏移量
    // 再根据偏移量范围在每个batch里面处理数据
    // 使用的是kafka的简单消费者api
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      //位置策略（可用的Executor上均匀分配分区）
      LocationStrategies.PreferConsistent,
      //消费策略（订阅固定的主题集合）
      ConsumerStrategies.Subscribe[String, String](AppParams.topic, AppParams.kafkaParams)
    )

    /**
      * 数据处理
      */
    stream.foreachRDD(rdd => {
      // val baseRdd = rdd.map(rd => (rd.value()))
      // rdd.foreach(println)
      //取得所有充值通知日志
      /**  equalsIgnoreCase
        * 将此 String 与另一个 String 进行比较，不考虑大小写。如果两个字符串的长度相等，
        * 并且两个字符串中的相应字符都相等（忽略大小写），则认为这两个字符串是相等的。
        */
      val baseData: RDD[JSONObject] = rdd.map(cr => JSON.parseObject(cr.value()))
        .filter(obj => obj.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))

      //wordCount  flatMap -> map -> reduceByKey
      /**
        * 获取到充值成功的订单笔数
        */
      val totalSucc: RDD[(String, Int)] = baseData.map(obj => {
        val reqId = obj.getString("requestId")

        //获取日期
        val day = reqId.substring(0, 8)
        //取出该条充值是否成功的标志
        val result = obj.getString("bussinessRst")
        val flag = if (result.equals("0000")) 1 else 0
        (day, flag)
      }).reduceByKey(_+_)

      /**
        * 获取到充值成功的订单金额
        */
      val totalMoney = baseData.map(obj => {
        val reqId = obj.getString("requestId")

        //获取日期
        val day = reqId.substring(0, 8)
        //取出该条充值是否成功的标志
        val result = obj.getString("bussinessRst")
        val fee = if (result.equals("0000")) obj.getString("chargefee").toDouble else 0
        (day, fee)
      }).reduceByKey(_+_)

      //总订单量
      val total = baseData.count()

      /**
        * 获取充值成功的充值时长
        */
      val totalTime = baseData.map(obj => {
        var reqId = obj.getString("requestId")
        //获取日期
        val day = reqId.substring(0, 8)

        //取出该条充值是否成功的标志
        val result = obj.getString("bussinessRst")
        //时间格式为yyyyMMddHHmmssSSS（年月日时分秒）
        val endTime = obj.getString("receiveNotifyTime")
        val startTime = reqId.substring(0, 17)

        val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
        val cost = if (result.equals("0000")) format.parse(endTime).getTime - format.parse(startTime).getTime else 0

        (day, cost)
      }).reduceByKey(_+_)

      //将充值成功的订单数写入到redis
      totalSucc.foreachPartition(itr => {
        val jedis = Jpools.getJedis
        itr.foreach(tp => {
          jedis.incrBy("CMCC-"+tp._1, tp._2)
        })
        jedis.close()
      })
  })

    //启动程序
    ssc.start()
    //等待程序被中断
    ssc.awaitTermination()
  }
}

