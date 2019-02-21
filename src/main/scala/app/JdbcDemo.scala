package app

import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

case class WordCount(word:String, count:Int)
object JdbcDemo {
  def main(args: Array[String]): Unit = {
    //加载数据库配置信息   默认加载db.default.*
    DBs.setup()

    //加载db.sheep配置信息
    //DBs.setup('sheep)

    /**
      * 查询数据并返回单个列，并将数据封装到集合
      */
    val list = DB readOnly {
      implicit session => SQL("select * from wordcount")
        .map(rs => {
          /*(
            rs.string("words"),
            rs.int(2)
          )*/
          WordCount(rs.string("words"), rs.int(2))
          })
        .list().apply()
    }
    list.foreach(println)

    /**
      * 事物插入
      */
    val insertResult = DB.localTx {
      implicit session =>
        SQL("insert into wordcount(words, count) values(?, ?)").bind("hadoop", "10").update().apply()

        SQL("insert into wordcount(words, count) values(?, ?)").bind("php", "10").update().apply()
    }

    /**
      * 删除数据
      */
    val deleteResult = DB.autoCommit {
      implicit session =>
        SQL("delete from wordcount where words=? ").bind("hadoop")
          .update().apply()
    }

  }
}
