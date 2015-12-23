package edu.thu.ss.PPTSearchEngine

import edu.thu.ss.PPTSearchEngine.Properties.Config
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object App {

  def main(args: Array[String]) {
    
    /*
    val query = Config.getString("query")
    val document = "李易峰 1905 电影 网 讯 近日 香港 导演 林岭东 最新 动作 电影 飞 霜 曝光 两岸 三 地 主演 阵容 内地 的 李易峰 张静初 袁泉 香港 的 吴彦祖 林家栋 任达华 谢天华 台湾 的 张孝全 郭采洁 王 识 贤 等 都 将 出演 该片 据悉 影片 将 于 9月 开机 拍摄 尽管 飞 霜 尚未 曝光 剧情 但 影片 云集 两岸 三 地 明星 的 主演 阵容 已经 让 该 片 备受 关注 有 消息 称 导演 林岭东 专程 到 台湾 宜 兰 勘 景 而 吴彦祖 与 李易峰 两 大 男 神 也 将 在 片中 挑战 激烈 动作 戏 份 无疑 为 影片 增加 看 点之前 李易峰 为 飞 霜 试 镜 的 片段 一经 曝光 已然 引发 了 粉丝 的 狂热 转发 林岭东 曾 凭 风云 三部曲 龙 虎 风云 监狱 风云 学校 风云 形成 自己 独特 的动作 影片 风格 而 自从 2007年 与 徐克 杜琪峰 合作 执导 铁三角 之后 已经 七 年 没有 新作 推出 他 的 最新 作品 谜 城 将 于 7月 30日 在 内地 上映 而 之后 林岭东 将 在 9月 开拍 飞 霜 据悉 他 还 将 拍摄 一 部 由 古天乐 和 刘青云 主演 的 警 匪 动作 片 看来 沉寂 多年 的他 也 开始 回归 到 拍 片 行列 中 了 本文 来源 m1905 电影 网 授权 刊载"
    .split(" ")
    val KeyWords = query.split(" ").map { keyword => document.indexOf(keyword) }.filter(index => index != -1)
    val index = KeyWords.map { index =>
      (index - 10 to index + 10)
        .toArray
    }.fold(Array[Int]())((a, b) => a ++ b)
    .filter(number => number >= 0 && number < 102).toSet.toList.sorted
    //index.foreach { println }

    def insertDot(index: List[Int]): List[Int] = {
      index match {
        case n1 :: n2 :: ns => {
          //println(n1+"~"+n2)
          if (n2 - n1 > 2) n1 :: (-1) :: insertDot(n2::ns) else n1 :: insertDot(n2 :: ns)
        }
        case ns             => ns
      }
    }

    println("====================")
    insertDot(index).map { number => if (number == -1) "..." else document(number) }.fold("")((a, b) => a ++ b)
    .foreach { println }
    
     val conf = new SparkConf().setAppName("app").setMaster("local[5]")
     conf.set("spark.driver.allowMultipleContexts" , "true") 
    val sc = new SparkContext(conf)
   
    val searcher =  PPTSearcher

    val result = searcher.search("吴彦祖")
    var resStr = ""
    var i = 0
    while(i < result.RetrievalDoc.length){
      resStr += result.RetrievalDoc(i).DocId + "|" + result.RetrievalDoc(i).Title + "|" +result.RetrievalDoc(i).Url + "|" +   result.RetrievalDoc(i).Date + "|" + result.RetrievalDoc(i).label + "|" +result.RetrievalDoc(i).Abstract + "||\n" 
    i+= 1
    }
     println(resStr)
    */
    /*
    val conf = new SparkConf().setAppName("app").setMaster("local")
    val sc = new SparkContext(conf)
    val docIds = Array(3504)
    val data = sc.textFile(Config.getString("dataDir"))
     .filter { line => docIds.contains(line.split('|').head.toInt) }
      .map { line => line.split('|') }.collect()
      println(data.size)
      * 
    val searcher =  PPTSearcher

    val result = searcher.search("吴彦祖")
    result.RetrievalDoc.foreach { doc => println(doc.DocId+doc.Abstract) }
    */
    
    val conf = new SparkConf().setAppName("app").setMaster("local")
    val sc = new SparkContext(conf)
    
    val rs = new PPTResultSet(Array(),"123",4,sc)
    println(rs.RetrievalDoc.length)
  }

}

