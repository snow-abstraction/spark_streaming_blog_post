import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object PriceAggregator {
  def main(args: Array[String]) {
    if (args.length < 3) {
      val usage = ("Usage: PriceAggregator <hostname> <port>"
        ++ " <batch interval in ms>")
      System.err.println(usage)
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("PriceAggregator")
    val batchInterval = args(2).toInt
    val ssc = new StreamingContext(sparkConf, Milliseconds(batchInterval))
    ssc.checkpoint("./checkpoints")

    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    val idAndPriceStrs = lines.map(_.split(","))
    val idAndPricesStream = 
      idAndPriceStrs.map(x => (x(0).toInt, x(1).toDouble))
    
    // more real-world code should probably window operations instead
    val idAndPricesState = 
      idAndPricesStream.updateStateByKey[Double](dropOldForFirstNewPrice)

    idAndPricesState.print()
    ssc.start()
    ssc.awaitTermination()
  }

  val dropOldForFirstNewPrice =
    (newPrices: Seq[Double], oldPrice: Option[Double]) => {
      val newPrice = newPrices.find(_ != Double.NaN)
      newPrice match {
        case Some(price) => newPrice
        case _ => Some(oldPrice.getOrElse(Double.NaN))
      }
    }

}
