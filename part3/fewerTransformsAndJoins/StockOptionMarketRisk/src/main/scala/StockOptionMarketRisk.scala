import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import InstrumentParameters._
import StockOptionPricing._

import akka.zeromq.Subscribe
import akka.util.ByteString
import org.apache.spark.streaming.zeromq._

object StockOptionMarketRisk {
  val shiftSize = 0.001
  val riskFreeRate = 0.05
  val pricer = new Black76StockOptionPricer(riskFreeRate)

  def main(args: Array[String]) {
    if (args.length != 3) {
      val usage = ("Usage: StockOptionMarketRisk <zeroMQurl> <topic>"
        ++ " <batch interval in ms>")
      System.err.println(usage)
      System.exit(1)
    }

    val Seq(url, topic, interval) = args.toSeq

    val ssc =  createStreamContext(interval.toInt)
    val stockPrices = createStockPricesStream(url, topic, ssc)
    val stockIdVolOptions = createStaticInstrumentData(ssc.sparkContext)

    // use transform to join RDD to DStream
    val idPricesValulationInfoNested = stockPrices.transform(
      (x: rdd.RDD[(Int, Double)]) => x.join(stockIdVolOptions))

    // change key from instrumentID of stock to that of options and
    // collect valuation paramaters
    // ? introduce classes to avoid all this tuple indexing ?
    val optionValulationData = idPricesValulationInfoNested.map(
      y => (y._2._2._2.instrumentID,
            StockOptionValuation(y._2._1, y._2._2._1, y._2._2._2)) )

    val priceAndDelta = optionValulationData.map(
      y => (y._1, priceAndDeltaFromOriginal(y._2)))

    // print (optionID, (option price, delta))
    priceAndDelta.print()
    ssc.start()
    ssc.awaitTermination()
  }

  val shiftMarketPrice =
    (param : StockOptionValuation) => { param.copy(underLyingMarketPrice = 
      param.underLyingMarketPrice + shiftSize) }

  val priceAndDeltaFromOriginal = 
        (p : StockOptionValuation) => {
          val param = p.optionParam
          val price = pricer.price(
            param.timeUntilMaturity,
            param.strikePrice,
            param.optionRight,
            p.volatility,
            _ : Double)

          val optionPrice = price(p.underLyingMarketPrice) 
          val shiftedUnderlyingPrice = p.underLyingMarketPrice + shiftSize
          val delta = (price(shiftedUnderlyingPrice) - optionPrice) / shiftSize
          (optionPrice, delta)
        }

  val dropOldForFirstNewPrice =
    (newPrices: Seq[Double], oldPrice: Option[Double]) => {
      val newPrice = newPrices.find(_ != Double.NaN)
      newPrice match {
        case Some(price) => newPrice
        case _ => Some(oldPrice.getOrElse(Double.NaN))
      }
    }

  private def createStreamContext(batchInterval : Int) = {
    val sparkConf = new SparkConf().setAppName("StockOptionMarketRisk")
    val ssc = new StreamingContext(sparkConf, Milliseconds(batchInterval))
    ssc.checkpoint("./checkpoints") 
    ssc
  }

  private def createStockPricesStream(
    url : String,
    topic : String, 
    ssc : StreamingContext) = {
    def bytesToStringIterator(x: Seq[ByteString]) = (x.map(_.utf8String)).iterator

    val lines = ZeroMQUtils.createStream(ssc, url, Subscribe(topic), bytesToStringIterator _)
    val idAndPriceStrs = lines.map(_.split(","))
    val idAndPricesStream =
      idAndPriceStrs.map(x => (x(0).toInt, x(1).toDouble))
    // better done with window operation
    idAndPricesStream.updateStateByKey[Double](dropOldForFirstNewPrice)
  }

  private def createStaticInstrumentData(sc : SparkContext) = {
     val stockIdAndVolatility = sc.parallelize(List(
      (1, 0.30),
      (2, 0.35),
      (3, 0.60),
      (4, 0.40)))

    val stockIdAndOptionParam = sc.parallelize(List(
      (1, StockOption(101, 0.25, 50.0, Call())),
      (2, StockOption(202, 0.50, 70.0, Put())),
      (2, StockOption(203, 0.50, 70.0, Call())),
      (3, StockOption(301, 0.60,  5.0, Call())),
      (3, StockOption(302, 0.75,  5.0, Put())),
      (4, StockOption(401, 1.00, 100.0, Call()))))

    // (id, (volatility, StockOption))
    stockIdAndVolatility.join(stockIdAndOptionParam)
  }
}
