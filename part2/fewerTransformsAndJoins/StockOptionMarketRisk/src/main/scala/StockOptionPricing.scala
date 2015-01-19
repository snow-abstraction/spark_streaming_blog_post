package StockOptionPricing
import InstrumentParameters._

class Black76StockOptionPricer(riskFreeRate : Double) {
  require(0.0 <= riskFreeRate)
  import org.apache.commons.math3.distribution.NormalDistribution
  private val gaussDist = new NormalDistribution()

  def price(
    timeUntilMaturity      : Double,
    strikePrice            : Double,
    optionRight            : OptionRight,
    volatility             : Double,
    underlyingSpotPrice    : Double) = {
    val s = underlyingSpotPrice
    val forwardPrice = s * math.exp(riskFreeRate * timeUntilMaturity)
    val volTime = volatility * math.sqrt(timeUntilMaturity)
    val zeroCouponRate = math.exp(-riskFreeRate * timeUntilMaturity)
    val d1 = math.log(forwardPrice / strikePrice) / volTime + 0.5*volTime
    val d2 = d1 - volTime
    def n(x : Double) = gaussDist.cumulativeProbability(x)

   optionRight match {
      case Call() => s * n(d1) - zeroCouponRate * strikePrice * n(d2)
      case Put()  => zeroCouponRate * strikePrice * n(-d2) - s * n(-d1)
    }
  }
}

