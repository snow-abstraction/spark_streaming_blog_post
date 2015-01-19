package InstrumentParameters

sealed abstract class OptionRight
case class Call() extends OptionRight
case class Put() extends OptionRight

case class StockOption(
  instrumentID           : Int,
  timeUntilMaturity      : Double,
  strikePrice            : Double,
  optionRight            : OptionRight)

case class StockOptionValuation(
  underLyingMarketPrice  : Double,
  volatility             : Double,
  optionParam            : StockOption)

