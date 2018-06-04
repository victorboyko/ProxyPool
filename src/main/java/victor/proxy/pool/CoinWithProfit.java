package victor.proxy.pool;

public class CoinWithProfit {
	String coinAbr;
	Double profitability;
	
	public CoinWithProfit(String coinAbr, Double profitability) {
		this.coinAbr = coinAbr;
		this.profitability = profitability;
	}
//	
//	public void fillFrom(CoinWithProfit cwp) {
//		this.coinAbr = cwp.coinAbr;
//		this.profitability = cwp.profitability;
//	}
		
	@Override
	public String toString() {
		return String.format("[%s,%4.2f]", coinAbr, profitability);
	}
}
