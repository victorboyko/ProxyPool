package victor.proxy.pool;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.PushbackInputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;


import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Main {
	
	private static final Logger logger = Logger.getLogger(Main.class);
	
	private static void exit() {
		System.exit(-121);
	}
	
	
	private static double feePercentage; // in percent, obviously
	private static double feePeriod; // in seconds
	private static Coin feeCoin;
	
	private static Set<String> coinsLimit;
	private static double updateCoinsTime;
	private static int poolPort;
	private static String extPool;
	
	private static Properties props = new Properties();

	private static void readProperties() {
		try {
			String propStr = FileUtils.readFileToString(new File("pool.properties"));
			props.load(new StringReader(propStr.replace("\\","\\\\")));
		} catch (FileNotFoundException e) {
			logger.error("no switcher.properties found");
			exit();
		} catch (IOException e) {
			logger.error("error reading properties: " + e);
			exit();
		}
		updateCoinsTime = Double.valueOf(props.getProperty("minehub.update.frequency").trim());		
		feePercentage = Double.valueOf(props.getProperty("fee.percentage").trim());
		feePeriod = Double.valueOf(props.getProperty("fee.period").trim());
		String feePool = props.getProperty("fee.pool").trim();
		feeCoin = Coin.parse(feePool).values().iterator().next();
		coinsLimit = new HashSet<>(Arrays.asList(props.getProperty("active.coins").trim().split(",")));
		poolPort = Integer.valueOf(props.getProperty("pool.port").trim());
		extPool = props.getProperty("ext.pool").trim();
	}
	
	private static String getStringByURL(URL jsonURL) throws IOException {
		HttpURLConnection conn;
		String jsonText = null;
		InputStream httpIn = null;
		
		try {
			conn = (HttpURLConnection) jsonURL.openConnection();
			
			conn.setRequestMethod("GET");
	        conn.setRequestProperty("Content-Type", 
	                   "application/x-www-form-urlencoded");
	        conn.setRequestProperty("Content-Language", "en-US"); 
	        conn.setRequestProperty("User-Agent",
	                "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11");
	        conn.setUseCaches(false);
	        conn.setDoInput(true);
	        conn.setDoOutput(true);
			
			httpIn = new BufferedInputStream(conn.getInputStream());     
			jsonText = IOUtils.toString(httpIn);
		} finally {
			IOUtils.closeQuietly(httpIn);
		}
		
		return jsonText;
	}
	
//	private static boolean shouldSwitch(double currentProf, double newProf, long timePassed /* millis */) {
//		boolean result = newProf / currentProf > neededIncrease(timePassed);
//		return result;
//	}
//	
//	private static double neededIncrease(long timePassed) {
//		return 1 + 20 / (timePassed / 1000d + 3);
//	}
	
	private static class Coin {
		String coinAbr, host, login, password;
		int port;
		
		private static Map<String, Coin> parse(String str) {
			Map<String, Coin> coins = new HashMap<>();
			String[] coinTexts = str.split(","); // ZEC=user:password@host:port,ZEN=user:password@host:port
			for(String coinText : coinTexts) {
				Coin c = new Coin();
				try { 
					coinText = coinText.trim();
					String[] pair = coinText.split("=");
					c.coinAbr = pair[0];
					pair = coinText.substring(coinText.indexOf('=')+1).split("@");
					String[] userAndPass = pair[0].split(":");
					c.login = userAndPass[0];
					c.password = userAndPass[1];
					pair = pair[1].split(":"); 
					c.host = pair[0];
					c.port = Integer.valueOf(pair[1]);
					coins.put(c.coinAbr, c);
				} catch (Exception e) {
					return null;
				}
			}
			
			
			return coins;
		}
		
		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this);
		}
	}
	
	private static ThreadLocal<Map<String, Coin>> minerCoins = new ThreadLocal<>();
	
	private static ThreadLocal<String> extranonce1 = new ThreadLocal<>();
	
	private static String getCurrentCoin() {
		synchronized (Main.coins) {
			for(CoinWithProfit coin : coins.keySet()) {
				if (minerCoins.get().containsKey(coin.coinAbr)) {
					return coin.coinAbr;
				}
			}
		}
		return null;
	}
	
	private static PrintWriter switchToCoin(Coin newCoin, PrintWriter minerPw) throws ParseException, IOException {
		
		Socket socketRemote = new Socket(newCoin.host, newCoin.port);
		PrintWriter pwRemote = new PrintWriter(socketRemote.getOutputStream());
		BufferedReader inRemote = new BufferedReader(new InputStreamReader(socketRemote.getInputStream()));

		String subscribeMessage = String.format("{\"method\":\"mining.subscribe\",\"id\":1,\"params\":[\"%s\",null,\"%s\",%d]}", extranonce1.get(), newCoin.host, newCoin.port);
		logger.debug("switchpool->pool" + subscribeMessage);
		pwRemote.println(subscribeMessage);
		pwRemote.flush();

		String lineRemote = inRemote.readLine();
		logger.debug("switchpool<-pool" + lineRemote);
//		JSONObject rootJsonRemote = (JSONObject)new JSONParser().parse(lineRemote);
//		String extranonce1 = ((JSONArray)rootJsonRemote.get("result")).get(1).toString();
		
		String authoriseMessageRem = String.format("{\"id\":2,\"method\":\"mining.authorize\",\"params\":[\"%s\",\"%s\"]}", newCoin.login, newCoin.password);
		logger.debug("switchpool->pool" + authoriseMessageRem);
		pwRemote.println(authoriseMessageRem);
		pwRemote.flush();
		
//		lineRemote = inRemote.readLine();
//		logger.debug("switchpool<-pool" + lineRemote);
//		minerPw.println(lineRemote);
//		minerPw.flush();
		
//		String setExtranonceMessage = String.format("{\"method\":\"mining.set_extranonce\",\"id\":3,\"params\":[\"%s\", null]}", extranonce1);
//		logger.debug("miner<-...<-pool" + setExtranonceMessage);
//		minerPw.println(setExtranonceMessage);
//		minerPw.flush();
		
		new Thread(() -> {
			String lineRemote2 = null;
			JSONParser parser = new JSONParser();
			try {
				while ((lineRemote2 = inRemote.readLine()) != null) {
					JSONObject json = (JSONObject)parser.parse(lineRemote2);
					Object id = json.get("id");
					if (id != null && "2".equals(id.toString())) {
						logger.debug("(null)<-...<-pool" + lineRemote2);
						continue;
					}
					logger.debug("miner<-...<-pool" + lineRemote2);
					minerPw.println(lineRemote2);
					minerPw.flush();
				}
			} catch (IOException e) {
				// TODO Do nothing??, PrintWriter must have been closed in
				// other thread
			} catch (ParseException e) {
				logger.error("Error parsing message: " +lineRemote2);
				try { socketRemote.close();} catch (IOException e1) {}
				minerPw.close();
			}
		}).start();
		
		return pwRemote;
	}
	
	private static void processSocketConnection(final Socket clientSocket) throws IOException, ParseException {
		BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		PrintWriter pw = new PrintWriter(clientSocket.getOutputStream());
		StringBuilder sb = new StringBuilder();
		JSONParser parser = new JSONParser();
		String line;
		Coin currentCoin = null;
		long startTime, currentTime, feePeriodStartTime, lastKeepingShareTime;
		startTime = currentTime = feePeriodStartTime = lastKeepingShareTime = System.currentTimeMillis();
		long feeTime = 0;
		
		String loginStr = extPool + ",ZCL=nhRig1:x@127.0.0.1:3034,HUSH=nhRig1:x@127.0.0.1:3033";
		Map<String, Coin> coinMap = Coin.parse(loginStr);
		if (coinMap == null) {
			logger.error(ParseException.ERROR_UNEXPECTED_TOKEN);
			throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN);
		}
		minerCoins.set(coinMap);
		Set<String> mainCoinAbrs = new HashSet<>();
		synchronized (Main.coins) {
			for(CoinWithProfit pair : Main.coins.keySet()) {
				mainCoinAbrs.add(pair.coinAbr);
			}
		}
		minerCoins.get().keySet().retainAll(mainCoinAbrs);
		String message = "Coins supported: " + minerCoins.get().keySet();
		logger.debug(message);
		
		final Coin mainCoin = currentCoin = coinMap.get(extPool.substring(0, 3));		
		if (!mainCoinAbrs.contains(mainCoin.coinAbr)) {
			logger.error(String.format("Main coin %s is not supported by pool", mainCoin.coinAbr));
			return;
		}
		Socket socketRemote = new Socket(currentCoin.host, currentCoin.port);
		PrintWriter mainCoinRemoteSocketPw = new PrintWriter(socketRemote.getOutputStream());
		class Wrapper<T> { // synchronized
			private T t;
			public Wrapper(T t) {
				this.t = t;
			}
			synchronized T get() {
				return t;
			}
			synchronized void set(T t) {
				this.t = t;
			}
			@Override
			public String toString() {
				return t.toString();
			}
		}
		Wrapper<PrintWriter> remoteSocketPw = new Wrapper(mainCoinRemoteSocketPw);
		BufferedReader inRemote = new BufferedReader(new InputStreamReader(socketRemote.getInputStream()));
		Wrapper<String> lastTarget = new Wrapper<>("");
		Wrapper<String> lastNotify = new Wrapper<>("");
		
		try {
		while((line = in.readLine())!=null) {
				long timeInterval = System.currentTimeMillis() - currentTime;
				currentTime = System.currentTimeMillis();				
			
				JSONObject json = (JSONObject)parser.parse(line);
				logger.debug("miner->switchpool" + json);
				String id = json.get("id").toString();
				String method = json.get("method").toString();
				if ("mining.subscribe".equalsIgnoreCase(method)) {					
					if (((JSONArray)json.get("params")).size() == 4) {
						((JSONArray)json.get("params")).set(2, currentCoin.host);
						((JSONArray)json.get("params")).set(3, currentCoin.port);
						line = json.toString();
					}					
					logger.debug("miner->...->pool" + line);
					remoteSocketPw.get().println(line);
					remoteSocketPw.get().flush();
					String resp = inRemote.readLine();
					JSONObject rootJsonRemote = (JSONObject)new JSONParser().parse(resp);
					String extranonce1 = ((JSONArray)rootJsonRemote.get("result")).get(1).toString();
					Main.extranonce1.set(extranonce1);
					logger.debug("miner<-...<-pool" + resp);
					pw.println(resp);
					pw.flush();
					
					new Thread(()->{
						String lineRemote2 = null;
						try {
							while ((lineRemote2 = inRemote.readLine()) != null) {
								JSONObject respJson = (JSONObject)parser.parse(lineRemote2);
								Object method2 = respJson.get("method");
								if (method2 != null && "mining.notify".equalsIgnoreCase(method2.toString())) {
									lastNotify.set(lineRemote2);
								}
								if (method2 != null && "mining.set_target".equalsIgnoreCase(method2.toString())) {
									lastTarget.set(lineRemote2);
								}
								boolean isMainCoin = remoteSocketPw.get() == mainCoinRemoteSocketPw;								
								if (isMainCoin || method2 == null) {
									logger.debug("miner<-...<-pool" + lineRemote2);
									pw.println(lineRemote2);
									pw.flush();
								} else {
									logger.debug("(null)<-...<-pool" + lineRemote2);
									//TODO return confirmations on accepted shares
								}
													
							}
							
						} catch (IOException e) {
							logger.error("Main currency connection error: " + e);
						} catch (ParseException e) {
							logger.error("Error parsing message: " + lineRemote2);
							try { socketRemote.close();} catch (IOException e1) {}
							pw.close();
						}
					}).start();

					new Thread(()->{
						boolean wasMainCoin = true;
						while (true) {
							try { Thread.sleep(50); } catch (InterruptedException e) {}
							boolean isMainCoin = remoteSocketPw.get() == mainCoinRemoteSocketPw;
							if (isMainCoin && !wasMainCoin) {
								logger.info("sending stored 'mining.set_target' and 'mining.notify' messaged to miner");
								logger.debug("miner<-proxypool: " + lastTarget.get());
								pw.println(lastTarget.get());
								pw.flush();
								logger.debug("miner<-proxypool: " + lastNotify.get());
								pw.println(lastNotify.get());
								pw.flush();
							}
							wasMainCoin = isMainCoin;
						}
					}).start();
					
					continue;
				}			
				if("mining.authorize".equalsIgnoreCase(method)) {
					JSONArray params = (JSONArray)json.get("params");
					((JSONArray)json.get("params")).set(0, currentCoin.login);
					((JSONArray)json.get("params")).set(1, currentCoin.password);
					line = json.toString();
					
//					String login = params.get(0).toString();
//					Map<String, Coin> coinMap = Coin.parse(login);
//					if (coinMap == null) {
//						throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN);
//					}
//					minerCoins.set(coinMap);
//					
//					Set<String> mainCoinAbrs = new HashSet<>();
//					synchronized (Main2.coins) {
//						for(CoinWithProfit pair : Main2.coins.keySet()) {
//							mainCoinAbrs.add(pair.coinAbr);
//						}
//					}
//					minerCoins.get().keySet().retainAll(mainCoinAbrs);
//					String message = "Coins supported: " + minerCoins.get().keySet();
//					logger.debug(message);
////					String jsonMessage = String.format("{\"method\":\"client.show_message\",\"params\":[\"%s\", null]}", message);
////					pw.println(jsonMessage);
////					pw.flush();
//					
//					synchronized (Main2.coins) {
//						currentCoin = minerCoins.get().get(getCurrentCoin());
//					}
//					if (currentCoin == null) throw new IOException("no requested coins supported");
//					switchToCoin(currentCoin, pw);
//
//					continue;
				}
				if("mining.submit".equalsIgnoreCase(method)) {
					if (currentCoin == null) throw new IOException("Forcing disconnect due to wrong internal state");					
					((JSONArray)json.get("params")).set(0, currentCoin.login);
					line = json.toString();
				}
				
				logger.debug("miner->...->pool" + line);
				if (remoteSocketPw.get() != null) {
					remoteSocketPw.get().println(line);
					remoteSocketPw.get().flush();
				} else {
					logger.error("Somehow remotePw was not initialized and we're going to loose the message: " + line);
				}
				
				
				if("mining.submit".equalsIgnoreCase(method)) {
					
					if (currentCoin == feeCoin) {
						feeTime += timeInterval;
						if (currentTime - feePeriodStartTime < feePeriod * 1000) {
							continue;
						}
					}
					if (currentCoin == mainCoin) {
						lastKeepingShareTime = currentTime;
					}
					double actualFeePart = (double)feeTime / (currentTime - startTime);
					double requiredFeePart = feePercentage / 100d;
										
//					logger.debug(String.format("(%2.7f)-(%2.7f) ; (%2.7f)-(%2.7f)", 
//							Math.random() * (currentTime - startTime), 0.5 * feePeriod * 1000 / requiredFeePart,
//							requiredFeePart, actualFeePart));
					
					String newCoin;					
					if (requiredFeePart > actualFeePart && Math.random() * (currentTime - startTime) > 0.5 * feePeriod * 1000 / requiredFeePart) {
						newCoin = feeCoin.coinAbr;
					} else {
						newCoin = getCurrentCoin();						
//						newCoin = "ZEN";
//						if (currentCoin.coinAbr.equalsIgnoreCase("ZEN")) {
//							newCoin = "ZCL";
//						}
//						if (currentCoin.coinAbr.equalsIgnoreCase("HUSH")) {
//							newCoin = "ZEN";
//						}
//						if (currentCoin.coinAbr.equalsIgnoreCase("ZCL")) {
//							newCoin = "HUSH";
//						}
						
					}
					if (currentTime - lastKeepingShareTime > 4 * 60 * 1000) {
						newCoin = mainCoin.coinAbr;
					}
					
					
					if (!currentCoin.coinAbr.equalsIgnoreCase(newCoin)) {
						
						if (currentCoin != mainCoin) {
							remoteSocketPw.get().close();
						}
						
						logger.debug(String.format("Switching coins! %s to %s", currentCoin.coinAbr, newCoin));
						if (feeCoin.coinAbr == newCoin) {
							currentCoin = feeCoin;
							feePeriodStartTime = currentTime;
							logger.info("Kicking off fee mining for " + clientSocket.getInetAddress());
						} else {
							synchronized (Main.coins) {
								currentCoin = minerCoins.get().get(newCoin);
							}
						}

						if (currentCoin == mainCoin) {
							remoteSocketPw.set(mainCoinRemoteSocketPw);
						} else {
							remoteSocketPw.set( switchToCoin(currentCoin, pw) );
						}
					}
				}
						
		}
		} finally {
			if (remoteSocketPw != null && remoteSocketPw.get() != null) {
				logger.debug("closing pool connection");
				remoteSocketPw.get().close();
			}
			logger.debug("closing miner connection");
			clientSocket.close();
		}
		
		
	}
	
	private static void handleConnections() {
		ServerSocket serverSocket = null;
		try {
			//serverSocket = (SSLServerSocket)SSLServerSocketFactory.getDefault().createServerSocket(7721);
			serverSocket = new ServerSocket(poolPort);
			Socket clientSocket;
			while((clientSocket = serverSocket.accept()) != null) {
				final Socket s = clientSocket;
				new Thread(()->{ 
					try {
						logger.info(String.format("miner (%s) connected ", s.getInetAddress()));
						processSocketConnection(s);
					} catch (IOException | ParseException e2) {
						logger.info(String.format("miner (%s) connection error (he might have shut it down): %s", s.getInetAddress(),  e2));
					}
				}).start();
			}
			
		} catch (IOException e) {
			logger.fatal("FATAL ERROR: " + e);
		} finally {
			try {
				if (serverSocket != null) serverSocket.close();
			} catch (IOException e) {
				logger.error("Couldn't close server socket: " + e);
			}
		}
	}
	
	
	
	
	private static Map<CoinWithProfit, String> coins = new TreeMap<>((a,b)->{
		return b.profitability.compareTo(a.profitability);
	});
	
	private static URL jsonURL;
	
	private static void updateCoinsListOnce() throws IOException, ParseException {
		JSONParser parser = new JSONParser();
		String jsonText = getStringByURL(jsonURL);
		JSONObject topJson = (JSONObject)parser.parse(jsonText);
		
		Map<String, CoinWithProfit> coinsNew = new HashMap<>();
		
		for (Object coinWrap : topJson.entrySet()) {
			Entry<Object, Object> coinPairVals = (Entry<Object, Object>) coinWrap;
			String coinAbr = (String) coinPairVals.getKey();
			if (!coinsLimit.contains(coinAbr)) {
				continue;
			}
			double profitability = Double.valueOf(((JSONArray)(coinPairVals.getValue())).get(0).toString().trim());
			coinsNew.put(coinAbr, new CoinWithProfit(coinAbr, profitability));
		}
		synchronized (Main.coins) {
			
			for(Entry<CoinWithProfit, String> entry : new HashSet<>(Main.coins.entrySet())) {
				String coinAbr;
				if (coinsNew.containsKey(coinAbr = entry.getValue())) {
					CoinWithProfit oldCoin = entry.getKey();
					CoinWithProfit newCoin = coinsNew.remove(coinAbr);
					Main.coins.remove(oldCoin);
					Main.coins.put(newCoin, coinAbr);
				}
			}
			for(Entry<String, CoinWithProfit> entry : coinsNew.entrySet()) {
				Main.coins.put(entry.getValue(), entry.getKey());
			}
		}
		
	}
	
	private static void updateCoinsList() {
		String jsonURLstr = props.getProperty("minehub.json.url");
		try { 
			jsonURL = new URL(jsonURLstr);
		} catch (MalformedURLException e) {
			logger.error("URL is incorrect : " + e);
			exit();
		}

		new Thread(()->{
			while(true) {
				try {
					try {
						updateCoinsListOnce();
						logger.debug(coins);
					} catch (IOException | ParseException e) {
						logger.error("Error reading coins info from MineHub :" + e);
					}
					try {
						Thread.sleep((long) (updateCoinsTime * 1000));
					} catch (InterruptedException e1) {
						logger.error("coins update waiting period interrupted :" + e1);
					}
				} catch (Exception e) {
					logger.fatal("Coin prices udpate loop crashed, repeating in 1 second : " + e);
					e.printStackTrace();
					try { Thread.sleep((long) (updateCoinsTime * 1000)); } catch (InterruptedException e1) { }
				}
			}
		}).start();
	}
	
	public static void main(String[] args) {
		logger.info("Proxy Pool for NH v0.1!");
		readProperties();
		updateCoinsList();

		handleConnections();

	}

}
