package victor.proxy.pool;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class StatisticsServer {

	private final static Logger logger = Logger.getLogger(StatisticsServer.class);
	private final static int colsNum = 50;
	
	public static void runStatistics(final Map<Date, Double> data) throws IOException {
		
		final String htmlTemplate = FileUtils.readFileToString(new File("statistics_page.html"));
		
		HttpServer httpServer = HttpServer.create(new InetSocketAddress(Main.httpPort), 0);
		
		httpServer.createContext("/stat", new HttpHandler() {
			
			public Map<String, String> queryToMap(String query){
			    Map<String, String> result = new HashMap<String, String>();
			    for (String param : query.split("&")) {
			        String pair[] = param.split("=");
			        if (pair.length>1) {
			            result.put(pair[0], pair[1]);
			        }else{
			            result.put(pair[0], "");
			        }
			    }
			    return result;
			}
			
			@Override
			public void handle(HttpExchange t) throws IOException {
				try {
					handleInner(t);
				} catch (RuntimeException e) {
					logger.error(String.format("Statistics request failed: %s %s %s", t.getRemoteAddress().toString(), 
							e, ArrayUtils.toString(e.getStackTrace())));
				}
			}
			
			public void handleInner(HttpExchange t) throws IOException {
				Map<String, String> params = t.getRequestURI().getQuery() != null ?  queryToMap(t.getRequestURI().getQuery()) : new HashMap<>();
				logger.debug(params);
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				Date from = null, to = null;
				String message = null;
				try {
					if (params.containsKey("from") && params.containsKey("to")) {
						from = df.parse(params.get("from").replaceAll("T", " "));
						to = df.parse(params.get("to").replaceAll("T", " "));
						if (to.compareTo(new Date()) > 0) {
							to = new Date();
						}
						if (from.compareTo(to) > 0) {
							from = to;
						}
					} else {
						double period = 24;
						if (params.containsKey("period")) {
							period = Double.parseDouble(params.get("period"));
						}
						to = new Date();
						from = new Date(to.getTime() - (int)(period * 3600 * 1000));											
					}
				} catch (NumberFormatException | ParseException e) {
					logger.error(message = ("Error processing http request parameters: " + e));
				}
				if (message != null) {
					t.sendResponseHeaders(404, message.length());
				} else {
					List<Date> keyList = new ArrayList<>(data.keySet());					
					List<Long> columns = new ArrayList<>();
					List<String> labels = new ArrayList<>();
					double total = 0;
					
					int index = Collections.binarySearch(keyList, from);
					if (index < 0) {
						index = -index-1;
					}
					
					logger.debug("Looking for date " + df.format(from) + " in " + keyList + ". \nGoing to start from index " + index);
					
					long delta = (to.getTime() - from.getTime()) / colsNum;
					
					for(int i = 0; i < colsNum+1; i++) {
						String labelText = i % 5 == 0 ? df.format(new Date(from.getTime() + i*delta)) : ".";
						labels.add("\"" + labelText  + "\"");
						double portion = 0d;						
						while(index < keyList.size() && keyList.get(index).getTime() < from.getTime() + (i+1)*delta) {
							double share = data.get(keyList.get(index++));
							portion += share;
						}
						columns.add((long)portion);
						total += portion;
					}
					
					double averageSpeed = total / ( (to.getTime() - from.getTime()) / 1000d );
					
					message = String.format(htmlTemplate, df.format(from).replaceAll(" ", "T"), df.format(to).replaceAll(" ", "T"),
							total/1000000d, averageSpeed, labels.toString(), columns.toString());
					
					t.sendResponseHeaders(200, message.length());	
				}
				
				OutputStream os = t.getResponseBody();
				try {
					os.write(message.getBytes());
				} finally {
					os.close();
				}
				
			}
		});
		
		httpServer.setExecutor(null);
		httpServer.start();

		
	}
}
