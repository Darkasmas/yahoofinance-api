package yahoofinance.query2v8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.time.Duration;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import yahoofinance.Utils;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.HistoricalQuote;
import yahoofinance.histquotes.Interval;
import yahoofinance.histquotes2.QueryInterval;
import yahoofinance.util.RedirectableRequest;

/**
 * @author Stijn Strickx
 */
public class HistQuotesQuery2V8Request {

    private static final Logger log = LoggerFactory.getLogger(HistQuotesQuery2V8Request.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final String symbol;
    private final Calendar from;
    private final Calendar to;
    private final Interval interval;

    private String range;

    public static final Calendar DEFAULT_FROM = Calendar.getInstance();

    static {
        DEFAULT_FROM.add(Calendar.YEAR, -1);
    }
    public static final Calendar DEFAULT_TO = Calendar.getInstance();
    public static final QueryInterval DEFAULT_INTERVAL = QueryInterval.MONTHLY;

    public HistQuotesQuery2V8Request(String symbol, Calendar from, Calendar to, Interval interval) {
        this.symbol = symbol;
        this.from = this.cleanHistCalendar(from);
        this.to = this.cleanHistCalendar(to);
        this.interval = interval;

        long daysDuration = ChronoUnit.DAYS.between(from.toInstant(), to.toInstant());
        if (daysDuration < 1) {
            this.range = Range.ONE_DAY.value;
        } else if (daysDuration < 5) {
            this.range = Range.FIVE_DAYS.value;
        } else if (daysDuration < 30) {
            this.range = Range.ONE_MONTH.value;
        } else {
            this.range = Range.ONE_YEAR.value;
        }
    }

    /**
     * Put everything smaller than days at 0
     * @param cal calendar to be cleaned
     */
    private Calendar cleanHistCalendar(Calendar cal) {
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.HOUR, 0);
        return cal;
    }

    public List<HistoricalQuote> getResult() throws IOException {
        String json = getJson();
        JsonNode resultNode = objectMapper.readTree(json).get("chart").get("result").get(0);
        JsonNode timestamps = resultNode.get("timestamp");
        JsonNode indicators = resultNode.get("indicators");
        JsonNode quotes = indicators.get("quote").get(0);
        JsonNode closes = quotes.get("close");
        JsonNode volumes = quotes.get("volume");
        JsonNode opens = quotes.get("open");
        JsonNode highs = quotes.get("high");
        JsonNode lows = quotes.get("low");
//        JsonNode adjCloses = indicators.get("adjclose").get(0).get("adjclose");

        List<HistoricalQuote> result = new ArrayList<HistoricalQuote>();
        for (int i = 0; i < timestamps.size(); i++) {
            long timestamp = timestamps.get(i).asLong();
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(timestamp * 1000);
            if (interval.equals(Interval.DAILY)) {
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
            }
//            BigDecimal adjClose = adjCloses.get(i).decimalValue();
            long volume = volumes.get(i).asLong();
            BigDecimal open = opens.get(i).decimalValue();
            BigDecimal high = highs.get(i).decimalValue();
            BigDecimal low = lows.get(i).decimalValue();
            BigDecimal close = closes.get(i).decimalValue();

            HistoricalQuote quote = new HistoricalQuote(
                symbol,
                calendar,
                open,
                low,
                high,
                close,
                null,
                volume);
            result.add(quote);
        }

        // Remove the last day if last day == today
        if (interval.equals(Interval.DAILY)) {
            Calendar today = Calendar.getInstance();
            today.set(Calendar.HOUR_OF_DAY, 0);
            today.set(Calendar.MINUTE, 0);
            today.set(Calendar.SECOND, 0);
            today.set(Calendar.MILLISECOND, 0);
            Calendar lastRecord = result.get(result.size() - 1).getDate();

            if (lastRecord.getTime().equals(today.getTime())) {
                result.remove(result.size() - 1);
            }
        }

        return result;
    }

    public String getJson() throws IOException {

        if(this.from.after(this.to)) {
            log.warn("Unable to retrieve historical quotes. "
                    + "From-date should not be after to-date. From: "
                    + this.from.getTime() + ", to: " + this.to.getTime());
            return "";
        }

        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("range", this.range);
        params.put("interval", this.interval.getTag());
        params.put("events", "div|split");

        String url = YahooFinance.HISTQUOTES_QUERY2V8_BASE_URL + URLEncoder.encode(this.symbol , "UTF-8") + "?" + Utils.getURLParameters(params);

        // Get CSV from Yahoo
        log.info("Sending request: " + url);

        URL request = new URL(url);
        RedirectableRequest redirectableRequest = new RedirectableRequest(request, 5);
        redirectableRequest.setConnectTimeout(YahooFinance.CONNECTION_TIMEOUT);
        redirectableRequest.setReadTimeout(YahooFinance.CONNECTION_TIMEOUT);
        URLConnection connection = redirectableRequest.openConnection();

        InputStreamReader is = new InputStreamReader(connection.getInputStream());
        BufferedReader br = new BufferedReader(is);
        StringBuilder builder = new StringBuilder();
        for (String line = br.readLine(); line != null; line = br.readLine()) {
            if (builder.length() > 0) {
                builder.append("\n");
            }
            builder.append(line);
        }
        return builder.toString();
    }

    private enum Range {

        ONE_DAY("1d"),
        FIVE_DAYS("5d"),
        ONE_MONTH("1mo"),
        THREE_MONTHS("3mo"),
        SIX_MONTHS("6mo"),
        ONE_YEAR("1y");

        private final String value;

        Range(String value) {
            this.value = value;
        }
    }

}
