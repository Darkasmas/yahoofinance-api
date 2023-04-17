
package yahoofinance.histquotes;

/**
 *
 * @author Stijn Strickx
 */
public enum Interval {

    FIVEMIN("5m"),
    DAILY("1d"),
    WEEKLY("1wk"),
    MONTHLY("1mo");
    
    private final String tag;
    
    Interval(String tag) {
        this.tag = tag;
    }
    
    public String getTag() {
        return this.tag;
    }
    
}
