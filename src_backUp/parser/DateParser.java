package parser;

import com.joestelmach.natty.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Adithya
 */
public class DateParser {

    public Date date;

    /*
     * Method to get the date from the Text (NLP)
     */
    public Date getDate(String text) {
        String matchingValue = "";
        Parser parser = new Parser();
        List<DateGroup> groups = parser.parse(text);

        for (DateGroup group : groups) {
            List<Date> dates = group.getDates();
            date = dates.get(dates.size() - 1);
        }

        return date;
    }

    public Date getDateType(String incRule) {
        Date lastDate = null;
        if (incRule.toLowerCase().contains("ly")) {
            if (incRule.toLowerCase().contains("dai")) {
                lastDate = getDate("Last 1 Day");
            } else if (incRule.toLowerCase().contains("week")) {
                lastDate = getDate("Last 1 Week");
            } else if (incRule.toLowerCase().contains("bi-week")) {
                lastDate = getDate("Last 2 Week");
            } else if (incRule.toLowerCase().contains("tri-week")) {
                lastDate = getDate("Last 3 Week");
            } else if (incRule.toLowerCase().contains("month")) {
                lastDate = getDate("Last 1 month");
            } else if (incRule.toLowerCase().contains("bi-month")) {
                lastDate = getDate("Last 2 month");
            } else if (incRule.toLowerCase().contains("quarter")) {
                lastDate = getDate("Last 3 month");
            } else if (incRule.toLowerCase().contains("half-year") || incRule.toLowerCase().contains("half year")) {
                lastDate = getDate("Last 6 month");
            } else if (incRule.toLowerCase().contains("year")) {
                lastDate = getDate("Last 12 months");
            }
        } else {
            System.out.println("Data");
            lastDate = getDate("last "+incRule);
        }

        return lastDate;
    }

    /*
     * Method to get the date in required Format
     */
    public String getFormatedDate(String pattern, Date date) {
        System.out.println("Date: "+date);
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        System.out.println("Formated Date: "+dateFormat.format(date));
        return dateFormat.format(date);
    }

//    public static void main(String[] args) {
//        DateParser dateParser = new DateParser();
//        Date d2 = dateParser.getDate("Today 09:00 AM PST");
//        Date d3 = dateParser.getDate("last 14 days");
//
//        System.out.println(dateParser.getFormatedDate("yyyymmdd", dateParser.getDate("Today 09:00 AM PST")) + " : " + dateParser.getFormatedDate("yyyymmdd", dateParser.getDate("Last  year IST")));
//
//    }
}
