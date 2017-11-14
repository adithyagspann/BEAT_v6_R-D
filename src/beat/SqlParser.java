/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved. * */
package beat;

import java.io.StringReader;
import java.util.logging.Level;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Adithya
 */

/*Checking the SQL Queries */
public class SqlParser {

    private final static Logger LOGGER = LoggerFactory.getLogger(SqlParser.class);

    private CCJSqlParserManager parserManager;
    private PlainSelect plainSelect;

    public SqlParser() {
        parserManager = new CCJSqlParserManager();
    }

    //Checking the SQLQuery
    public boolean checkSqlQuery(String sql) throws JSQLParserException {

        Statement statement = parserManager.parse(new StringReader(sql));
        LOGGER.info("Verifed the Sql Query");
        return true;

    }

    public String getColumnNamefromQuery(String sql) throws JSQLParserException {
        LOGGER.info("Fetching column name from Sql Query");
        Select select = (Select) parserManager.parse(new StringReader(sql));
        plainSelect = (PlainSelect) select.getSelectBody();

        StringBuilder sb = new StringBuilder(plainSelect.getSelectItems().toString());
        sb.reverse().replace(0, sb.length(), sb.toString().split(" ")[0]).reverse();
        String columnName = sb.toString().replace("]", "").replace("[", "");
        LOGGER.info("Fetched column name from Sql Query: " + columnName);
        return columnName;
    }

    public String getColumnRulefromQuery(String sql) throws JSQLParserException {
        LOGGER.info("Fetching column rule from Sql Query: "+sql);
        Select select = (Select) parserManager.parse(new StringReader(sql));
        plainSelect = (PlainSelect) select.getSelectBody();

        StringBuilder sb = new StringBuilder(plainSelect.getSelectItems().toString());
        String col = sb.toString().replace("[", "").replace("]", "");

        String columnName = col.substring(0, col.toLowerCase().indexOf(" as "));
//        String columnName = sb.toString().replace("]", "").replace("[", "");
        LOGGER.info("Fetched column rule from Sql Query: " + columnName);
        return columnName;
    }

    public String getTableNameFromQuery(String sql) throws JSQLParserException {
        Select select = (Select) parserManager.parse(new StringReader(sql));
        plainSelect = (PlainSelect) select.getSelectBody();

        System.out.println("Table: " + plainSelect.getFromItem());
        String table = plainSelect.getFromItem().toString();
        String tableName = table.substring(table.indexOf(".") + 1, table.length());
        LOGGER.info("Fetched Table name from Sql Query: " + tableName);
        return tableName;
    }

    public static void main(String[] args) {
        try {
            SqlParser parser = new SqlParser();
            String value = parser.getColumnRulefromQuery("select SUBSTRING(order_number,1,instr(order_number,'-')-1) as data from CSVEX_source");
            System.out.println("Value: " + value);
        } catch (JSQLParserException ex) {
            java.util.logging.Logger.getLogger(SqlParser.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
