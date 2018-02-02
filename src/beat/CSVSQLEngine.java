/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved.* */
package beat;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import org.relique.jdbc.csv.CsvDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ravindra
 */
public class CSVSQLEngine {

    private final static Logger LOGGER = LoggerFactory.getLogger(CSVSQLEngine.class);

    public Connection getFFConn(String path) throws Exception {
        /*
         * LOGGER.info("Preparing Flat File connection : " + path); Connection
         * conn = null;
         *
         * File f = new File(path); path = f.getParent();
         * System.out.println("CSV ENGINE FF CONN PATH:" + path); // Load the
         * driver. Class.forName("org.relique.jdbc.csv.CsvDriver"); conn =
         * DriverManager.getConnection("jdbc:relique:csv:" + path);
         * LOGGER.info("Flat File connection has been Created : " + conn);
         * return conn;
         */

        LOGGER.info("Preparing Flat File connection : " + path);
        Connection conn = null;

        Properties props = new Properties();
        props.put("fileExtension", ".txt");
        props.put("separator", ",");

        File f = new File(path);
        String filePath = f.getParent();
        System.out.println("CSV ENGINE FF CONN PATH:" + f.getAbsolutePath());
        Class.forName("jstels.jdbc.csv.CsvDriver2");
//        if (path.toLowerCase().contains(".txt")) {
            conn = DriverManager.getConnection("jdbc:jstels:csv:" + f.getAbsolutePath(), props);
//        } else {
//            conn = DriverManager.getConnection("jdbc:jstels:csv:" + f.getAbsolutePath());
//        }

        // Load the driver.
        LOGGER.info("Flat File connection has been Created : " + conn);
        return conn;

    }

    public String getFFCount(Connection conn, String qry) throws SQLException {

        String count = "";
        LOGGER.info("Fetching Flat file count for Query: " + qry);
        System.out.println("CSV ENGINE FF COUNT QRY:" + qry);
        Statement stmt = conn.createStatement();
        ResultSet results = stmt.executeQuery(qry);

        results.next();
        count = results.getString(1);
        boolean append = true;
        CsvDriver.writeToCsv(results, System.out, append);

        // Clean up
        conn.close();

        return count;

    }

    public ObservableList getFFDuplicateCount(Connection conn, String qry) throws SQLException {
        ObservableList<ObservableList<String>> data = FXCollections.observableArrayList();
        System.out.println("CSV ENGINE FF DUP QRY:" + qry);
        LOGGER.info("Fetching Flat file Duplicate Count for Query: " + qry);
        Statement stmt = conn.createStatement();

        // Select the ID and NAME columns from sample.csv
        ResultSet rs = stmt.executeQuery(qry);

        int count = 0;
        String temp = "";
        while (rs.next()) {
            //Iterate Row
            ObservableList<String> row = FXCollections.observableArrayList();
            //row.add(String.valueOf(++j));

            for (int i = 1; i < rs.getMetaData().getColumnCount(); i++) {
                //Iterate Column
                temp = rs.getString(i);
                if (temp == null) {
                    row.add("null");
                } else {
                    row.add(temp);
                }

            }
            //System.out.println("Row [1] added " + row);
            data.add(row);
        }
        boolean append = true;
//        CsvDriver.writeToCsv(results, System.out, append);

        // Clean up
        conn.close();

        return data;

    }

    public List getFFColumns(String path, String qry) throws Exception {

        System.out.println("CSV ENGINE FF COLUMNS");
        LOGGER.info("Fetching Flat file Columns for Query: " + qry);

        Connection conn = getFFConn(path);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(qry);
        String colname;
        List collist = new ArrayList();

        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            colname = rs.getMetaData().getColumnName(i);
//            System.out.println(rs.getMetaData().getColumnTypeName(i));
            collist.add(colname);

        }
//        System.out.println("CSV COLUMN LIST : " + collist);
        LOGGER.info("CSV COLUMN LIST : " + collist);
        return collist;
    }

    public String getFFColumnType(String path, String col, String filename, boolean flag) throws Exception {

        System.out.println("CSV ENGINE FF COLUMNS");
        LOGGER.info("Fetching Flat file Column Type for Query: " + path + "/" + filename);

        Connection conn = getFFConn(path);
        Statement stmt = conn.createStatement();
        String qry;
        if (flag) {
            qry = "select " + col + " from \"" + filename + "\" limit 1";
        } else {
            qry = "select sum(" + col + ") from " + filename + " limit 1";
        }
        System.out.println("Query Gen: " + qry);
        ResultSet rs = stmt.executeQuery(qry);
        String coltype;
        //List collist = new ArrayList();

        //for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
        coltype = rs.getMetaData().getColumnTypeName(1);
        coltype = coltype.equalsIgnoreCase("expression") ? "string" : coltype;
        //  collist.add(coltype);

        //}
//        System.out.println("CSV COLUMN TYPE : " + coltype);
        LOGGER.info("CSV COLUMN TYPE : " + coltype);
        return coltype;
    }

    public ObservableList getFFTableData(String path, String SQL) {//throws Exception {
        ObservableList<Object> data = FXCollections.observableArrayList();
        try {
            System.out.println("CSV ENGINE FF TABLE DATA");
            LOGGER.info("Fetching Data from Flat file : " + path);
            System.out.println("Query Recived: " + SQL);
            Connection conn = getFFConn(path);
            String temp;
            int j = 0;

            Statement stmt = conn.createStatement();

            // Select the ID and NAME columns from sample.csv
            ResultSet rs = stmt.executeQuery(SQL);

            while (rs.next()) {
                //Iterate Row
                ObservableList<String> row = FXCollections.observableArrayList();
                //row.add(String.valueOf(++j));

                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    //Iterate Column
                    temp = rs.getString(i);
                    if (temp == null) {
                        row.add("null");
                    } else {
                        row.add(temp);
                    }

                }
                //System.out.println("Row [1] added " + row);
                data.add(row);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return data;
    }

    /*
     * public static void main(String[] args) throws Exception { CSVSQLEngine
     * cse = new CSVSQLEngine();
     * //cse.getFFColumns("F:/CollegeScorecard_Raw_Data/test.csv","select * from
     * test"); //
     * cse.getFFColumnType("F:/CollegeScorecard_Raw_Data/test.csv","col2","test");
     * cse.getFFTableData("F:/CollegeScorecard_Raw_Data/test.csv", " select
     * count(col1) from test group by col1 having count(col1) > 5"); }
     *
     */
}
