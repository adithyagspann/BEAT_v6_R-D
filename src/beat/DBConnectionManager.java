/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved. * */
package beat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ravindra
 */
public class DBConnectionManager {

    private final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DBConnectionManager.class);
    private String JDBCDRIVER = "";
    private String DB_URL = "";
    private String USER = "";
    private String PASS = "";
    private String PATH = "";
    private Connection conn = null;
    private String key = "dangerousboy";

    public DBConnectionManager() {

    }

    public DBConnectionManager(String driver, String url, String uid, String pass, String jarpath) throws ClassNotFoundException, SQLException {
        LOGGER.info("Preparing for DB Connection Manager");
        JDBCDRIVER = driver;
        DB_URL = url;
        USER = uid;
        PASS = pass;
        PATH = jarpath;

        //SETP 1: LOAD THE CLASSPATH
        new ClassLoaderEngine(PATH);

        //STEP 2: Register JDBC driver
        Class.forName(JDBCDRIVER);

        //STEP 3: Open a connection
        //System.out.println("Connecting to database...");
        if (!USER.isEmpty()) {
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
        } else {
            conn = DriverManager.getConnection(DB_URL);
        }
        LOGGER.info("DB Connecetion has estabilished successfully: " + conn.getMetaData().getURL());
    }

    public Connection getDBConFromFile(String conname) throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
        System.out.println("Creating DB Connection");
        LOGGER.info("Creating DB Connection from Connection File: " + conname);
        FileReader fr = null;
        File file = new File("conn/" + conname + ".con");
        fr = new FileReader(file.getAbsoluteFile());
        BufferedReader br = new BufferedReader(fr);
        br.readLine();
        DB_URL = br.readLine();
        USER = br.readLine();
        PASS = br.readLine();
        JDBCDRIVER = br.readLine();
        PATH = br.readLine();

//        System.out.println("DB_URL : " + DB_URL);
//        System.out.println("USER : " + USER);
//        System.out.println("PASS : " + PASS);
//        System.out.println("JDBCDRIVER : " + JDBCDRIVER);
//        System.out.println("PATH : " + PATH);
        new ClassLoaderEngine(PATH);
        Class.forName(JDBCDRIVER);

        if (!USER.isEmpty()) {
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            //conn.getMetaData().getSchemas().next();

            //System.out.println(conn.getMetaData().getSchemas().getString(1));
        } else {
            conn = DriverManager.getConnection(DB_URL);
        }
        return conn;
    }

    public Connection getDBCon() {
        LOGGER.info("DB Connection retrieved: " + conn);
        return conn;
    }

    //To Get databases from the connectionss
    public List getDatabaseNames(Connection conn, String conname) throws SQLException, IOException {
        LOGGER.info("Retreving DB Name for connection: " + conn);
        List dblist = new ArrayList();

        String db_sql_type = new ReadPropertyFile().getDBPropval(conname);

        System.out.println("db_sql_type : " + db_sql_type);

        int index = 1;

        Statement stmt = conn.createStatement();
        ResultSet tabs;
        String qry = db_sql_type;
        System.out.println("Database List Qry : " + qry);
        tabs = stmt.executeQuery(db_sql_type);
        tabs.next();
        do {
            String tablename = tabs.getString(index);    // "TABLES"
            dblist.add(tablename);
//            System.out.println("Database Name : " + tablename);
        } while (tabs.next());
        LOGGER.info("Retrived DB Name from connection: " + conn);
        return dblist;

    }

    //To GET SCHEMA NAMES Woking for all DBs  
    public List getSchemaNames(Connection conn, String conname, String db_name) throws SQLException, IOException {
        LOGGER.info("Retreving Schema Name for connection: " + conn);
        List schlist = new ArrayList();

        ReadPropertyFile rpf = new ReadPropertyFile();
        String schema_sql_type = new ReadPropertyFile().getSchemaPropval(conname);

        System.out.println("schema_sql_type : " + schema_sql_type);

        if (schema_sql_type.equalsIgnoreCase("default")) {

            DatabaseMetaData meta = conn.getMetaData();
            ResultSet schemas = meta.getSchemas();

            if (!schemas.next()) {
                schemas = meta.getCatalogs();
                schemas.next();
            }

            do {
                String tableSchema = schemas.getString(1);    // "TABLE_SCHEM"
                //String tableCatalog = schemas.getString(2); //"TABLE_CATALOG"
                schlist.add(tableSchema);
//                System.out.println("tableSchema : " + tableSchema);
            } while (schemas.next());

        } else if (rpf.getDBType(conname).equalsIgnoreCase("sqlserver") || rpf.getDBType(conname).equalsIgnoreCase("jtds")) {

            int index = 1;

            Statement stmt = conn.createStatement();
            ResultSet tabs;
            String qry = schema_sql_type.replace("###", db_name);
            System.out.println("Database List Qry : " + qry);
            tabs = stmt.executeQuery(qry);
            tabs.next();
            index = 1;

            do {
                String tablename = tabs.getString(index);    // "TABLES"
                schlist.add(tablename);
//                System.out.println("Schema Name : " + tablename);
            } while (tabs.next());

        }
        LOGGER.info("Retrived Schema Name from connection: " + conn);
        return schlist;
    }

    //To GET SCHEMA NAMES Woking for all DBs  
    public List getSchemaNames(Connection conn, String conname) throws SQLException, IOException {
        LOGGER.info("Retreving Schema Name for connection: " + conn);
        List schlist = new ArrayList();

        String schema_sql_type = new ReadPropertyFile().getSchemaPropval(conname);

        System.out.println("schema_sql_type : " + schema_sql_type);

        if (schema_sql_type.equalsIgnoreCase("default")) {
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet schemas = meta.getSchemas();

            if (!schemas.next()) {
                schemas = meta.getCatalogs();
                schemas.next();
            }

            do {
                String tableSchema = schemas.getString(1);    // "TABLE_SCHEM"
                //String tableCatalog = schemas.getString(2); //"TABLE_CATALOG"
                schlist.add(tableSchema);
//                System.out.println("tableSchema : " + tableSchema);
            } while (schemas.next());
        }

        Collections.sort(schlist);
        LOGGER.info("Retrived Schema Name from connection: " + conn);
        return schlist;
    }

//to get table names    
    public List getTableNames(Connection conn, String conname, String SCHEMA_NAME) throws IOException, SQLException {
        LOGGER.info("Retriving Table Name from Schema: " + SCHEMA_NAME);
        List tablist = new ArrayList();
        String tableType[] = {"TABLE", "T"};
        int index = 3;

        System.out.println("conname :" + conname);
        System.out.println("SCHEMA_NAME :" + SCHEMA_NAME);
        ReadPropertyFile rpf = new ReadPropertyFile();
        String table_sql_type = rpf.getTablePropval(conname);

        System.out.println("table_sql_type : " + table_sql_type);

        if (table_sql_type.equalsIgnoreCase("default")) {

            try {
                DatabaseMetaData meta = conn.getMetaData();
                // ResultSet tabs = meta.getTables(null,SCHEMA_NAME,null,tableType);   
                ResultSet tabs = meta.getTables(SCHEMA_NAME, SCHEMA_NAME, "%", tableType);

                if (!tabs.next()) {
                    Statement stmt = conn.createStatement();

                    if (rpf.getDBType(conname).equalsIgnoreCase("bigquery")) {
                        tabs = stmt.executeQuery("select table_id from " + SCHEMA_NAME + ".__TABLES__");
                        tabs.next();
                        index = 1;
                    } else {
                        tabs = stmt.executeQuery("select TABNAME from syscat.tables where tabschema = '" + SCHEMA_NAME + "'");
                        tabs.next();
                        index = 1;
//                    System.out.println("Table : " + tabs);
                    }
                }

                do {
                    String tablename = tabs.getString(index);    // "TABLES"
                    tablist.add(tablename);
//                    System.out.println("table : " + tablename);
                } while (tabs.next());
            } catch (Exception ex) {
                new ExceptionUI(ex);
            } finally {
                try {
                    conn.close();
                } catch (Exception ex) {
                    new ExceptionUI(ex);
                }
            }
        } //if default is not working
        else {

            Statement stmt = conn.createStatement();
            ResultSet tabs;

            String qry;

            if (conname.contains("SQL_SERVER")) {
                qry = table_sql_type.replace("###", SCHEMA_NAME);
            } else {
                qry = table_sql_type + "'" + SCHEMA_NAME + "'";
            }

            System.out.println("Table List Qry : " + qry);
            tabs = stmt.executeQuery(qry);
            tabs.next();
            index = 1;

            do {
                String tablename = tabs.getString(index);    // "TABLES"
                tablist.add(tablename);
//                System.out.println("table : " + tablename);
            } while (tabs.next());

        }

        LOGGER.info("Retrived Table Name from Schema: " + SCHEMA_NAME);
        return tablist;
    }

    //To get view names
    public List getViewNames(Connection conn, String conname, String SCHEMA_NAME) throws IOException, SQLException {
        LOGGER.info("Retriving View Name from Schema: " + SCHEMA_NAME);
        List viewlist = new ArrayList();
        String viewType[] = {"VIEW", "V"};
        int index = 3;

        System.out.println("conname :" + conname);
        System.out.println("SCHEMA_NAME :" + SCHEMA_NAME);

        String view_sql_type = new ReadPropertyFile().getViewPropval(conname);

        System.out.println("view_sql_type : " + view_sql_type);

        if (view_sql_type.equalsIgnoreCase("default")) {

            DatabaseMetaData meta = conn.getMetaData();
            // ResultSet tabs = meta.getTables(null,SCHEMA_NAME,null,tableType);   
            ResultSet views = meta.getTables(SCHEMA_NAME, SCHEMA_NAME, "%", viewType);

            views.next();
            do {
                String tablename = views.getString(index);    // "TABLES"
                viewlist.add(tablename);
//                System.out.println("View : " + tablename);
            } while (views.next());

        } //if default is not working
        else {

            Statement stmt = conn.createStatement();
            ResultSet views;

            String qry;

            if (conname.contains("SQL_SERVER")) {
                qry = view_sql_type.replace("###", SCHEMA_NAME);
            } else {
                qry = view_sql_type + "'" + SCHEMA_NAME + "'";
            }

            System.out.println("View List Qry : " + qry);
            views = stmt.executeQuery(qry);
            views.next();
            index = 1;

            do {
                String viewname = views.getString(index);    // "Views"
                viewlist.add(viewname);
//                System.out.println("view : " + viewname);
            } while (views.next());

        }
        LOGGER.info("Retrived View Name from Schema: " + SCHEMA_NAME);
        return viewlist;
    }

    public List getColNames(Connection conn, String SQL) throws SQLException {

        LOGGER.info("Retreving Column Name: " + SQL);
        List data = new ArrayList();

        ResultSet rs = conn.createStatement().executeQuery(SQL);

        ResultSetMetaData rsMetaData = rs.getMetaData();
        int numberOfColumns = rsMetaData.getColumnCount();
        String mtdat;
        //System.out.println("Column Names :");
        for (int i = 1; i <= numberOfColumns; i++) {
            // get the designated column's SQL type.
            mtdat = rsMetaData.getColumnName(i);
            String[] col = mtdat.split("\\.");
            if (col.length > 1) {
                data.add(col[1]);
            } else {
                data.add(mtdat);
            }

        }
        LOGGER.info("Retreved Column Name: " + SQL);
        return data;

    }

    public List getColType(Connection conn, String SQL) throws SQLException {
        //System.out.println("SQl: " + SQL);
        LOGGER.info("Retreving Column Type: " + SQL);
        List data = new ArrayList();

        ResultSet rs = conn.createStatement().executeQuery(SQL);

        ResultSetMetaData rsMetaData = rs.getMetaData();
        int numberOfColumns = rsMetaData.getColumnCount();
        String mtdat;
        //System.out.println("Column Types :");
        for (int i = 1; i <= numberOfColumns; i++) {
            // get the designated column's SQL type.
            mtdat = rsMetaData.getColumnTypeName(i);
            String[] col = mtdat.split("\\.");
            if (col.length > 1) {
                data.add(col[1]);
            } else {
                data.add(mtdat);
            }
            //System.out.println(i + ") " + mtdat);
        }
        LOGGER.info("Retreved Column Type: " + SQL);
        return data;

    }

    public List getKeyColNames(Connection conn, String SCHEMA_NAME, String table_name) throws SQLException {
        LOGGER.info("Retreving Key Column: " + SCHEMA_NAME + ", " + table_name);
        List data = new ArrayList();

        String dbtype = conn.getClass().getName();
        ResultSet rs;
        String mtdat;

        System.out.println("DBTYPE : " + dbtype);
        try {
            if (dbtype.contains("db2")) {
                System.out.println("DBTYPE : " + dbtype);
                String PKSQL = "SELECT sc.name FROM SYSIBM.SYSCOLUMNS SC WHERE SC.TBNAME = '" + table_name + "' AND sc.identity ='N' AND sc.tbcreator='" + SCHEMA_NAME + "' AND sc.keyseq=1";
                System.out.println("PKSQL : " + PKSQL);
                Statement stmt = conn.createStatement();
                rs = stmt.executeQuery(PKSQL);

                while (rs.next()) {
                    mtdat = rs.getString("name");
                    data.add(mtdat);
//                    System.out.println("getPrimaryKeys(): columnName=" + mtdat);

                }
            } else {
                DatabaseMetaData meta = conn.getMetaData();
                rs = meta.getPrimaryKeys(SCHEMA_NAME, SCHEMA_NAME, table_name);

                while (rs.next()) {
                    mtdat = rs.getString("COLUMN_NAME");
                    data.add(mtdat);
//                    System.out.println("getPrimaryKeys(): columnName=" + mtdat);
                }
            }

        } catch (Exception e) {
            LOGGER.error(e.toString());
            System.out.println(e);
        }
        LOGGER.info("Retreved Key Column: " + SCHEMA_NAME + ", " + table_name);
        return data;
    }

    public ObservableList getDataFromQuery(Connection conn, String qry) throws SQLException {
        System.out.println("Fetching data from DB: " + qry);
        LOGGER.info("Fetching the Data from query: " + qry);
        ObservableList rows = FXCollections.observableArrayList();

        //throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(qry);
        int count = 0;
        while (rs.next()) {
            ObservableList cols = FXCollections.observableArrayList();
            int colcount = rs.getMetaData().getColumnCount();

            for (int i = 0; i < colcount; i++) {
                if (rs.getString(i + 1) != null) {
                    if (rs.getString(i + 1).contains(",")) {

                        cols.add(rs.getString(i + 1).replace(",", "_"));
                    } else {

                        cols.add(rs.getString(i + 1));
                    }
                } else {
//                        cols.add(rs.getString(i + 1));
                    cols.add("");
                }
            }

            rows.add(cols);
//                ++count;
//  System.out.println("Count: "+(count));
        }

//  System.out.println("Count: "+(count));
        LOGGER.info("Fetch the Data from query: " + qry + " with count : " + rows.size());
        return rows;
    }

}
