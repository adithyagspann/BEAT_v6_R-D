/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved. * */
package beat;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.ObservableMap;
import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;
import net.sf.jsqlparser.JSQLParserException;
import org.hibernate.engine.jdbc.internal.BasicFormatterImpl;
import org.hibernate.engine.jdbc.internal.Formatter;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Adithya
 */

/* 
 This is designed to read the STM excel Data and process the final ouput to the excel with the exceptions where data is null for mandatory.
 */
public class STMExcellDataProcess {

    private static org.slf4j.Logger logger = LoggerFactory.getLogger(STMExcellDataProcess.class);
    private Workbook excelFile;
    private Sheet workSheet;
    private STMBean sTMBean;
    private ObservableList<STMBean> sTMList;
    private ObservableMap<String, String> connectionData;
    private ObservableMap<String, String> comSplData;
    private String temp; //To Validate the Inputs from the Excel
    private StringBuffer exceptionData; //To store all the Exception in single String and raise all exception in single shot.
    private SqlParser sqlparser;
    private int i;

    public STMExcellDataProcess(String workBook) throws IOException, BiffException {
        logger.info("Creating connection to STM file : "+workBook);
        excelFile = Workbook.getWorkbook(new File(workBook));
        workSheet = excelFile.getSheet(0);

        sTMList = FXCollections.observableArrayList();
        connectionData = FXCollections.observableHashMap();
        comSplData = FXCollections.observableHashMap();
        exceptionData = new StringBuffer();
        sqlparser = new SqlParser();
    }

    /*Getting the Connection Data*/
    public ObservableMap getConnectionData() {
        logger.info("Retreving Connection Details from STM");
        connectionData.clear();

        //Title Null Execption
        temp = workSheet.getCell(1, 2).getContents().trim();
        if (!temp.isEmpty() && !temp.equals("")) {
            connectionData.put(workSheet.getCell(0, 2).getContents(), temp);
        } else {
            exceptionData.append(++i).append(". Title Cannot be Empty!\n");
        }

        //Author Null Execption
        temp = workSheet.getCell(1, 3).getContents().trim();
        if (!temp.isEmpty() && !temp.equals("")) {
            connectionData.put(workSheet.getCell(0, 3).getContents(), temp);
        } else {
            exceptionData.append(++i).append(". Author Cannot be Empty!\n");
        }

        connectionData.put(workSheet.getCell(0, 4).getContents(), workSheet.getCell(1, 4).getContents());
        connectionData.put(workSheet.getCell(0, 5).getContents(), workSheet.getCell(1, 5).getContents());
        connectionData.put(workSheet.getCell(0, 6).getContents(), workSheet.getCell(1, 6).getContents());

        //Targ Host Null Exception
        temp = workSheet.getCell(1, 7).getContents().trim();
        if (!temp.isEmpty() && !temp.equals("")) {
            connectionData.put(workSheet.getCell(0, 7).getContents(), temp);
        } else {
            exceptionData.append(++i).append(". Target Host Cannot be Empty!\n");
        }

        //Targ DB/File Null Exception
        temp = workSheet.getCell(1, 8).getContents().trim();
        if (!temp.isEmpty() && !temp.equals("")) {
            connectionData.put(workSheet.getCell(0, 8).getContents(), temp);
        } else {
            exceptionData.append(++i).append(". Target DB/File Cannot be Empty!\n");
        }

        //Targ DB/File Location Null Exception
        temp = workSheet.getCell(1, 9).getContents().trim();
        if (!temp.isEmpty() && !temp.equals("")) {
            connectionData.put(workSheet.getCell(0, 9).getContents(), temp);
        } else {
            exceptionData.append(++i).append(". Target DB Table/File name Cannot be Empty!\n");
        }

        connectionData.put(workSheet.getCell(0, 10).getContents(), workSheet.getCell(1, 10).getContents());
        //Targ DB/File Type Null Exception
        temp = workSheet.getCell(1, 11).getContents().trim();
        if (!temp.isEmpty() && !temp.equals("")) {
            connectionData.put(workSheet.getCell(0, 11).getContents(), temp);
        } else {
            exceptionData.append(++i).append(". Target DB/File Type Cannot be Empty!\n");
        }

        connectionData.put(workSheet.getCell(0, 12).getContents(), workSheet.getCell(1, 12).getContents());
        //Source Host Null Exception
        temp = workSheet.getCell(1, 13).getContents().trim();
        if (!temp.isEmpty() && !temp.equals("")) {
            connectionData.put(workSheet.getCell(0, 13).getContents(), temp);
        } else {
            exceptionData.append(++i).append(". Source Host Cannot be Empty!\n");
        }

        //Source DB/File Null Exception
        temp = workSheet.getCell(1, 14).getContents().trim();
        if (!temp.isEmpty() && !temp.equals("")) {
            connectionData.put(workSheet.getCell(0, 14).getContents(), temp);
        } else {
            exceptionData.append(++i).append(". Source DB/File Cannot be Empty!\n");
        }
        connectionData.put(workSheet.getCell(0, 14).getContents(), workSheet.getCell(1, 14).getContents());
        //Source DB/File Location Null Exception
        temp = workSheet.getCell(1, 15).getContents().trim();
        if (!temp.isEmpty() && !temp.equals("")) {
            connectionData.put(workSheet.getCell(0, 15).getContents(), temp);
        } else {
            exceptionData.append(++i).append(". Source DB Table/File name Cannot be Empty!\n");
        }

        connectionData.put(workSheet.getCell(0, 16).getContents(), workSheet.getCell(1, 16).getContents());
        //Source DB/File Type Null Exception
        temp = workSheet.getCell(1, 17).getContents().trim();
        if (!temp.isEmpty() && !temp.equals("")) {
            connectionData.put(workSheet.getCell(0, 17).getContents(), temp);
        } else {
            exceptionData.append(++i).append(". Source DB/File Type Cannot by Empty!\n");
        }
        connectionData.put(workSheet.getCell(0, 17).getContents(), workSheet.getCell(1, 17).getContents());
        connectionData.put(workSheet.getCell(0, 18).getContents(), workSheet.getCell(1, 1).getContents());
logger.info("Retrieved Connection Details from STM");
        return connectionData;
    }

    /*Getting Common Bussiness and Special rule*/
    public ObservableMap getComSpecialRule() {
        //Set Exception if null
        logger.info("Retrieving  Special rules from STM");
        comSplData.put("common_rule", workSheet.getCell(3, 2).getContents());

        if (!workSheet.getCell(7, 2).getContents().isEmpty() && !workSheet.getCell(7, 2).getContents().equals("")) {
            comSplData.put("target_key", workSheet.getCell(7, 2).getContents());
        } else {
            exceptionData.append(++i).append(". Target Key Columns Cannot be Empty!\n");
        }

        comSplData.put("target_rule", workSheet.getCell(9, 2).getContents());
        System.out.println("Target_Rule: " + comSplData.get("target_rule"));
        if (!workSheet.getCell(11, 2).getContents().isEmpty() && !workSheet.getCell(11, 2).getContents().equals("")) {
            comSplData.put("source_key", workSheet.getCell(11, 2).getContents());
        } else {
            exceptionData.append(++i).append(". Source Key Columns Cannot be Empty!\n");
        }
logger.info("Retrieved  Special rules from STM");
        return comSplData;
    }

    /*Getting the STM Transformation Rules*/
    public ObservableList<STMBean> getSTMTranData() throws JSQLParserException {
        logger.info("Processing table metadata and source transformation rule from STM");
        System.out.println("getSTMTranData called");

        sTMList.clear();
        for (int row = 20; row < workSheet.getRows(); row++) {
            sTMBean = new STMBean();
            //Set Exception if null
            if (!workSheet.getCell(0, row).getContents().isEmpty() && !workSheet.getCell(0, row).getContents().equals("")) {
                sTMBean.targetSchema.setValue(workSheet.getCell(0, row).getContents());
            } else {
                exceptionData.append(++i).append(". Target Schema Cannot be Empty! Check the Excel Cell : 1,").append(row + 1).append("\n");
            }

            //Set Exception if null
            if (!workSheet.getCell(1, row).getContents().isEmpty() && !workSheet.getCell(1, row).getContents().equals("")) {
                sTMBean.targetTableName.setValue(workSheet.getCell(1, row).getContents());
            } else {
                exceptionData.append(++i).append(". Target Table Name Cannot be Empty! Check the Excel Cell : 2,").append(row + 1).append("\n");
            }

            if (!workSheet.getCell(2, row).getContents().isEmpty() && !workSheet.getCell(2, row).getContents().equals("")) {
                sTMBean.targetColumnName.setValue(workSheet.getCell(2, row).getContents());
            } else {
                exceptionData.append(++i).append(". Target Column Name Cannot be Empty! Check the Excel Cell : 3,").append(row + 1).append("\n");
            }

            sTMBean.targetColumnSpec.setValue(workSheet.getCell(3, row).getContents());
            sTMBean.targetColumnKey.setValue(workSheet.getCell(5, row).getContents());
            sTMBean.sourceColumnSpec.setValue(workSheet.getCell(10, row).getContents());
            //Set Exception if null
            String tranRuleData = workSheet.getCell(12, row).getContents().trim();
            if (!tranRuleData.isEmpty() && !tranRuleData.trim().equals("")) {
//workSheet.getCell(1, 11)                //Checking the Query
                if (!tranRuleData.equalsIgnoreCase("#INPROGRESS")) {

                    if (tranRuleData.equalsIgnoreCase("#STRAIGHTMAPPING")) {
                        //STRAIGHT MAPPING Data
//                        getSrcTranRuleGen(workSheet.getCell(7, row).getContents().trim(), workSheet.getCell(8, row).getContents().trim(), workSheet.getCell(9, row).getContents().trim(), sTMBean.getTargetColumnName());
                        sTMBean.columnTransRule.setValue(getSrcTranRuleGen(workSheet.getCell(7, row).getContents().trim(), workSheet.getCell(8, row).getContents().trim(), workSheet.getCell(9, row).getContents().trim(), sTMBean.getTargetColumnName()));
                    } else {
                        //Not Straight Mapping Data
                        try {
                            sqlparser.checkSqlQuery(tranRuleData);
                            sTMBean.columnTransRule.setValue(workSheet.getCell(12, row).getContents());
                        } catch (JSQLParserException ex) {
                            Logger.getLogger(STMExcellDataProcess.class.getName()).log(Level.SEVERE, null, ex);
                            exceptionData.append(++i).append(". ").append(ex.getCause()).append("12,").append(row + 1).append("\n");
                        }
                    }

                } else {
                    sTMBean.columnTransRule.setValue(workSheet.getCell(12, row).getContents());
                }

            } else {
                exceptionData.append(++i).append(". Source Transformation Cannot be Empty! Check the Excel Cell : 13,").append(row + 1).append("\n");
            }

            sTMList.add(sTMBean);
        }
logger.info("Processed table metadata and source transformation rule from STM");
        return sTMList;
    }

    public void getExecptionData() throws Exception {
        logger.info("Notifying exception to user");
        if (!exceptionData.toString().isEmpty()) {
            temp = exceptionData.toString();
            exceptionData.delete(0, exceptionData.length());
            if (temp.split("\n").length == 1) {
                throw new Exception(temp.toString());
            } else {
                throw new Exception("Invalid Data / Fields missing in the STM \n" + temp.toString());
            }
        }
    }

    public String getSrcTranRuleGen(String dbName, String tableName, String srcColName, String trgColName) throws JSQLParserException {
//        System.out.println("getSrcTranRuleGen called");
        logger.info("Generating Source transformation rule query");
        String formatted_sql_code = null;
        if (workSheet.getCell(1, 17).getContents().equalsIgnoreCase("db")) {

            String qry = "select " + srcColName + " as " + trgColName + " from " + dbName + "." + tableName;
            Formatter f = new BasicFormatterImpl();
            formatted_sql_code = f.format(qry);
            sqlparser.checkSqlQuery(formatted_sql_code);
        } else {
//            String qry = "select " + srcColName + " as " + trgColName + " from " + dbName + "/" + tableName;
String qry = "select " + srcColName + " as " + trgColName + " from "  + tableName;
            formatted_sql_code = qry;
        }

        return formatted_sql_code.trim();
    }

}
