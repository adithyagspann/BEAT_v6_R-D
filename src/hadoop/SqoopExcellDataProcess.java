/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved. * */
package hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.poi.ss.usermodel.*;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Adithya
 *
 */
/*
 * This is designed to read the STM excel Data and process the final ouput to
 * the excel with the exceptions where data is null for mandatory.
 */
public class SqoopExcellDataProcess {

    private static org.slf4j.Logger logger = LoggerFactory.getLogger(SqoopExcellDataProcess.class);
    private Workbook excelFile;
    private Sheet workSheet;
    private SqoopBean sqoopBean;
    private ObservableList<SqoopBean> sqoopList;
    private static StringBuffer exceptionData;
    private int exceptionIndex;
    private Iterator<Row> rowIterator;

    public SqoopExcellDataProcess(String workBook) throws IOException, BiffException {
        logger.info("Creating connection to STM file : " + workBook);
        excelFile = Workbook.getWorkbook(new File(workBook));
        workSheet = excelFile.getSheet(0);
        exceptionData = new StringBuffer();
        sqoopList = FXCollections.observableArrayList();
//        FileInputStream excelFile = new FileInputStream(new File(workBook));
//            Workbook workbook = new XSSFWorkbook(excelFile);
//            Sheet datatypeSheet = workbook.getSheetAt(0);
//             rowIterator = datatypeSheet.iterator();

    }
    public String getAuthor(){
        return workSheet.getCell(1, 1).getContents();
    }
    
    public String getTitle(){
        return workSheet.getCell(1, 0).getContents();
    }
    
    public String getVersion(){
        return workSheet.getCell(1, 2).getContents();
    }

    public ObservableList<SqoopBean> getSqopList() throws JSQLParserException {

        exceptionIndex = 0;
        for (int rowIndex = 5; rowIndex < workSheet.getRows(); rowIndex++) {
//            for (int colIndex = 0; colIndex < workSheet.getColumns(); colIndex++) {
            sqoopBean = new SqoopBean();
            if (!workSheet.getCell(0, rowIndex).getContents().isEmpty()) {
                sqoopBean.setSqopDBType(workSheet.getCell(0, rowIndex).getContents());
            } else {
                exceptionData.append(++exceptionIndex).append(". DB Type Cannot be Empty in row " + rowIndex + "\n");
            }
//            }
            if (!workSheet.getCell(1, rowIndex).getContents().isEmpty()) {
                sqoopBean.setSqopJdbcUrl(workSheet.getCell(1, rowIndex).getContents());
            } else {
                exceptionData.append(++exceptionIndex).append(". JDBC URL Cannot be Empty in row " + rowIndex + "\n");
            }

            if (!workSheet.getCell(2, rowIndex).getContents().isEmpty()) {
                sqoopBean.setSqopDriverclass(workSheet.getCell(2, rowIndex).getContents());
            } else {
                exceptionData.append(++exceptionIndex).append(". JDBC Driver Class Cannot be Empty in row " + rowIndex + "\n");
            }
            if (!workSheet.getCell(3, rowIndex).getContents().isEmpty()) {
                sqoopBean.setSqopUserName(workSheet.getCell(3, rowIndex).getContents());
            } else {
                exceptionData.append(++exceptionIndex).append(". DB User Name Cannot be Empty in row " + rowIndex + "\n");
            }
            if (!workSheet.getCell(4, rowIndex).getContents().isEmpty()) {
                System.out.println("Sqoop not Empty");
                sqoopBean.setSqoopPasswd(workSheet.getCell(4, rowIndex).getContents());
            } else {
                System.out.println("Sqoop Empty");
                exceptionData.append(++exceptionIndex).append(". DB Password Cannot be Empty in row " + rowIndex + "\n");
            }
            if (!workSheet.getCell(5, rowIndex).getContents().isEmpty()) {
                System.out.println("Sqoop Schema");
                sqoopBean.setSqopSrcSchema(workSheet.getCell(5, rowIndex).getContents());
            } else {
                exceptionData.append(++exceptionIndex).append(". Source Schema Cannot be Empty in row " + rowIndex + "\n");
            }

            if (!workSheet.getCell(6, rowIndex).getContents().isEmpty()) {
                sqoopBean.setSqopSrcTbl(workSheet.getCell(6, rowIndex).getContents());
            } else {
                exceptionData.append(++exceptionIndex).append(". Source Table Cannot be Empty in row " + rowIndex + "\n");
            }

            sqoopBean.setSqopSrcWhere(workSheet.getCell(7, rowIndex).getContents());

            if (!workSheet.getCell(8, rowIndex).getContents().isEmpty()) {
                sqoopBean.setSqopTrgSchema(workSheet.getCell(8, rowIndex).getContents());
            } else {
                exceptionData.append(++exceptionIndex).append(". Target Schema Cannot be Empty in row " + rowIndex + "\n");
            }
            if (!workSheet.getCell(9, rowIndex).getContents().isEmpty()) {
                sqoopBean.setSqopTrgTable(workSheet.getCell(9, rowIndex).getContents());
            } else {
                exceptionData.append(++exceptionIndex).append(". Target Table Cannot be Empty in row " + rowIndex + "\n");

            }
            if (!workSheet.getCell(10, rowIndex).getContents().isEmpty()) {
                sqoopBean.setSqopTrgDir(workSheet.getCell(10, rowIndex).getContents());
            } else {
                exceptionData.append(++exceptionIndex).append(". Target Dir Cannot be Empty in row " + rowIndex + "\n");

            }

            sqoopBean.setSqopFieldDelimt(workSheet.getCell(11, rowIndex).getContents());
            sqoopBean.setSqopLineDelimt(workSheet.getCell(12, rowIndex).getContents());
            sqoopBean.setSqopMapper(workSheet.getCell(13, rowIndex).getContents());
            sqoopBean.setSqopSplitBy(workSheet.getCell(14, rowIndex).getContents());
            sqoopBean.setSqopTgtExist(workSheet.getCell(15, rowIndex).getContents());
            sqoopBean.setSqopSeverName(workSheet.getCell(16, rowIndex).getContents());
            sqoopBean.setSqopServerusrnme(workSheet.getCell(17, rowIndex).getContents());
            sqoopBean.setSqopServerpswd(workSheet.getCell(18, rowIndex).getContents());
            sqoopBean.setSqoopServerPath(workSheet.getCell(19, rowIndex).getContents());
//            System.out.println(sqoopBean);
            sqoopList.add(sqoopBean);

        }

        return sqoopList;
    }

    public String getExceptionData() {
//String exception = exceptionData.toString();
        return exceptionData.toString();
    }
}
