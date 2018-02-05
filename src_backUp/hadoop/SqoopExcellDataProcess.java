/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved. * */
package hadoop;

import java.io.File;
import java.io.IOException;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;
import net.sf.jsqlparser.JSQLParserException;
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

    public SqoopExcellDataProcess(String workBook) throws IOException, BiffException {
        logger.info("Creating connection to STM file : " + workBook);
        excelFile = Workbook.getWorkbook(new File(workBook));
        workSheet = excelFile.getSheet(0);

        sqoopList = FXCollections.observableArrayList();

    }

    public String getSqopList() throws JSQLParserException {

        return "";
    }

}
