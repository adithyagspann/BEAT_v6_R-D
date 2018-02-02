/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved. * */
package beat;

import hadoop.SqoopBean;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.binding.IntegerBinding;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.ObservableMap;
import javafx.collections.ObservableSet;
import javafx.concurrent.Task;
import javafx.concurrent.WorkerStateEvent;
import javafx.event.ActionEvent;
import javafx.event.Event;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.RadioButton;
import javafx.scene.control.Separator;
import javafx.scene.control.SingleSelectionModel;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableRow;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.Toggle;
import javafx.scene.control.ToggleGroup;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;
import javafx.scene.text.Text;
import javafx.stage.DirectoryChooser;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.util.Callback;
import javafx.util.Pair;
import jfxtras.scene.control.LocalDateTimePicker;
import jxl.Sheet;
import jxl.Workbook;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.poi.ss.usermodel.FontFamily;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.tools.ant.DirectoryScanner;
import org.hibernate.engine.jdbc.internal.BasicFormatterImpl;
import org.hibernate.engine.jdbc.internal.Formatter;
import org.slf4j.LoggerFactory;
import remoteutility.FTPEngine;

/**
 *
 * @author Ravindra
 */
public class MainStageController implements Initializable {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MainStageController.class);
    private static final String FILEPATH = new File(System.getProperty("user.dir")).getPath() + "/files/schema.xml";
    @FXML
    private TreeView dbtreeview;
    @FXML
    private ContextMenu dbrghtclkmenu;
    @FXML
    private ContextMenu ffrghtclkmenu;
    @FXML
    private VBox mainvbox;
    @FXML
    private TreeView flatfilelist;
    @FXML
    private CheckBox chktcnts, chkncnts, chknncnts, chkdupcnts, chkdstcnts, chksumnum, chkmax, chkmin, chkfst1000, chklst1000, chkcmpltdata, chkincrdata, chkschtest;
    @FXML
    private TabPane tbpanemanautotest, autosemifulltabpane, manualResultTabpane;
    @FXML
    private TabPane tbpaneautoresult, advanResultTabPane, testScenTabPane;
    @FXML
    private TextField tfsrcconname;
    @FXML
    private TextField tftrgconname;

    @FXML
    TextField stm_conTitle_txt_fieldLoad, stm_conAut_txt_fieldLoad, stm_conVer_txt_fieldLoad;
    @FXML
    private Tab tabmantesting, semiautotab, fullautotab, manualResultSummaryTab, tabdataload;
    @FXML
    private Tab tabautotesting, advanceTestScenTab, advanResultTab, basicResultTab, basicTestScenTab, manualsrcdttab, manualtrgdttab;
    @FXML
    private Tab tabrslttotalcnt, tabrsltnullcol, tabrsltnotnullcol, tabrsltdupcol, tabrsltdstcol, tabrsltsumcol, tabrsltmincol, tabrsltmaxcol;
    @FXML
    private ImageView ffloadinggif, dbloadinggif, proggif_image;
    @FXML
    private Label progstatus_label;
    @FXML
    private TextArea commonRuletxtarLoad, sourceKeyColtxtarLoad, targetKeyColtxtarLoad;
    @FXML
    private TableView manualsrctabview, manualtrgtabview, manualunmatchedsrctabview, manualunmatchedtrgtabview;
    @FXML
    private TableView<ManualSummaryBean> manualviewsummarytable;
    @FXML
    private TableView transDataTbl, transDataLoadTbl;
    @FXML
    private TableColumn trgSchemaTblclm, trgTableTblclm, trgColumnTblclm, trgColumnSpecTblclm, trgColumnKeyTblclm, srcColumnSpecTblclm, columnTransfTblclm;

    @FXML
    private TableColumn trgSchemaTblclmload, trgTableTblclmload, trgColumnTblclmload, trgColumnSpecTblclmload, trgColumnKeyTblclmload, srcColumnSpecTblclmload, columnTransfTblclmload;
    @FXML
    private Button autoresultrunbt, resultSavebtn, resultClearbtn, autoLoadbtn, loadResultSavebtn, loadResultClearbtn;
    @FXML
    private TableView dataLoadVdlSmry, totalCounts_tbl_view, totalCounts_null_tbl_view, totalCountsnot_null_tbl_view, countDupli_tbl_view, countDistinct_tbl_view, countNumerics_tbl_view, max_tbl_view, min_tbl_view, sourceData_tbl_view, targetData_tbl_view, unMatchsourceData_tbl_view, unMatchtargetData_tbl_view, dataVdlsmry;
    @FXML
    private TableColumn stm_src_field_tbl_col, stm_src_tran_tbl_col, stm_trg_field_tbl_col, src_count_tbl_col, trg_count_tbl_col, result_count_tbl_col, src_col_tbl_col, src_col_count_tbl_col, trg_col_tbl_col, trg_col_count_tbl_col, result_count_nulls_tbl_col, cnt_null_src_col_tbl_col, cnt_null_src_col_count_tbl_col, cnt_null_trg_col_tbl_col, cnt_null_trg_col_count_tbl_col, cnt_null_result_count_nulls_tbl_col, cnt_dup_src_col_tbl_col, cnt_dup_src_col_count_tbl_col, cnt_dup_trg_col_tbl_col, cnt_dup_trg_col_count_tbl_col, cnt_dup_result_count_nulls_tbl_col, cnt_dis_src_col_tbl_col, cnt_dis_src_col_count_tbl_col, cnt_dis_trg_col_tbl_col, cnt_dis_trg_col_count_tbl_col, cnt_dis_result_count_tbl_col, cnt_num_src_col_tbl_col, cnt_num_src_col_count_tbl_col, cnt_num_trg_col_tbl_col, cnt_num_trg_col_count_tbl_col, cnt_num_result_count_tbl_col, cnt_max_src_col_tbl_col, cnt_max_src_col_count_tbl_col, cnt_max_trg_col_tbl_col, cnt_max_trg_col_count_tbl_col, cnt_max_result_count_tbl_col, cnt_min_src_col_tbl_col, cnt_min_src_col_count_tbl_col, cnt_min_trg_col_tbl_col, cnt_min_trg_col_count_tbl_col, cnt_min_result_count_tbl_col, vdlType, vdlsrcCnt, vdltrgCnt, vdlResult;
    @FXML
    private Label dataValid_status_lbl;
    @FXML
    private TextField stm_conTitle_txt_field, stm_conAut_txt_field, stm_conVer_txt_field;
    @FXML
    private TextArea commonRuletxtar, sourceKeyColtxtar, targetKeyColtxtar, manualjtasrcquery, manualjtatrgquery;
    @FXML
    private TableColumn tabPane_tbl_columns;

    /*
     * Sqoop Tabelview
     */
    @FXML
    private TableView<SqoopBean> sqopTblView;
    @FXML
    private TableColumn sqopJdbcUrl,
            sqopDriverclass,
            sqopUserName,
            sqoopPasswd,
            sqopSrcSchema,
            sqopSrcTbl,
            sqopSrcWhere,
            sqopTrgSchema,
            sqopTrgTable,
            sqopTrgDir,
            sqopFieldDelimt,
            sqopLineDelimt,
            sqopMapper,
            sqopSplitBy;
    @FXML
    private ObservableList<SqoopBean> sqoopList;

    @FXML
    private TableColumn<ManualSummaryBean, String> viewsummarytbltcid, viewsummarytblrslt;

    //stmcondata
    private ObservableMap<String, String> stmConData;

    //source & file files
    private String srcfile;
    private String trgfile;
    //Variables
    private LoadConnectionsTreeView lctv;
    private LoadFlatFilesTreeView lfftv;
    private TreeItem<String> connections, databases, tables;
    private TreeItem<String> nodeselect, nodeselect1;
    private List tblkeys, srccoltypes, trgcoltypes;
    private int tabindex;
    private String dbtype;

    //testplan
    Map<String, String> total_cnt_testplan;
    Map<String, String> null_cnt_testplan;
    Map<String, String> notnull_cnt_testplan;
    Map<String, String> dup_cnt_testplan;
    Map<String, String> dst_cnt_testplan;
    Map<String, String> sum_num_testplan;
    Map<String, String> max_col_testplan;
    Map<String, String> min_col_testplan;
    Map<String, String> cmpl_data_testplan;

    //Test Plan Data
    ObservableList<TotalCountBean> total_cnt_testplan_data;
    ObservableList<CountsMaxMinBean> null_cnt_testplan_data;
    ObservableList<CountsMaxMinBean> notnull_cnt_testplan_data;
    ObservableList<CountsMaxMinBean> dup_cnt_testplan_data;
    ObservableList<CountsMaxMinBean> dst_cnt_testplan_data;
    ObservableList<CountsMaxMinBean> sum_num_testplan_data;
    ObservableList<CountsMaxMinBean> max_col_testplan_data;
    ObservableList<CountsMaxMinBean> min_col_testplan_data;
    ObservableList<CountsMaxMinBean> frst_1000_testplan_data;
    ObservableList<CountsMaxMinBean> last_1000_testplan_data;
    ObservableList<CountsMaxMinBean> cmpl_data_tesplan_data;
    ObservableList src_data_Load;
    ObservableList unMatched;

    //Data Validation plan queries
    String datasrcqry;
    String datatrgqry;

    //manual cols list
    List srcColList;
    List trgColList;

    /*
     * Code Created By Adithya 14-05-2017
     */
    private File stmFile;
    private FileChooser fileChooser;
    private ObservableSet<CheckBox> selectedCheckBoxes = FXCollections.observableSet();
    private ObservableSet<CheckBox> unselectedCheckBoxes = FXCollections.observableSet();

    private IntegerBinding numCheckBoxesSelected = Bindings.size(selectedCheckBoxes);
    private ObservableList<STMBean> stmTransData;

    private String[] trgColData;

    // Object for process the STM and writing the final data to the Excel
    private STMExcellDataProcess stmData;

    private TotalCountBean bean;
    private ObservableMap<String, String> stmComSplRule;
    private ObservableList stmSrcTranData;
    private StringBuffer exceptionValue; //To store all the Exception in single String and raise all exception in single shot.
    private String srcCnt;
    private String trgCnt;
    private ObservableList<ObservableList> srcCmplResult;
    private ObservableList trgCmplResult;

    private List src_table, trg_table;
    private CSVSQLEngine csvengine;
    private final int maxNumSelected = 1;
    private Map<String, TableView> resultTableList;
    private Map<String, ObservableList> resultTableListData;

    //for auto testcase store
    Map<String, String> srctcidmap;
    Map<String, String> trgtcidmap;

    @FXML
    private ComboBox testResultSelect;
    //for auto test row buffer    
    int testcaserowsize = 0;
    @FXML
    private Button manualClear, manualRun, manualSave;

//    Test Case Load Variable
    private Dialog<Pair<String, String>> dialog;
    private Dialog<Pair<String, String>> incDiaLog;
    private ButtonType loginButtonType, incOkBut;
    private Button test;
    private Button localfilebt;
    private TextField hosturl;

    private ComboBox filetypecmb;
    private TextField jarpath;

    private Label msglabel;
    private Label dataissuelabel;

    @FXML
    private Label title;

    private CheckBox whereClausesrcStatus;
    private TextArea whereClausesrc;

//file chooser
    File file;
    private TextArea whereClausetrg, groupBysrc, groupBytrg, havingSrc, havingTrg, orderBySrc, orderByTrg;

    private CheckBox whereClausetrgStatus;
    private List fileWhereClause;
    @FXML
    private CheckBox srcExec, trgExec;
    private boolean manualTestCaseExec;
    private List<String> incrementalCondition;
    @FXML
    private TableColumn vdlLoadType, vdlLoadsrcCnt, vdlLoadtrgCnt, vdlLoadResult;

    @FXML
    private void dbAddButtonAction(ActionEvent event) {
        logger.info("Clicked on Add DB Button");
        try {
            //CALLING ADD DB CONNECTION UI
            System.out.print("Clicked ADD DB Button");
            new AddDBConnectionUI(lctv, "");
        } catch (IOException ex) {
            logger.error(ex.toString());
            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    @FXML
    private void ffAddButtonAction(ActionEvent event) {

        System.out.print("Clicked ADD FILE Button");
        logger.info("Clicked on  Add File Button");
        //CALLING ADD FILE CONNECTION UI
        new FlatFileConnectionUI(lfftv, mainvbox, "");
        new LoadFlatFilesTreeView(flatfilelist);

    }

    @Override
    public void initialize(URL url, ResourceBundle rb) {
        logger.info("UI Loaded");
        //Setting Results button disable
        autoresultrunbt.setDisable(true);
        resultSavebtn.setDisable(true);
        resultClearbtn.setDisable(true);
        autoLoadbtn.setDisable(true);
        loadResultSavebtn.setDisable(true);
        loadResultClearbtn.setDisable(true);
        manualClear.setDisable(true);
        manualRun.setDisable(true);
        manualSave.setDisable(true);

        resultTableList = FXCollections.observableHashMap();
        resultTableListData = FXCollections.observableHashMap();
        testScenTabPane.getSelectionModel().selectedItemProperty().addListener(
                new ChangeListener<Tab>() {
            @Override
            public void changed(ObservableValue<? extends Tab> ov, Tab t, Tab t1) {

                if (testScenTabPane.getSelectionModel().getSelectedItem().getText().equalsIgnoreCase("Data Validation Test Scenarios")) {
                    advanResultTabPane.getSelectionModel().select(advanResultTab);
                } else {
                    advanResultTabPane.getSelectionModel().select(basicResultTab);
                }
            }
        }
        );
        //setting loading images to db / ff
        dbloadinggif.setImage(new Image(getClass().getResourceAsStream("/icon/dbtabicon.png")));
        ffloadinggif.setImage(new Image(getClass().getResourceAsStream("/icon/filesicon.png")));

        //loading db connection into treeview
        lctv = new LoadConnectionsTreeView(this.dbtreeview);
        dbtreeview.getSelectionModel().selectedItemProperty()
                .addListener((v, oldValue, newValue) -> {
                    dbRightClickMenuControl((TreeItem<String>) newValue);
                    dbtree_SelectionChanged((TreeItem<String>) newValue);
                });

        //loading flat files
        lfftv = new LoadFlatFilesTreeView(this.flatfilelist);
        flatfilelist.getSelectionModel().selectedItemProperty()
                .addListener((v, oldValue, newValue) -> {
                    fftree_SelectionChanged((TreeItem<String>) newValue);
                });

        //check box listeners
        chktcnts.selectedProperty().addListener(new ChangeListener<Boolean>() {
            public void changed(ObservableValue<? extends Boolean> ov,
                    Boolean old_val, Boolean new_val) {

                if (chktcnts.isSelected()) {
                    addTab(tbpaneautoresult, tabrslttotalcnt, tabindex);

                } else {
                    tabindex = tbpaneautoresult.getTabs().indexOf(tabrslttotalcnt);
                    closeTab(tabrslttotalcnt);
                }

            }
        });

        chkncnts.selectedProperty().addListener(new ChangeListener<Boolean>() {
            public void changed(ObservableValue<? extends Boolean> ov,
                    Boolean old_val, Boolean new_val) {

                if (chkncnts.isSelected()) {
                    addTab(tbpaneautoresult, tabrsltnullcol, tabindex);

                } else {
                    tabindex = tbpaneautoresult.getTabs().indexOf(tabrsltnullcol);
                    closeTab(tabrsltnullcol);
                }

            }
        });

        chknncnts.selectedProperty().addListener(new ChangeListener<Boolean>() {
            public void changed(ObservableValue<? extends Boolean> ov,
                    Boolean old_val, Boolean new_val) {

                if (chknncnts.isSelected()) {
                    addTab(tbpaneautoresult, tabrsltnotnullcol, tabindex);

                } else {
                    tabindex = tbpaneautoresult.getTabs().indexOf(tabrsltnotnullcol);
                    closeTab(tabrsltnotnullcol);
                }

            }
        });

        chkdupcnts.selectedProperty().addListener(new ChangeListener<Boolean>() {
            public void changed(ObservableValue<? extends Boolean> ov,
                    Boolean old_val, Boolean new_val) {

                if (chkdupcnts.isSelected()) {
                    addTab(tbpaneautoresult, tabrsltdupcol, tabindex);

                } else {
                    tabindex = tbpaneautoresult.getTabs().indexOf(tabrsltdupcol);
                    closeTab(tabrsltdupcol);
                }

            }
        });

        chkdstcnts.selectedProperty().addListener(new ChangeListener<Boolean>() {
            public void changed(ObservableValue<? extends Boolean> ov,
                    Boolean old_val, Boolean new_val) {

                if (chkdstcnts.isSelected()) {
                    addTab(tbpaneautoresult, tabrsltdstcol, tabindex);

                } else {
                    tabindex = tbpaneautoresult.getTabs().indexOf(tabrsltdstcol);
                    closeTab(tabrsltdstcol);
                }

            }
        });

        chksumnum.selectedProperty().addListener(new ChangeListener<Boolean>() {
            public void changed(ObservableValue<? extends Boolean> ov,
                    Boolean old_val, Boolean new_val) {

                if (chksumnum.isSelected()) {
                    addTab(tbpaneautoresult, tabrsltsumcol, tabindex);

                } else {
                    tabindex = tbpaneautoresult.getTabs().indexOf(tabrsltsumcol);
                    closeTab(tabrsltsumcol);
                }

            }
        });

        chkmax.selectedProperty().addListener(new ChangeListener<Boolean>() {
            public void changed(ObservableValue<? extends Boolean> ov,
                    Boolean old_val, Boolean new_val) {

                if (chkmax.isSelected()) {
                    addTab(tbpaneautoresult, tabrsltmaxcol, tabindex);

                } else {
                    tabindex = tbpaneautoresult.getTabs().indexOf(tabrsltmaxcol);
                    closeTab(tabrsltmaxcol);
                }

            }
        });

        chkmin.selectedProperty().addListener(new ChangeListener<Boolean>() {
            public void changed(ObservableValue<? extends Boolean> ov,
                    Boolean old_val, Boolean new_val) {

                if (chkmin.isSelected()) {
                    addTab(tbpaneautoresult, tabrsltmincol, tabindex);

                } else {
                    tabindex = tbpaneautoresult.getTabs().indexOf(tabrsltmincol);
                    closeTab(tabrsltmincol);
                }

            }
        });

        chkfst1000.selectedProperty().addListener(new ChangeListener<Boolean>() {
            public void changed(ObservableValue<? extends Boolean> ov,
                    Boolean old_val, Boolean new_val) {

            }
        });

        chklst1000.selectedProperty().addListener(new ChangeListener<Boolean>() {
            public void changed(ObservableValue<? extends Boolean> ov,
                    Boolean old_val, Boolean new_val) {

            }
        });

        chkcmpltdata.selectedProperty().addListener(new ChangeListener<Boolean>() {
            public void changed(ObservableValue<? extends Boolean> ov,
                    Boolean old_val, Boolean new_val) {

            }
        });

        chkincrdata.selectedProperty().addListener(new ChangeListener<Boolean>() {
            public void changed(ObservableValue<? extends Boolean> ov,
                    Boolean old_val, Boolean new_val) {
                if (chkincrdata.isSelected()) {
                    System.out.println("incre Data Selected");
                    if (incrementalCondition.isEmpty()) {
                        try {
                            List incRule = getDynamicIncRule();
                            System.out.println("Data: " + incRule);
                            if (incRule.isEmpty()) {

                                chkincrdata.setSelected(false);

                            }
                        } catch (Exception ex) {
                            new ExceptionUI(ex);
                        }
                    }
                } else {

                }
            }
        });

        chkschtest.selectedProperty().addListener(new ChangeListener<Boolean>() {
            public void changed(ObservableValue<? extends Boolean> ov,
                    Boolean old_val, Boolean new_val) {

            }
        });

        configureCheckBox(chkfst1000);
        configureCheckBox(chklst1000);
        configureCheckBox(chkcmpltdata);
        configureCheckBox(chkincrdata);
        configureCheckBox(chkschtest);
//        chkfst1000.setDisable(true);
//        chklst1000.setDisable(true);
//        chkschtest.setDisable(true);

//        submitButton.setDisable(true);
        setTestScenarios(true);
        numCheckBoxesSelected.addListener((obs, oldSelectedCount, newSelectedCount) -> {
            if (newSelectedCount.intValue() >= maxNumSelected) {
                unselectedCheckBoxes.forEach(cb -> cb.setDisable(true));

            } else {
                unselectedCheckBoxes.forEach(cb -> cb.setDisable(false));

            }
        });

        /*
         * The below statements are used STM table
         */
        viewsummarytbltcid.setCellValueFactory(cellData -> cellData.getValue().getTestCaseID());
        viewsummarytblrslt.setCellValueFactory(cellData -> cellData.getValue().getResult());

        //summary testresult table click event handler
        manualviewsummarytable.setOnMousePressed((MouseEvent event) -> {
            if (event.isPrimaryButtonDown() && event.getClickCount() == 2) {
                ManualSummaryBean msb = manualviewsummarytable.getSelectionModel().getSelectedItem();
                System.out.println("clicked table :" + msb);
                try {
                    //place ur code
                    manualTestCaseExec = true;
                    processSinglTestcase(msb.getTestCaseID().getValue());
                } catch (Exception ex) {
                    Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
                }

            }
        });

        //Obect Creation for the File Chooser
        fileChooser = new FileChooser();

        setTableColumnsBean();
        exceptionValue = new StringBuffer();

        testResultSelect.getItems().addAll("Failed", "Both", "Passed");
        testResultSelect.setDisable(true);

        testResultSelect.getSelectionModel().selectedItemProperty().addListener(new ChangeListener() {
            @Override
            public void changed(ObservableValue observable, Object oldValue, Object newValue) {
                Map<String, TableView> tableView = getSelectedResultTableList(getSelectedTestCases());
                Map<String, ObservableList> tableData = getSelectedResultTableData(getSelectedTestCases());
                ObservableList listData = null;
                for (Map.Entry<String, ObservableList> tableD : tableData.entrySet()) {
                    String tableV = tableD.getKey();

                    if (tableV.equalsIgnoreCase("total_cnts")) {
                        ObservableList<TotalCountBean> data = tableD.getValue();
                        if (newValue.toString().equalsIgnoreCase("passed")) {
                            tableView.get(tableV).setItems(FXCollections.observableArrayList(data.stream().filter(t -> t.getTotCnt().equalsIgnoreCase("passed")).collect(Collectors.toList())));

                        } else if (newValue.toString().equalsIgnoreCase("failed")) {
                            tableView.get(tableV).setItems(FXCollections.observableArrayList(data.stream().filter(t -> t.getTotCnt().equalsIgnoreCase("failed")).collect(Collectors.toList())));

                        } else {

                            tableView.get(tableV).setItems(data);

                        }

                    } else {
                        ObservableList<CountsMaxMinBean> data = tableD.getValue();
                        if (newValue.toString().equalsIgnoreCase("passed")) {
                            tableView.get(tableV).setItems(FXCollections.observableArrayList(data.stream().filter(t -> t.getResult().equalsIgnoreCase("passed")).collect(Collectors.toList())));

                        } else if (newValue.toString().equalsIgnoreCase("failed")) {
                            tableView.get(tableV).setItems(FXCollections.observableArrayList(data.stream().filter(t -> t.getResult().equalsIgnoreCase("failed")).collect(Collectors.toList())));

                        } else {

                            tableView.get(tableV).setItems(data);

                        }

                    }

                }

            }
        });

        manualjtasrcquery.textProperty().addListener((observable, oldValue, newValue) -> {
            if (manualjtasrcquery.getText().isEmpty() && !srcExec.isSelected()) {
                manualRun.setDisable(true);
            } else if (!manualjtasrcquery.getText().isEmpty() && srcExec.isSelected()) {
                manualRun.setDisable(false);
            }
        });

        manualjtatrgquery.textProperty().addListener((observable, oldValue, newValue) -> {
            if (manualjtatrgquery.getText().isEmpty() && !trgExec.isSelected()) {
                manualRun.setDisable(true);
            } else if (!manualjtatrgquery.getText().isEmpty() && trgExec.isSelected()) {
                manualRun.setDisable(false);
            }
        });

    }

    /*
     * Dynamic Inrc Rule
     */
    public List<String> getDynamicIncRule() throws Exception {
        System.out.println("Inc Check");
        Date fromDate = null;
        Date toDate = null;
        List<String> inrCol = new ArrayList<>();
        List<String> inrRuleData = new ArrayList<>();
        for (int i = 0; i < stmTransData.size(); i++) {
            if (stmTransData.get(i).getTargetColumnSpec().equalsIgnoreCase("date") || stmTransData.get(i).getTargetColumnSpec().equalsIgnoreCase("timestamp") || stmTransData.get(i).getTargetColumnSpec().equalsIgnoreCase("datetime")) {
//                if(stmTransData.get(i).getIncrementalCheck())
                inrCol.add(stmTransData.get(i).getTargetColumnName());
                inrCol.add(stmTransData.get(i).getSourceColumnName());
            }
        }
        if (inrCol.size() > 0) {
            showInrRule(inrCol, inrRuleData);
//            System.out.println(inrRuleData.get(1) + " : " + inrRuleData.get(2));

        } else {
            System.out.println("Failed Processing UI");
            throw new Exception("No Incremental Column is specified in STM");
        }
        return inrRuleData;
    }
//Set the Column name with the Bean variable

    public void setTableColumnsBean() {
        System.out.println("setTableColumns called");
        logger.info("Setting STM Table for UI");
        trgSchemaTblclm.setCellValueFactory(new PropertyValueFactory("targetSchema"));
        trgTableTblclm.setCellValueFactory(new PropertyValueFactory("targetTableName"));
        trgColumnTblclm.setCellValueFactory(new PropertyValueFactory("targetColumnName"));
        trgColumnSpecTblclm.setCellValueFactory(new PropertyValueFactory("targetColumnSpec"));
        trgColumnKeyTblclm.setCellValueFactory(new PropertyValueFactory("targetColumnKey"));
        srcColumnSpecTblclm.setCellValueFactory(new PropertyValueFactory("sourceColumnSpec"));
        columnTransfTblclm.setCellValueFactory(new PropertyValueFactory("columnTransRule"));

        trgSchemaTblclmload.setCellValueFactory(new PropertyValueFactory("targetSchema"));
        trgTableTblclmload.setCellValueFactory(new PropertyValueFactory("targetTableName"));
        trgColumnTblclmload.setCellValueFactory(new PropertyValueFactory("targetColumnName"));
        trgColumnSpecTblclmload.setCellValueFactory(new PropertyValueFactory("targetColumnSpec"));
        trgColumnKeyTblclmload.setCellValueFactory(new PropertyValueFactory("targetColumnKey"));
        srcColumnSpecTblclmload.setCellValueFactory(new PropertyValueFactory("sourceColumnSpec"));
        columnTransfTblclmload.setCellValueFactory(new PropertyValueFactory("columnTransRule"));

        logger.info("Setting Total Counts Table for UI");
        /*
         * The below statements are used STM table
         */
        src_count_tbl_col.setCellValueFactory(new PropertyValueFactory<TotalCountBean, String>("srcCnt"));
        trg_count_tbl_col.setCellValueFactory(new PropertyValueFactory<TotalCountBean, String>("trgCnt"));
        result_count_tbl_col.setCellValueFactory(new PropertyValueFactory<TotalCountBean, String>("totCnt"));

        total_cnt_testplan_data = FXCollections.observableArrayList();

        //Null Count Coloumns
        logger.info("Setting Null Counts Table for UI");
        src_col_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("srcCol"));
        src_col_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("srcColCount"));
        trg_col_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("trgCol"));
        trg_col_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("trgColCount"));
        result_count_nulls_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("result"));
        null_cnt_testplan_data = FXCollections.observableArrayList();

        //NOt Null Count Coloumns
        logger.info("Setting Not Null Counts Table for UI");
        cnt_null_src_col_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("srcCol"));
        cnt_null_src_col_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("srcColCount"));
        cnt_null_trg_col_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("trgCol"));
        cnt_null_trg_col_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("trgColCount"));
        cnt_null_result_count_nulls_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("result"));
        notnull_cnt_testplan_data = FXCollections.observableArrayList();

        //Count Distinct
        logger.info("Setting Duplicate Counts Table for UI");
        cnt_dup_src_col_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("srcCol"));
        cnt_dup_src_col_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("srcColCount"));
        cnt_dup_trg_col_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("trgCol"));
        cnt_dup_trg_col_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("trgColCount"));
        cnt_dup_result_count_nulls_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("result"));
        dup_cnt_testplan_data = FXCollections.observableArrayList();

        //Distinct 
        logger.info("Setting Distinct Counts Table for UI");
        cnt_dis_src_col_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("srcCol"));
        cnt_dis_src_col_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("srcColCount"));
        cnt_dis_trg_col_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("trgCol"));
        cnt_dis_trg_col_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("trgColCount"));
        cnt_dis_result_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("result"));
        dst_cnt_testplan_data = FXCollections.observableArrayList();
        logger.info("Setting Sum on Columns in Table for UI");
        cnt_num_src_col_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("srcCol"));
        cnt_num_src_col_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("srcColCount"));
        cnt_num_trg_col_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("trgCol"));
        cnt_num_trg_col_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("trgColCount"));
        cnt_num_result_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("result"));
        sum_num_testplan_data = FXCollections.observableArrayList();
        logger.info("Setting Max on Columns in Table for UI");
        cnt_max_src_col_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("srcCol"));
        cnt_max_src_col_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("srcColCount"));
        cnt_max_trg_col_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("trgCol"));
        cnt_max_trg_col_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("trgColCount"));
        cnt_max_result_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("result"));
        max_col_testplan_data = FXCollections.observableArrayList();
        logger.info("Setting Min on Columns in Table for UI");
        cnt_min_src_col_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("srcCol"));
        cnt_min_src_col_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("srcColCount"));
        cnt_min_trg_col_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("trgCol"));
        cnt_min_trg_col_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("trgColCount"));
        cnt_min_result_count_tbl_col.setCellValueFactory(new PropertyValueFactory<CountsMaxMinBean, String>("result"));
        min_col_testplan_data = FXCollections.observableArrayList();

        vdlType.setCellValueFactory(new PropertyValueFactory<DataVdlSummry, String>("valditionType"));
        vdlsrcCnt.setCellValueFactory(new PropertyValueFactory<DataVdlSummry, String>("srcCnt"));
        vdltrgCnt.setCellValueFactory(new PropertyValueFactory<DataVdlSummry, String>("trgCnt"));
        vdlResult.setCellValueFactory(new PropertyValueFactory<DataVdlSummry, String>("result"));

        vdlLoadType.setCellValueFactory(new PropertyValueFactory<DataVdlSummry, String>("valditionType"));
        vdlLoadsrcCnt.setCellValueFactory(new PropertyValueFactory<DataVdlSummry, String>("srcCnt"));
        vdlLoadtrgCnt.setCellValueFactory(new PropertyValueFactory<DataVdlSummry, String>("trgCnt"));
        vdlLoadResult.setCellValueFactory(new PropertyValueFactory<DataVdlSummry, String>("result"));

        src_data_Load = FXCollections.observableArrayList();

        sqopJdbcUrl.setCellValueFactory(new PropertyValueFactory<SqoopBean, String>("sqopJdbcUrl"));
        sqopDriverclass.setCellValueFactory(new PropertyValueFactory<SqoopBean, String>("sqopDriverclass"));
        sqopUserName.setCellValueFactory(new PropertyValueFactory<SqoopBean, String>("sqopUserName"));
        sqoopPasswd.setCellValueFactory(new PropertyValueFactory<SqoopBean, String>("sqoopPasswd"));
        sqopSrcSchema.setCellValueFactory(new PropertyValueFactory<SqoopBean, String>("sqopSrcSchema"));
        sqopSrcTbl.setCellValueFactory(new PropertyValueFactory<SqoopBean, String>("sqopSrcTbl"));
        sqopSrcWhere.setCellValueFactory(new PropertyValueFactory<SqoopBean, String>("sqopSrcWhere"));
        sqopTrgSchema.setCellValueFactory(new PropertyValueFactory<SqoopBean, String>("sqopTrgSchema"));
        sqopTrgTable.setCellValueFactory(new PropertyValueFactory<SqoopBean, String>("sqopTrgTable"));
        sqopTrgDir.setCellValueFactory(new PropertyValueFactory<SqoopBean, String>("sqopTrgDir"));
        sqopFieldDelimt.setCellValueFactory(new PropertyValueFactory<SqoopBean, String>("sqopFieldDelimt"));
        sqopLineDelimt.setCellValueFactory(new PropertyValueFactory<SqoopBean, String>("sqopLineDelimt"));
        sqopMapper.setCellValueFactory(new PropertyValueFactory<SqoopBean, String>("sqopMapper"));
        sqopSplitBy.setCellValueFactory(new PropertyValueFactory<SqoopBean, String>("sqopSplitBy"));
        sqoopList = FXCollections.observableArrayList();
    }

    public void dbRightClickMenuControl(TreeItem<String> nodeselect) {
        logger.info("DB Connection Context Menu is been Used");
        String nodename = nodeselect.getValue();
        System.out.println("Manual: " + tabmantesting.isSelected());
        System.out.println("Auto: " + tabautotesting.isSelected());
//        if (tabmantesting.isSelected()) {
//            try {
//
//                if (nodename.equalsIgnoreCase("connections")) {
//                    dbrghtclkmenu.getItems().get(0).setDisable(false);
//                    dbrghtclkmenu.getItems().get(1).setDisable(true);
//                    dbrghtclkmenu.getItems().get(2).setDisable(true);
//                    dbrghtclkmenu.getItems().get(3).setDisable(true);
//                    dbrghtclkmenu.getItems().get(4).setDisable(true);
//
//                } else if (nodeselect.getParent().getValue().equalsIgnoreCase("connections")) {
//                    dbrghtclkmenu.getItems().get(0).setDisable(false);
//                    dbrghtclkmenu.getItems().get(1).setDisable(false);
//                    dbrghtclkmenu.getItems().get(2).setDisable(false);
//                    dbrghtclkmenu.getItems().get(3).setDisable(false);
//                    dbrghtclkmenu.getItems().get(4).setDisable(false);
//                }
//            } catch (Exception e) {
//                logger.error("Execption Raised at DB right Click : " + e.toString());
//            }
//
//        } else {
        try {

            if (nodename.equalsIgnoreCase("connections")) {

                dbrghtclkmenu.getItems().get(0).setDisable(false);
                dbrghtclkmenu.getItems().get(1).setDisable(true);
                dbrghtclkmenu.getItems().get(2).setDisable(true);
                dbrghtclkmenu.getItems().get(3).setDisable(true);
                dbrghtclkmenu.getItems().get(4).setDisable(true);
            }

            if (nodename.contains("(#") && nodename.contains("#)")) {

                dbrghtclkmenu.getItems().get(0).setDisable(false);
                dbrghtclkmenu.getItems().get(1).setDisable(true);
                dbrghtclkmenu.getItems().get(2).setDisable(true);
                dbrghtclkmenu.getItems().get(3).setDisable(false);
                dbrghtclkmenu.getItems().get(4).setDisable(false);
            }

            if (nodename.equalsIgnoreCase("Databases")) {

                dbrghtclkmenu.getItems().get(0).setDisable(false);
                dbrghtclkmenu.getItems().get(1).setDisable(true);
                dbrghtclkmenu.getItems().get(2).setDisable(true);
                dbrghtclkmenu.getItems().get(3).setDisable(true);
                dbrghtclkmenu.getItems().get(4).setDisable(true);

            }

            if (nodeselect.getParent().getValue().equalsIgnoreCase("Databases")) {
                dbrghtclkmenu.getItems().get(0).setDisable(false);
                dbrghtclkmenu.getItems().get(1).setDisable(true);
                dbrghtclkmenu.getItems().get(2).setDisable(true);
                dbrghtclkmenu.getItems().get(3).setDisable(true);
                dbrghtclkmenu.getItems().get(4).setDisable(true);

            }

            if (nodeselect.getParent().getParent().getValue().equalsIgnoreCase("Databases")) {
                dbrghtclkmenu.getItems().get(0).setDisable(false);
                dbrghtclkmenu.getItems().get(1).setDisable(true);
                dbrghtclkmenu.getItems().get(2).setDisable(true);
                dbrghtclkmenu.getItems().get(3).setDisable(true);
                dbrghtclkmenu.getItems().get(4).setDisable(true);

            }

            if (nodename.equalsIgnoreCase("Tables") || nodename.equalsIgnoreCase("Views")) {
                dbrghtclkmenu.getItems().get(0).setDisable(false);
                dbrghtclkmenu.getItems().get(1).setDisable(true);
                dbrghtclkmenu.getItems().get(2).setDisable(true);
                dbrghtclkmenu.getItems().get(3).setDisable(true);
                dbrghtclkmenu.getItems().get(4).setDisable(true);
            }

            if (nodeselect.getParent().getValue().equalsIgnoreCase("Tables") || nodeselect.getParent().getValue().equalsIgnoreCase("Views")) {
                dbrghtclkmenu.getItems().get(0).setDisable(true);
                dbrghtclkmenu.getItems().get(1).setDisable(false);
                dbrghtclkmenu.getItems().get(2).setDisable(false);
                dbrghtclkmenu.getItems().get(3).setDisable(true);
                dbrghtclkmenu.getItems().get(4).setDisable(true);

            }

        } catch (Exception e) {
            logger.error("Execption Raised at DB right Click : " + e.toString());
        }
//        }

    }

    static boolean flag = false;

    private void dbtree_SelectionChanged(TreeItem<String> nodeselect) {

        logger.info("DB Connection Tree Selection Changed");
        this.nodeselect = nodeselect;

        String nodename = nodeselect.getValue();
        String parent;
        if (!nodename.equalsIgnoreCase("connections")) {
            parent = nodeselect.getParent().getValue();
            Connection conn = null;
            DBConnectionManager ct = null;
            List schlist;
            List tablist;
            List viewlist;
            List dblist;
            System.out.println("Name :" + nodename);
            System.out.println("Parent :" + parent);

            //For schema list or db list 
            if (nodename.equalsIgnoreCase("databases")) {

                dbtype = parent.split("#")[1];

                //get database based on dbtype
                if (nodeselect.getChildren().isEmpty()) {
                    if (dbtype.equalsIgnoreCase("SQL_SERVER")) {
                        try {
                            ct = new DBConnectionManager();
                            conn = ct.getDBConFromFile(parent);
                            dblist = ct.getDatabaseNames(conn, parent);
                            lctv.loadDBTreeView(dblist, nodeselect);
                            schlist = ct.getSchemaNames(conn, parent);
                            lctv.loadSchemaTreeView(schlist, nodeselect);
                        } catch (IOException | SQLException ex) {
                            logger.error(ex.toString());
                            new ExceptionUI(ex);
                            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
                        } finally {
                            try {
                                logger.info("DB Connection Closed");
                                conn.close();
                            } catch (SQLException ex) {
                                logger.error(ex.toString());
                                Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }

                    } else {
                        try {
                            ct = new DBConnectionManager();
                            conn = ct.getDBConFromFile(parent);
                            schlist = ct.getSchemaNames(conn, parent);
                            lctv.loadSchemaTreeView(schlist, nodeselect);
                        } catch (IOException | SQLException ex) {
                            logger.error(ex.toString());
                            new ExceptionUI(ex);
                            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
                        } finally {
                            try {
                                logger.info("DB Connection Closed");
                                conn.close();
                            } catch (SQLException ex) {
                                logger.error(ex.toString());
                                Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                    }
                }
            }

            //schema or db select
            if (nodeselect.getParent().getValue().equalsIgnoreCase("databases")) {

                parent = nodeselect.getParent().getParent().getValue();

                dbtype = parent.split("#")[1];

                if (dbtype.equalsIgnoreCase("SQL_SERVER")) {

                    try {
                        String dbname = nodeselect.getValue();
                        ct = new DBConnectionManager();
                        conn = ct.getDBConFromFile(parent);
                        schlist = ct.getSchemaNames(conn, parent, dbname);
                        lctv.loadSchemaTreeView(schlist, nodeselect);
                    } catch (IOException | SQLException ex) {
                        logger.error(ex.toString());
                        new ExceptionUI(ex);
                        Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
                    } finally {
                        try {
                            logger.info("DB Connection Closed");
                            conn.close();
                        } catch (SQLException ex) {
                            logger.error(ex.toString());
                            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }

                }

            }

            String parent1 = "";
            String dbname1 = "";
            try {
                if (nodeselect.getParent().getParent().getParent().getParent().getValue().equalsIgnoreCase("databases")) {

                    parent1 = nodeselect.getParent().getParent().getParent().getParent().getParent().getValue();
                    dbname1 = nodeselect.getParent().getParent().getValue();

                } else if (nodeselect.getParent().getParent().getParent().getValue().equalsIgnoreCase("databases")) {

                    parent1 = nodeselect.getParent().getParent().getParent().getParent().getValue();
                    dbname1 = nodeselect.getParent().getParent().getValue();

                } else {
                    parent1 = nodeselect.getParent().getParent().getParent().getValue();
                    dbname1 = nodeselect.getParent().getValue();

                }

                //set dbtype
                dbtype = parent1.split("#")[1];
                System.out.println("dbtype : " + dbtype);

            } catch (Exception e) {
                logger.error(e.toString());
            }

            //table list   
            if (nodeselect.getValue().equalsIgnoreCase("tables")) {
                if (nodeselect.getChildren().isEmpty()) {
                    try {
                        ct = new DBConnectionManager();
                        conn = ct.getDBConFromFile(parent1);
                        tablist = ct.getTableNames(conn, parent1, dbname1);

                        lctv.loadTableTreeView(tablist, nodeselect);
                    } catch (IOException | SQLException ex) {
                        logger.error(ex.toString());
                        new ExceptionUI(ex);
                        Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
                    } finally {
                        try {
                            logger.info("DB Connection Closed");
                            conn.close();
                        } catch (SQLException ex) {
                            logger.error(ex.toString());
                            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                }
            }

            //view list
            if (nodeselect.getValue().equalsIgnoreCase("views")) {
                if (nodeselect.getChildren().isEmpty()) {
                    try {
                        ct = new DBConnectionManager();
                        conn = ct.getDBConFromFile(nodeselect.getParent().getParent().getParent().getValue());
                        viewlist = ct.getViewNames(conn, nodeselect.getParent().getParent().getParent().getValue(), nodeselect.getParent().getValue());

                        lctv.loadTableTreeView(viewlist, nodeselect);
                    } catch (IOException | SQLException ex) {
                        logger.error(ex.toString());
                        new ExceptionUI(ex);
                        Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
                    } finally {
                        try {
                            logger.info("DB Connection Closed");
                            conn.close();
                        } catch (SQLException ex) {
                            logger.error(ex.toString());
                            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                }
            }
        }
    }

    private void fftree_SelectionChanged(TreeItem<String> nodeselect1) {
        logger.info("Selecting flat file");
        this.nodeselect1 = nodeselect1;

        ffrghtclkmenu.getItems().get(0).setDisable(true);
        ffrghtclkmenu.getItems().get(1).setDisable(true);
        ffrghtclkmenu.getItems().get(2).setDisable(true);
        ffrghtclkmenu.getItems().get(3).setDisable(true);
        ffrghtclkmenu.getItems().get(4).setDisable(true);

        String nodename = "";
        if (nodeselect1.getValue() == null) {
            nodename = "Flat Files";
        } else {
            nodename = nodeselect1.getValue();
        }
        System.out.println("Selected Node :" + nodename);
        if (!nodename.equalsIgnoreCase("Flat Files") && !nodename.equalsIgnoreCase("TEXT") && !nodename.equalsIgnoreCase("CSV") && !nodename.equalsIgnoreCase("XML") && !nodename.equalsIgnoreCase("EXCEL") && !nodename.equalsIgnoreCase("JSON")) {

            ffrghtclkmenu.getItems().get(0).setDisable(true);
            ffrghtclkmenu.getItems().get(1).setDisable(false);
            ffrghtclkmenu.getItems().get(2).setDisable(false);
            ffrghtclkmenu.getItems().get(3).setDisable(false);
            ffrghtclkmenu.getItems().get(4).setDisable(false);

        } else if (nodename.equalsIgnoreCase("Flat Files") || nodename.equalsIgnoreCase("TEXT") || nodename.equalsIgnoreCase("CSV") || nodename.equalsIgnoreCase("XML") || nodename.equalsIgnoreCase("EXCEL") || nodename.equalsIgnoreCase("JSON")) {

            ffrghtclkmenu.getItems().get(0).setDisable(false);

        }
        logger.info("flat file selected ");
    }

    @FXML
    private void dbaddSrcButtonAction(ActionEvent event) {

        System.out.println("Clicked - DB Add Src Button");
        logger.info("Adding DB to Source");
        String tablename = "";
        String dbname = "";
        String connname = "";

        if (tabmantesting.isSelected()) {
//
//            connname = nodeselect.getValue();
//            tfsrcconname.setText(connname);

            if (dbtype.equalsIgnoreCase("SQL_SERVER")) {
                tablename = nodeselect.getValue();
                dbname = nodeselect.getParent().getParent().getParent().getValue() + "." + nodeselect.getParent().getParent().getValue();
                connname = nodeselect.getParent().getParent().getParent().getParent().getParent().getValue();
            } else {
                tablename = nodeselect.getValue();
                dbname = nodeselect.getParent().getParent().getValue();
                connname = nodeselect.getParent().getParent().getParent().getParent().getValue();
            }

            tfsrcconname.setText(connname + "::" + dbname + "::" + tablename);
        } else {
            if (dbtype.equalsIgnoreCase("SQL_SERVER")) {
                tablename = nodeselect.getValue();
                dbname = nodeselect.getParent().getParent().getParent().getValue() + "." + nodeselect.getParent().getParent().getValue();
                connname = nodeselect.getParent().getParent().getParent().getParent().getParent().getValue();
            } else {
                tablename = nodeselect.getValue();
                dbname = nodeselect.getParent().getParent().getValue();
                connname = nodeselect.getParent().getParent().getParent().getParent().getValue();
            }

            tfsrcconname.setText(connname + "::" + dbname + "::" + tablename);
        }
        logger.info("DB has be added to Source: " + tfsrcconname);
    }

    @FXML
    private void dbaddTrgButtonAction(ActionEvent event) {

        System.out.println("Clicked - DB Add Trg Button");
        logger.info("Adding DB to Target");
        String tablename = "";
        String dbname = "";
        String connname = "";

        if (tabmantesting.isSelected()) {

            if (dbtype.equalsIgnoreCase("SQL_SERVER")) {
                tablename = nodeselect.getValue();
                dbname = nodeselect.getParent().getParent().getParent().getValue() + "." + nodeselect.getParent().getParent().getValue();
                connname = nodeselect.getParent().getParent().getParent().getParent().getParent().getValue();
            } else {
                tablename = nodeselect.getValue();
                dbname = nodeselect.getParent().getParent().getValue();
                connname = nodeselect.getParent().getParent().getParent().getParent().getValue();
            }

            tftrgconname.setText(connname + "::" + dbname + "::" + tablename);

        } else {
            if (dbtype.equalsIgnoreCase("SQL_SERVER")) {
                tablename = nodeselect.getValue();
                dbname = nodeselect.getParent().getParent().getParent().getValue() + "." + nodeselect.getParent().getParent().getValue();
                connname = nodeselect.getParent().getParent().getParent().getParent().getParent().getValue();
            } else {
                tablename = nodeselect.getValue();
                dbname = nodeselect.getParent().getParent().getValue();
                connname = nodeselect.getParent().getParent().getParent().getParent().getValue();
            }

            tftrgconname.setText(connname + "::" + dbname + "::" + tablename);
        }
        logger.info("DB has be added to Target: " + tftrgconname);
    }

    @FXML
    private void dbConnEditButtonAction(ActionEvent event) {
        try {
            System.out.println("Clicked - DB Conn Edit Button");
            logger.info("Editing the DB Connection");
            new AddDBConnectionUI(lctv, nodeselect.getValue());
            logger.info("DB Connection Completed");
            new LoadConnectionsTreeView(this.dbtreeview);
        } catch (IOException ex) {
            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @FXML
    private void dbConnDeltButtonAction(ActionEvent event) throws IOException {
        System.out.println("Clicked - DB Conn Delete Button");
        logger.info("Deleting the DB Connection");
        Files.deleteIfExists(Paths.get("conn/" + nodeselect.getValue() + ".con"));
        new LoadConnectionsTreeView(this.dbtreeview);
        logger.info("DB Connection Deleted");
    }

    @FXML
    private void dbConnRefreshButtonAction(ActionEvent event) {
        System.out.println("Clicked - DB Conn Refresh Button");
        logger.info("Refresh the DB Connection");
        new LoadConnectionsTreeView(this.dbtreeview);
        logger.info("DB Connection Refreshed");
    }

    @FXML
    private void ffaddSrcButtonAction(ActionEvent event) {

        System.out.println("Clicked - FF Add Src Button");
        logger.info("Adding Flat File to Source");
        try {

            String filename = nodeselect1.getValue();
            String filetype = nodeselect1.getParent().getValue();

            if (filetype.equalsIgnoreCase("text")) {
                filetype = "txt";
            }

            String connName = getSetFileName(filename, filetype, tfsrcconname);
            logger.info("Added Flat File to Source: " + connName);
        } catch (Exception ex) {
            logger.error(ex.toString());
            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
            new ExceptionUI(ex);

        }

    }

    @FXML
    private void ffaddTrgButtonAction(ActionEvent event) {
        System.out.println("Clicked - FF DB Add Trg Button");
        logger.info("Adding Flat File to Target");
        try {

            String filename = nodeselect1.getValue();
            String filetype = nodeselect1.getParent().getValue();
            if (filetype.equalsIgnoreCase("text")) {
                filetype = "txt";
            }
            String connName = getSetFileName(filename, filetype, tftrgconname);
            logger.info("Added Flat File to Target: " + connName);
        } catch (Exception ex) {
            logger.error(ex.toString());
            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
            new ExceptionUI(ex);
        }

    }

    @FXML
    private void ffConnEditButtonAction(ActionEvent event) {
        System.out.println("Clicked - FF Conn Edit Button");
        new FlatFileConnectionUI(lfftv, mainvbox, nodeselect1.getValue());
    }

    @FXML
    private void ffConnDeltButtonAction(ActionEvent event) throws IOException {
        System.out.println("Clicked - FF Conn Delete Button: " + nodeselect1.getParent().getValue().toLowerCase());
        Files.deleteIfExists(Paths.get("files/" + nodeselect1.getValue().toLowerCase().substring(nodeselect1.getValue().toLowerCase().lastIndexOf(".") + 1, nodeselect1.getValue().toLowerCase().length()) + "/" + nodeselect1.getValue()));
        new LoadFlatFilesTreeView(this.flatfilelist);
    }

    @FXML
    private void ffConnRefreshButtonAction(ActionEvent event) {
        System.out.println("Clicked - FF Conn Refresh Button");
        new LoadFlatFilesTreeView(this.flatfilelist);
    }

    @FXML
    private void automatedDataTabAction() {
        title.setText("Automated Loading");
        System.out.println("Clicked - Data Loading Tab Action");
        logger.info("Data Loading Tab Selected ");
        chktcnts.setDisable(true);
        chkncnts.setDisable(true);
        chknncnts.setDisable(true);
        chkdupcnts.setDisable(true);
        chkdstcnts.setDisable(true);
        chksumnum.setDisable(true);
        chkmax.setDisable(true);
        chkmin.setDisable(true);
        chkfst1000.setDisable(true);
        chklst1000.setDisable(true);
        chkcmpltdata.setDisable(true);
        chkincrdata.setDisable(true);
        chkschtest.setDisable(true);
    }

    @FXML
    private void manualTestTabAction() {
        title.setText("Manual Testing");
        System.out.println("Clicked - Manual Test Tab Action");
        logger.info("Manual Testing Tab Selected and Verifying Test scenarios ");
        chktcnts.setDisable(true);
        chkncnts.setDisable(true);
        chknncnts.setDisable(true);
        chkdupcnts.setDisable(true);
        chkdstcnts.setDisable(true);
        chksumnum.setDisable(true);
        chkmax.setDisable(true);
        chkmin.setDisable(true);
        chkfst1000.setDisable(true);
        chklst1000.setDisable(true);
        chkcmpltdata.setDisable(true);
        chkincrdata.setDisable(true);
        chkschtest.setDisable(true);
    }

    @FXML
    private void automatedTestTabAction() {
        title.setText("Automated Testing");
        System.out.println("Clicked - Automated Test Tab Action");
        logger.info("Automated Testing Tab Selected and Verifying Test scenarios ");
        chktcnts.setDisable(true);
        chkncnts.setDisable(true);
        chknncnts.setDisable(true);
        chkdupcnts.setDisable(true);
        chkdstcnts.setDisable(true);
        chksumnum.setDisable(true);
        chkmax.setDisable(true);
        chkmin.setDisable(true);
        chkfst1000.setDisable(true);
        chklst1000.setDisable(true);
        chkcmpltdata.setDisable(true);
        chkincrdata.setDisable(true);
        chkschtest.setDisable(true);
    }

    /*
     * Updating FF Connection Name to simply the connection used by STM and
     * Manual Testing
     */
    public String getSetFileName(String filename, String filetype, TextField connname) {
        logger.info("Setting Flat File Connection Name : " + filename);
        FileReader fr = null;
        try {
            File file = new File("files/" + filetype + "/" + filename);
            fr = new FileReader(file.getAbsoluteFile());
            BufferedReader br = new BufferedReader(fr);
            String from = br.readLine();

//            connname.setText("FlatFile::" + from + "::" + actualname);
//            File filePath = null;
            if (from.equalsIgnoreCase("local")) {
                String actualname = br.readLine();
//                filePath = new File(actualname);
                connname.setText("FlatFile::" + from + "::" + actualname.replace("\\", "/"));
                logger.info("Flat File Connection set : " + connname.getText());

            } else {
                String connectType = br.readLine();
                String hostName = br.readLine();
                String actualname = br.readLine();
//                filePath = new File(actualname);
                connname.setText("FlatFile::" + connectType + "@" + hostName + "::" + actualname.replace("\\", "/"));
                logger.info("Flat File Connection set : " + connname.getText());
            }
        } catch (FileNotFoundException ex) {
            logger.error(ex.toString());
            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
            new ExceptionUI(ex);
        } catch (IOException ex) {
            logger.error(ex.toString());
            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
            new ExceptionUI(ex);
        } finally {
            try {
                logger.info("File Reading Connection Closed");
                fr.close();
            } catch (IOException ex) {
                logger.error(ex.toString());
                Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
                new ExceptionUI(ex);
            }
        }

        return null;
    }

    private void closeTab(Tab tab) {
        logger.info("Closing Test Tab: " + tab.getText());
        EventHandler<Event> handler = tab.getOnClosed();
        if (null != handler) {
            handler.handle(null);
        } else {
            tab.getTabPane().getTabs().remove(tab);
        }
    }

    private void addTab(TabPane tabpane, Tab tab, int index) {
        EventHandler<Event> handler = tab.getOnClosed();
        if (null != handler) {
            handler.handle(null);
        } else {
            tabpane.getTabs().add(index, tab);
            tabpane.getSelectionModel().select(tab);
        }
    }

    private String srcRTmpPath, trgRTmpPath;

    //Download Remote file to process the ETL Testing Operations
    private String getRemoteFile(TextField connType, String connName) throws IOException, ParseException {
        logger.info("Processing to download the Remote File " + connName);
        String finalPath = "";
        String tmpPath = "tmp";
        String metaDataPath = tmpPath + "/rFile.meta";
        File fileTmp = new File(tmpPath);
        boolean tmpPathCheck = false;
        if (!fileTmp.isDirectory() && !fileTmp.exists()) {
            logger.info("Creating the TMP Direcorty and Mera file");
            fileTmp.mkdir();
            Files.createFile(Paths.get(metaDataPath));
        }

        if (connType.getText().toLowerCase().contains("@")) {

            List<String> fileConn = readFF(connName);
            File dir = new File(fileConn.get(3));

            String trgPath = tmpPath + "/" + dir.getName();

            List<String> rFileStats;
            if (checkFileDirExist("file", tmpPath, dir.getName())) {
                readWriteRfileMetaData(metaDataPath, "read", dir.getName(), "");
                rFileStats = getRemoteFiletoLocal(fileConn.get(2), fileConn.get(1).equalsIgnoreCase("ftp") ? 21 : 22, fileConn.get(4), fileConn.get(5), fileConn.get(3), trgPath, new SimpleDateFormat("yyyyMMddHHmmss").parse(readWriteRfileMetaData(metaDataPath, "read", dir.getName(), "")));
                if (rFileStats.size() > 1) {
                    readWriteRfileMetaData(metaDataPath, "write", dir.getName(), rFileStats.get(1));
                    System.out.println("Path Sent: " + trgPath);
                    logger.info("Remote Path set: " + trgPath);
                    return trgPath;
                } else {
                    logger.info("Remote Path set: " + trgPath);
                    return trgPath;
                }
            } else {
//                Files.createDirectory(Paths.get(trgPath));
                rFileStats = getRemoteFiletoLocal(fileConn.get(2), fileConn.get(1).equalsIgnoreCase("ftp") ? 21 : 22, fileConn.get(4), fileConn.get(5), fileConn.get(3), trgPath, new SimpleDateFormat("YYYY-MM-DD").parse("1870-10-01"));
                readWriteRfileMetaData(metaDataPath, "write", dir.getName(), rFileStats.get(1));
                System.out.println("TrgPath: " + trgPath);
                finalPath = trgPath;
            }

        }
        logger.info("Remote Path set: " + finalPath);
        return finalPath;
    }

    //Method body to Read / Write properties to file
    private String readWriteRfileMetaData(String filePath, String processType, String key, String value) throws FileNotFoundException, IOException {

        Properties metaProperties = new Properties();
        if (processType.equalsIgnoreCase("read")) {
            logger.info("Reading Meta Data for Key: " + key);
            metaProperties.load(new FileInputStream(filePath));
            return metaProperties.get(key).toString();
        } else {
            metaProperties.load(new FileInputStream(filePath));
            logger.info("Writing Meta Data for Key: " + key);
            metaProperties.setProperty(key, value);
//            metaProperties.store(new FileOutputStream(filePath), srcCnt);
            metaProperties.save(new FileOutputStream(filePath), new Date().toString());
        }
        return null;
    }

    //
    //Download Remote file
    public List<String> getRemoteFiletoLocal(String hostName, int port, String userName, String password, String srcPath, String trgPath, Date oldTime) throws IOException, ParseException {

        List<String> downloadStats = new ArrayList<>();
        FTPEngine fTPEngine = new FTPEngine(hostName, port, userName, password);

        String newTime = fTPEngine.getRemoteModTimeStamp(srcPath);
        if (new SimpleDateFormat("yyyyMMddHHmmss").parse(newTime).after(oldTime)) {
            logger.info("Downloading the latest file from FTP ");
            boolean downloadCheck = fTPEngine.getFile(srcPath, trgPath);
            if (downloadCheck == true) {
                downloadStats.add("true");
                downloadStats.add(newTime);
            }
        } else {
            logger.info("No latest file found on FTP server using old file");
            downloadStats.add("false");
        }

        return downloadStats;
    }

    //Method Body for Checking file existence
    //searchType --> file / dir
    public boolean checkFileDirExist(String searchType, String filePath, String fileName) throws IOException {
        System.out.println("checkFileDirExist Called");

        DirectoryScanner scanner = new DirectoryScanner();
        scanner.setIncludes(new String[]{fileName});
        scanner.setBasedir(filePath);
        scanner.setCaseSensitive(false);
        scanner.scan();
        System.out.println("Dirs: " + filePath + "__.___");
        System.out.println("Dirs: " + fileName + "__.___");
        String dirs[] = null, files[] = null;
        if (searchType.equalsIgnoreCase("dir")) {
            logger.info("Checking if Dir is present or not");
            dirs = scanner.getIncludedDirectories();

            if (dirs.length == 1) {

                if (fileName.equalsIgnoreCase(dirs[0])) {
                    return true;
                }
            } else if (dirs.length >= 1) {
                for (String file : dirs) {

                    Files.delete(Paths.get(fileName));
                }
            }
        } else {
            logger.info("Checking if files is present or not");
            files = scanner.getIncludedFiles();

            if (files.length == 1) {
                System.out.println("File: " + files[0]);
                if (fileName.equalsIgnoreCase(files[0])) {
                    System.out.println("File: " + files[0]);
                    return true;
                }
            } else if (files.length >= 1) {
                System.out.println("File: " + files);
                for (String file : files) {

                    Files.delete(Paths.get(filePath + "/" + file));
                }
            }
        }

        return false;
    }
    String partition = "";

    private String generateInsertQuery(String trgType, String dbName, String table) {
        partition = "";
        StringBuilder buffer = new StringBuilder();
        if (trgType.equalsIgnoreCase("oracle")) {
            buffer.append("insert all ");//.append(dbName).append(".").append(table);
        } else if (trgType.equalsIgnoreCase("oracle")) {

            stmTransData.forEach(stmbean -> {
                if (stmbean.getIncrementalCheck().equalsIgnoreCase("y")) {
                    partition = stmbean.getTargetColumnName();
                }
            });
            if (partition.isEmpty()) {
                buffer.append("insert into ").append(dbName).append(".").append(table).append(" ");
            } else {
                buffer.append("insert into ").append(dbName).append(".").append(table).append(" partition (").append(partition).append(") ");
            }
        } else {

            buffer.append("insert into ").append(dbName).append(".").append(table).append(" ");
        }

        StringBuilder columns = new StringBuilder();

        for (int colIndex = 0; colIndex < stmTransData.size(); colIndex++) {
            if (colIndex == 0) {
                columns.append(" (").append(stmTransData.get(colIndex).getTargetColumnName()).append(", ");
            } else if (colIndex == stmTransData.size() - 1) {
                columns.append(stmTransData.get(colIndex).getTargetColumnName()).append(") ");
            } else {
                columns.append(stmTransData.get(colIndex).getTargetColumnName()).append(",");
            }
        }

        String colVal = "";

        if (trgType.equalsIgnoreCase("oracle")) {
            colVal = "into " + dbName + "." + table + " " + columns.toString() + " values ";
        } else {
            colVal = columns.toString() + " values ";
        }

        StringBuilder sqlValues = new StringBuilder();
        if (!trgType.equalsIgnoreCase("oracle")) {

            sqlValues.append(colVal);
        }
        for (int i = 0; i < srcCmplResult.size(); i++) {
            if (trgType.equalsIgnoreCase("oracle")) {

                sqlValues.append(colVal);
            }
            sqlValues.append(" (");
            for (int j = 0; j < srcCmplResult.get(i).size(); j++) {

                if (j == srcCmplResult.get(i).size() - 1) {

                    if (stmTransData.get(j).getTargetColumnSpec().toLowerCase().contains("varchar") || stmTransData.get(j).getTargetColumnSpec().equalsIgnoreCase("String") || stmTransData.get(j).getTargetColumnSpec().equalsIgnoreCase("date")) {

                        sqlValues.append("\"").append(srcCmplResult.get(i).get(j)).append("\"").append(")");

                        if (trgType.equalsIgnoreCase("mysql") || trgType.equalsIgnoreCase("hive")) {
                            System.out.println("srcCmplResult.size(): " + srcCmplResult.size());
                            System.out.println("i: " + i);
                            if (!(i == srcCmplResult.size() - 1)) {
                                sqlValues.append(",");
                            }

                        }

                    } else {

                        sqlValues.append(srcCmplResult.get(i).get(j)).append(")");
                        if (trgType.equalsIgnoreCase("mysql") || trgType.equalsIgnoreCase("hive")) {
                            if (!(i == srcCmplResult.size() - 1)) {
                                sqlValues.append(",");
                            }
                        }
                    }

                } else {
                    if (stmTransData.get(j).getTargetColumnSpec().toLowerCase().contains("varchar") || stmTransData.get(j).getTargetColumnSpec().equalsIgnoreCase("String") || stmTransData.get(j).getTargetColumnSpec().equalsIgnoreCase("date")) {
                        sqlValues.append("\"").append(srcCmplResult.get(i).get(j)).append("\"").append(",");

                    } else {
                        sqlValues.append(srcCmplResult.get(i).get(j)).append(",");
                    }
                }

            }
        }
        if (trgType.equalsIgnoreCase("oracle")) {
            return buffer.toString() + " " + sqlValues + " select * from dual";
        }

        return buffer.toString() + " " + sqlValues;
    }
    boolean getApproval = false;

    private boolean getLoadApproval() {

        Dialog dialogLoadAppr = new Dialog<>();

        dialogLoadAppr.setTitle("Data Load Approval");
        dialogLoadAppr.setHeaderText("Would you link to process data to Target");

        // Get the Stage.
        Stage stage = (Stage) dialogLoadAppr.getDialogPane().getScene().getWindow();
        loginButtonType = new ButtonType("PROCEED", ButtonBar.ButtonData.OK_DONE);
        test = new Button("Test");
        test.setMinWidth(100);
        dialogLoadAppr.getDialogPane().getButtonTypes().addAll(loginButtonType, ButtonType.CANCEL);
        Node loginButton = dialogLoadAppr.getDialogPane().lookupButton(loginButtonType);
        dialogLoadAppr.setResultConverter(dialogButton -> {
            if (dialogButton == loginButtonType) {
                return new Pair<>("", "");
            }
            return null;
        });
        Optional<Pair<String, String>> result = dialogLoadAppr.showAndWait();

        result.ifPresent(dbconstring -> {
            getApproval = true;
        });
        return getApproval;
    }

    @FXML
    public void exitApplication(ActionEvent event) {
        logger.info("Closing & Exiting the application");
        try {
        } catch (Exception e) {
        }

        Platform.exit();
    }
    String alertInfo;
    ObservableList trgList;

    @FXML
    public void autoLoadRunButtonAction() {

        if (getLoadApproval()) {
            getApproval = false;
            logger.info("Processing to Load data from SRC to TRG");
            if (semiautotab.isSelected()) {

                System.out.println("Selected semiautotab");

                Task task = new Task<Void>() {
                    @Override
                    public Void call() throws SQLException, IOException, FileNotFoundException, ClassNotFoundException, Exception {
                        //non ui code
                        String[] da = getSrcTransRuleData(transDataLoadTbl);
                        getIncRule();

//                    srcIncRule = incrementalCondition.get(1);
                        STMDataValidationSrcQueryGenerator datsrctranqry = new STMDataValidationSrcQueryGenerator();
                        if (getTableNameFromUI("src").equalsIgnoreCase("#multiple_tables") || commonRuletxtar.getText().toLowerCase().contains("join")) {
                            /*
                             * Step to create query when data is fetched from
                             * Multiple Tables
                             */
                            datasrcqry = datsrctranqry.getSrcDataValidationQuery(stmSrcTranData, commonRuletxtar.getText(), stmComSplRule.get("source_key"), srcIncRule);
                        } else {
                            System.out.println("Src Validation data :");
                            datasrcqry = datsrctranqry.getSrcDataValidationQuery(da, getTableNameFromUI("src").replaceAll("#", ""), getDBNameFromUI("src"), commonRuletxtar.getText().toLowerCase(), getSourceType(), stmComSplRule.get("source_key"), srcIncRule);
                        }

                        execueteTestPlan(src_data_Load, "tot_dat_val", datasrcqry, "", src_table, trg_table, null, null, getSourceType(), getTargetType());

                        if (!srcCmplResult.isEmpty()) {
                            String qry = generateInsertQuery(getDbTypeName("trg"), getDBNameFromUI("trg"), getTableNameFromUI("trg"));
                            System.out.println("Insert Query: \n" + qry);
                            logger.info("Data Count: " + srcCmplResult.size());

                            DBConnectionManager dBConnectionManager = new DBConnectionManager();
                            Connection conn = dBConnectionManager.getDBConFromFile(stmConData.get("*Target Host Name"));
                            if (conn != null) {
                                dBConnectionManager.executeInsertQuery(conn, qry);
                                QueryGenerator generator = new QueryGenerator();
                                String[] src_table = getSrcTransRuleData(transDataLoadTbl);
                                String trgQury = generator.getTotalCntQueries(getDBNameFromUI("trg"), getTableNameFromUI("trg"), src_table[0], getTargetType(), "trg", stmComSplRule.get("target_rule"), getDbTypeName("trg"), trgIncRule);
                                trgList = dBConnectionManager.getDataFromQuery(conn, trgQury);
//                                    execueteTestPlan(src_data_Load, "tot_dat_val", trgQury, "", src_table, trg_table, null, null, getSourceType(), getTargetType());
                                alertInfo = "Data has been inserted";

                            } else {
                                new ConnectException("Unable to Connect to DB");
                            }
                        } else {
                            alertInfo = "No Data is available to Insert ";
                        }

                        Platform.runLater(new Runnable() {
                            public void run() {

                                ObservableList dataVdlSmrydtls = FXCollections.observableArrayList();
                                DataVdlSummry dataVdlSummry = new DataVdlSummry();

                                dataVdlSummry.valditionType.set("Total Count");
                                dataVdlSummry.srcCnt.setValue(Integer.toString(srcCmplResult.size()));
                                dataVdlSummry.trgCnt.setValue(Integer.toString(Integer.parseInt(trgList.get(0).toString().replaceAll("\\[", "").replaceAll("\\]", ""))));
                                dataVdlSummry.result.setValue((Integer.parseInt(dataVdlSummry.srcCnt.getValue()) == Integer.parseInt(dataVdlSummry.trgCnt.getValue())) ? "Passed" : "Failed");

                                dataVdlSmrydtls.add(dataVdlSummry);
                                dataLoadVdlSmry.setItems(dataVdlSmrydtls);
                                DataValidationHighLighter(dataLoadVdlSmry);
                            }
                        });
                        return null;
                    }
                };

                task.setOnRunning(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {

                        progstatus_label.setText("Generating insert query Plan...");
                        progressLoadingImage();

                    }
                });

                task.setOnSucceeded(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {
                        progstatus_label.setText("Data Insertion Completed");
                        progressCompletedImage();
                        //callProcessTestPlan(ll);
                        //processTestPlan(ll);
                        new AlertUI(alertInfo);
                    }
                });

                task.setOnFailed(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent arg0) {
                        progstatus_label.setText("Failed with Exception");
                        progressCompletedImage();
                        Throwable throwable = task.getException();
                        Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, throwable);
                        new ExceptionUI(new Exception(throwable.getMessage()));
                    }
                });

                Thread t = new Thread(task);
                t.setDaemon(true);
                t.start();
            } else if (fullautotab.isSelected()) {

                System.out.println("Selected fullautotab");

            }

        }
    }

    private List ll;  //Select Testcase

    public void getSeletcedTestCase() throws IOException, ParseException {

        if (tfsrcconname.getText().toLowerCase().contains("flatfile")) {
            srcRTmpPath = getRemoteFile(tfsrcconname, "files/" + stmConData.get("*Source File Type (csv, txt etc)").toString().trim() + "/" + stmConData.get("*Source Host Name").toString().trim());
            System.out.println("SRC Path Received: " + srcRTmpPath);
            logger.info("Setting the Remote Path in Local for Source Data Retreival: " + srcRTmpPath);
        }
        if (tftrgconname.getText().toLowerCase().contains("flatfile")) {
            trgRTmpPath = getRemoteFile(tftrgconname, "files/" + stmConData.get("*Target File Type (csv, txt etc)").toString().trim() + "/" + stmConData.get("*Target Host Name").toString().trim());
            System.out.println("SRC Path Received: " + trgRTmpPath);
            logger.info("Setting the Remote Path in Local for Target Data Retreival: " + trgRTmpPath);
        }
        ll = getSelectedTestCases();
        System.out.println("Selected TestCases : " + ll);

        List testplan = new ArrayList();

        String[] da = this.getSrcTransRuleData(transDataTbl);
    }

    @FXML
    public void autoResultRunButtonAction() {

        try {
            getSeletcedTestCase();
            if (semiautotab.isSelected()) {

                System.out.println("Selected semiautotab");

                Task task = new Task<Void>() {
                    @Override
                    public Void call() throws SQLException, IOException, FileNotFoundException, ClassNotFoundException, Exception {
                        //non ui code
                        generateTestplan(ll);
                        Platform.runLater(new Runnable() {
                            public void run() {
                                //ui code5
                            }
                        });
                        return null;
                    }
                };

                task.setOnRunning(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {

                        progstatus_label.setText("Generating Test Plan...");
                        progressLoadingImage();

                    }
                });

                task.setOnSucceeded(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {
                        progstatus_label.setText("Test Plan Completed");
                        progressCompletedImage();
                        //callProcessTestPlan(ll);
                        processTestPlan(ll);

                    }
                });

                task.setOnFailed(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent arg0) {
                        progstatus_label.setText("Failed with Exception");
                        progressCompletedImage();
                        Throwable throwable = task.getException();
                        Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, throwable);
                        new ExceptionUI(new Exception(throwable.getMessage()));
                    }
                });

                Thread t = new Thread(task);
                t.setDaemon(true);
                t.start();
            } else if (fullautotab.isSelected()) {

                System.out.println("Selected fullautotab");

            }
        } catch (IOException ex) {
            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
            new ExceptionUI(ex);
        } catch (ParseException ex) {
            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
            new ExceptionUI(ex);
        }

    }

    public List getSelectedTestCases() {

        List testcaselist = new ArrayList();

        String tmp = chktcnts.isSelected() ? "total_cnts" : "";
        testcaselist.add(tmp);
        tmp = chkncnts.isSelected() ? "null_cnts" : "";
        testcaselist.add(tmp);
        tmp = chknncnts.isSelected() ? "not_null_cnts" : "";
        testcaselist.add(tmp);
        tmp = chkdupcnts.isSelected() ? "dup_cnts" : "";
        testcaselist.add(tmp);
        tmp = chkdstcnts.isSelected() ? "dst_cnts" : "";
        testcaselist.add(tmp);
        tmp = chksumnum.isSelected() ? "sum_num_cols" : "";
        testcaselist.add(tmp);
        tmp = chkmax.isSelected() ? "max_cols" : "";
        testcaselist.add(tmp);
        tmp = chkmin.isSelected() ? "min_cols" : "";
        testcaselist.add(tmp);
        tmp = chkcmpltdata.isSelected() ? "tot_dat_val" : "";
        testcaselist.add(tmp);
        tmp = chkfst1000.isSelected() ? "fst_1000_dat_val" : "";
        testcaselist.add(tmp);
        tmp = chklst1000.isSelected() ? "lst_1000_dat_val" : "";
        testcaselist.add(tmp);
        tmp = chkincrdata.isSelected() ? "incr_dat_val" : "";
        testcaselist.add(tmp);

        return testcaselist;
    }

    public String getTableNameFromUI(String type) {

        String tablename = "";

        if (type.equalsIgnoreCase("src")) {

            if (!tfsrcconname.getText().equalsIgnoreCase("") && !tfsrcconname.getText().contains("FlatFile")) {

                tablename = tfsrcconname.getText().split("::")[2];

            } else if (tfsrcconname.getText().contains("FlatFile")) {

                tablename = tfsrcconname.getText().split("::")[2];

                File f = new File(tablename);
                tablename = f.getName();

            }
        } else if (type.equalsIgnoreCase("trg")) {

            if (!tftrgconname.getText().equalsIgnoreCase("") && !tftrgconname.getText().contains("FlatFile")) {

                tablename = tftrgconname.getText().split("::")[2];

            } else if (tftrgconname.getText().contains("FlatFile")) {

                tablename = tftrgconname.getText().split("::")[2];
                System.out.println("tablename: " + tablename);
                File f = new File(tablename);
                tablename = f.getName();

            }

        }

        return tablename;
    }

    public String getConnNameFromUI(String type) {

        String connname = "";

        if (type.equalsIgnoreCase("src")) {

            if (!tfsrcconname.getText().equalsIgnoreCase("") && !tfsrcconname.getText().contains("FlatFile")) {
                connname = tfsrcconname.getText().split("::")[0];
            } else if (tfsrcconname.getText().contains("FlatFile")) {
                System.out.println("Connetion: " + tfsrcconname);
                connname = tfsrcconname.getText().split("::")[2];

                System.out.println("Connetion name: " + connname);
            }
        } else if (type.equalsIgnoreCase("trg")) {

            if (!tftrgconname.getText().equalsIgnoreCase("") && !tftrgconname.getText().contains("FlatFile")) {
                connname = tftrgconname.getText().split("::")[0];
            } else if (tftrgconname.getText().contains("FlatFile")) {

                connname = tftrgconname.getText().split("::")[2];

            }

        }

        return connname;
    }

    public String getDBNameFromUI(String type) {

        String dbname = "";

        if (type.equalsIgnoreCase("src")) {
            if (!tfsrcconname.getText().equalsIgnoreCase("") && !tfsrcconname.getText().contains("FlatFile")) {
                dbname = tfsrcconname.getText().split("::")[1];
            }
            if (!tfsrcconname.getText().equalsIgnoreCase("") && tfsrcconname.getText().contains("FlatFile")) {
                dbname = tfsrcconname.getText().split("::")[2];
            }
        } else {
            if (!tftrgconname.getText().equalsIgnoreCase("") && !tftrgconname.getText().contains("FlatFile")) {
                dbname = tftrgconname.getText().split("::")[1];

            }
            if (!tftrgconname.getText().equalsIgnoreCase("") && tftrgconname.getText().contains("FlatFile")) {

                dbname = tftrgconname.getText().split("::")[2];
            }
        }
        System.out.println("DbName: " + dbname);
        return dbname;
    }

    public String getDbTypeName(String type) {
        logger.info("DB MAP Type: " + type);
        String dbname = "";

        if (type.equalsIgnoreCase("src")) {
            System.out.println("Source Type: " + dbname);
            if (!tfsrcconname.getText().equalsIgnoreCase("") && !tfsrcconname.getText().contains("FlatFile")) {
                dbname = tfsrcconname.getText().split("::")[0];
                dbname = dbname.substring(dbname.indexOf("#") + 1, dbname.lastIndexOf("#"));
                System.out.println("Source Type: " + dbname);
            }
            if (!tfsrcconname.getText().equalsIgnoreCase("") && tfsrcconname.getText().contains("FlatFile")) {
                dbname = "";
                System.out.println("Source Type1: " + dbname);
            }

        } else {
            if (!tftrgconname.getText().equalsIgnoreCase("") && !tftrgconname.getText().contains("FlatFile")) {
                dbname = tftrgconname.getText().split("::")[0];
                dbname = dbname.substring(dbname.indexOf("#") + 1, dbname.lastIndexOf("#"));
            }
            if (!tftrgconname.getText().equalsIgnoreCase("") && tftrgconname.getText().contains("FlatFile")) {
                dbname = "";

            }

        }
        System.out.println("DbNameType: " + dbname);

        return dbname;

    }

    public String getSourceType() {
        if (tfsrcconname.getText().toLowerCase().contains("flatfile")) {

            return "ff";
        } else {
            return "db";
        }

    }

    public String getTargetType() {
        if (tftrgconname.getText().toLowerCase().contains("flatfile")) {
            return "ff";
        } else {
            return "db";
        }

    }

    private String srcIncRule = "";
    private String trgIncRule = "";

    private void getIncRule() {
        if ((incrementalCondition.size() == 2) && chkincrdata.isSelected()) {

            trgIncRule = incrementalCondition.get(0);
            srcIncRule = incrementalCondition.get(1);
        } else if ((incrementalCondition.size() == 2) && tabdataload.isSelected()) {
            trgIncRule = incrementalCondition.get(0);
            srcIncRule = incrementalCondition.get(1);
        } else {
            trgIncRule = "";
            srcIncRule = "";
        }
    }

    private void generateTestplan(List ll) throws SQLException, IOException, FileNotFoundException, ClassNotFoundException, Exception {

        //total_cnts, null_cnts, not_null_cnts, dup_cnts, dst_cnts, sum_num_cols, max_cols, min_cols
        String[] src_table = getSrcTransRuleData(transDataTbl);//need to updated with stm src transcol ;
        String[] trg_table = getTargetColumnData();//need to updated with stm trg col ;
//        System.out.println(this.getDBNameFromUI("src") + " : " + this.getTableNameFromUI("src"));
        getIncRule();
        if (src_table.length == 0 || trg_table.length == 0) {

            new AlertUI("[ERROR] Missing Source / Target");
        } else {
            QueryGenerator qgen = new QueryGenerator();

            total_cnt_testplan = new HashMap<String, String>();
            null_cnt_testplan = new HashMap<String, String>();
            notnull_cnt_testplan = new HashMap<String, String>();
            dup_cnt_testplan = new HashMap<String, String>();
            dst_cnt_testplan = new HashMap<String, String>();
            sum_num_testplan = new HashMap<String, String>();
            max_col_testplan = new HashMap<String, String>();
            min_col_testplan = new HashMap<String, String>();
            cmpl_data_testplan = new HashMap<String, String>();
            try {
                for (Object item : ll) {

                    if (item.toString().equals("total_cnts")) {
                        if (this.getSourceType().equalsIgnoreCase("ff")) {
                            System.out.println("DB Name: " + this.getDBNameFromUI("src"));
                            System.out.println("Table Name: " + this.getTableNameFromUI("src"));
                            System.out.println("src_table[0]: " + src_table[0]);
                            //need to update qgen class
                            total_cnt_testplan.put("Total_Cnt_Src_Testcase", qgen.getTotalCntQueries(this.getDBNameFromUI("src").replaceAll("#", ""), this.getTableNameFromUI("src").replaceAll("#", ""), src_table[0], getSourceType(), "src", stmComSplRule.get("common_rule"), this.getDbTypeName("src"), srcIncRule));
                        } else {
                            //need to update qgen class
//                            String[] srcColTran = getSrcTransRuleData();
//                            System.out.println("DB Name: " + this.getDBNameFromUI("src")+" :: "+srcColTran.length);
//                            for (int i = 0; i < srcColTran.length; i++) {
//
//                                if (!srcColTran[i].equalsIgnoreCase("#INPROGRESS")) {
//                                    total_cnt_testplan.put("Total_Cnt_Src_Testcase", qgen.getTotalCntQueries(this.getDBNameFromUI("src"), srcColTran[i], "src", "", ""));
//                                    break;
//                                }
//                            }
                            System.out.println("SRC DB");
                            total_cnt_testplan.put("Total_Cnt_Src_Testcase", qgen.getTotalCntQueries(this.getDBNameFromUI("src"), this.getTableNameFromUI("src").replaceAll("#", ""), src_table[0], getSourceType(), "src", "", this.getDbTypeName("src"), srcIncRule));

                        }

                        System.out.println("DB Name: " + this.getDBNameFromUI("trg"));
                        if (this.getTargetType().equalsIgnoreCase("ff")) {
                            System.out.println("TRG FIle");
                            //need to update qgen class
                            total_cnt_testplan.put("Total_Cnt_Trg_Testcase", qgen.getTotalCntQueries(this.getDBNameFromUI("trg"), this.getTableNameFromUI("trg"), src_table[0], getTargetType(), "trg", stmComSplRule.get("target_rule"), this.getDbTypeName("trg"), trgIncRule));
                        } else {
                            System.out.println("TRG DB");
                            //need to update qgen class
                            total_cnt_testplan.put("Total_Cnt_Trg_Testcase", qgen.getTotalCntQueries(this.getDBNameFromUI("trg"), this.getTableNameFromUI("trg"), src_table[0], getTargetType(), "trg", stmComSplRule.get("target_rule"), this.getDbTypeName("trg"), trgIncRule));
                        }

                        System.out.println("Count Test Plan :" + total_cnt_testplan);

                    }

                    if (item.toString().equals("null_cnts")) {

                        List testcase = new ArrayList();
                        System.out.println("Null Count Data:");

                        //src and trg
                        for (int i = 0; i < src_table.length; i++) {
                            System.out.println("Target rule: " + stmComSplRule.get("target_rule"));
                            System.out.println("Source rule: " + stmComSplRule.get("common_rule"));
                            //need to update qgen class
                            if (!src_table[i].equalsIgnoreCase("#INPROGRESS")) {
                                null_cnt_testplan.put("Null_Cnt_Src_Testcase_" + i, qgen.getNullCntQueries(this.getDBNameFromUI("src"), this.getTableNameFromUI("src").replaceAll("#", ""), src_table[i], this.getSourceType(), "src", stmComSplRule.get("common_rule"), this.getDbTypeName("trg"), srcIncRule, this.getDBNameFromUI("src")));
                                null_cnt_testplan.put("Null_Cnt_Trg_Testcase_" + i, qgen.getNullCntQueries(this.getDBNameFromUI("trg"), this.getTableNameFromUI("trg"), trg_table[i], this.getTargetType(), "trg", stmComSplRule.get("target_rule"), this.getDbTypeName("trg"), trgIncRule, this.getDBNameFromUI("trg")));
                            } else {
                                null_cnt_testplan.put("Null_Cnt_Src_Testcase_" + i, "");
                                null_cnt_testplan.put("Null_Cnt_Trg_Testcase_" + i, "");
                            }
                            //need to update qgen class

                        }

                        System.out.println("Null Count Test Plan :" + null_cnt_testplan);

                    }

                    if (item.toString().equals("not_null_cnts")) {

                        List testcase = new ArrayList();
                        System.out.println("Not Null Count Data:");
                        //src and trg
                        for (int i = 0; i < src_table.length; i++) {

                            //need to update qgen class
                            if (!src_table[i].equalsIgnoreCase("#INPROGRESS")) {
                                notnull_cnt_testplan.put("NotNull_Cnt_Src_Testcase_" + i, qgen.getNotNullCntQueries(this.getDBNameFromUI("src"), this.getTableNameFromUI("src").replaceAll("#", ""), src_table[i], this.getSourceType(), "src", stmComSplRule.get("common_rule"), srcIncRule, this.getDBNameFromUI("src")));
                                notnull_cnt_testplan.put("NotNull_Cnt_Trg_Testcase_" + i, qgen.getNotNullCntQueries(this.getDBNameFromUI("trg"), this.getTableNameFromUI("trg"), trg_table[i], this.getTargetType(), "trg", stmComSplRule.get("target_rule"), trgIncRule, this.getDBNameFromUI("trg")));
                            } else {
                                notnull_cnt_testplan.put("NotNull_Cnt_Src_Testcase_" + i, "");
                                notnull_cnt_testplan.put("NotNull_Cnt_Trg_Testcase_" + i, "");
                            }
                            //need to update qgen class

                        }

                        System.out.println("Not Null Count Test Plan :" + notnull_cnt_testplan);

                    }

                    if (item.toString().equals("dst_cnts")) {

                        List testcase = new ArrayList();
                        System.out.println("Distinct Count Data:");
                        //src and trg
                        for (int i = 0; i < src_table.length; i++) {

                            //need to update qgen class
                            if (!src_table[i].equalsIgnoreCase("#INPROGRESS")) {
                                dst_cnt_testplan.put("Dst_Cnt_Src_Testcase_" + i, qgen.getDistinctCntQueries(this.getDBNameFromUI("src"), this.getTableNameFromUI("src").replaceAll("#", ""), src_table[i], this.getSourceType(), "src", stmComSplRule.get("common_rule"), srcIncRule, this.getDBNameFromUI("src")));
                                dst_cnt_testplan.put("Dst_Cnt_Trg_Testcase_" + i, qgen.getDistinctCntQueries(this.getDBNameFromUI("trg"), this.getTableNameFromUI("trg"), trg_table[i], this.getTargetType(), "trg", stmComSplRule.get("target_rule"), trgIncRule, this.getDBNameFromUI("trg")));
                            } else {
                                dst_cnt_testplan.put("Dst_Cnt_Src_Testcase_" + i, "");
                                dst_cnt_testplan.put("Dst_Cnt_Trg_Testcase_" + i, "");
                            }
                            //need to update qgen class

                        }

                        System.out.println("Distinct Count Test Plan :" + dst_cnt_testplan);

                    }

                    if (item.toString().equals("dup_cnts")) {

                        List testcase = new ArrayList();
                        System.out.println("Duplicate Count Data:");
                        //src and trg
                        for (int i = 0; i < src_table.length; i++) {
                            //need to update qgen class
                            if (!src_table[i].equalsIgnoreCase("#INPROGRESS")) {
                                dup_cnt_testplan.put("Dup_Cnt_Src_Testcase_" + i, qgen.getDuplicateCntQueries(this.getDBNameFromUI("src"), this.getTableNameFromUI("src").replaceAll("#", ""), src_table[i], this.getSourceType(), "src", stmComSplRule.get("common_rule"), this.getDbTypeName("src"), srcIncRule, this.getDBNameFromUI("src")));
                                dup_cnt_testplan.put("Dup_Cnt_Trg_Testcase_" + i, qgen.getDuplicateCntQueries(this.getDBNameFromUI("trg"), this.getTableNameFromUI("trg"), trg_table[i], this.getTargetType(), "trg", stmComSplRule.get("target_rule"), this.getDbTypeName("trg"), trgIncRule, this.getDBNameFromUI("trg")));
                            } else {
                                dup_cnt_testplan.put("Dup_Cnt_Src_Testcase_" + i, "");
                                dup_cnt_testplan.put("Dup_Cnt_Trg_Testcase_" + i, "");
                            }
                            //need to update qgen class

                        }

                        System.out.println("Duplicate Count Test Plan :" + dup_cnt_testplan);

                    }

                    if (item.toString().equals("max_cols")) {

                        List testcase = new ArrayList();
                        System.out.println("Max Count Data:");
                        //src and trg
                        for (int i = 0; i < src_table.length; i++) {

                            //need to update qgen class
                            if (!src_table[i].equalsIgnoreCase("#INPROGRESS")) {
                                max_col_testplan.put("Max_Col_Src_Testcase_" + i, qgen.getMaxColQueries(this.getDBNameFromUI("src"), this.getTableNameFromUI("src").replaceAll("#", ""), src_table[i], this.getSourceType(), "src", stmComSplRule.get("common_rule"), srcIncRule));
                                max_col_testplan.put("Max_Col_Trg_Testcase_" + i, qgen.getMaxColQueries(this.getDBNameFromUI("trg"), this.getTableNameFromUI("trg"), trg_table[i], this.getTargetType(), "trg", stmComSplRule.get("target_rule"), trgIncRule));
                            } else {
                                max_col_testplan.put("Max_Col_Src_Testcase_" + i, "");
                                max_col_testplan.put("Max_Col_Trg_Testcase_" + i, "");
                            }
                            //need to update qgen class

                        }

                        System.out.println("Max of Col Test Plan :" + max_col_testplan.size());

                    }

                    if (item.toString().equals("min_cols")) {

                        List testcase = new ArrayList();
                        System.out.println("Min Count Data:");
                        //src and trg
                        for (int i = 0; i < src_table.length; i++) {
                            //need to update qgen class
                            if (!src_table[i].equalsIgnoreCase("#INPROGRESS")) {
                                min_col_testplan.put("Min_Col_Src_Testcase_" + i, qgen.getMinColQueries(this.getDBNameFromUI("src"), this.getTableNameFromUI("src").replaceAll("#", ""), src_table[i], this.getSourceType(), "src", stmComSplRule.get("common_rule"), srcIncRule));
                                min_col_testplan.put("Min_Col_Trg_Testcase_" + i, qgen.getMinColQueries(this.getDBNameFromUI("trg"), this.getTableNameFromUI("trg"), trg_table[i], this.getTargetType(), "trg", stmComSplRule.get("target_rule"), trgIncRule));
                            } else {
                                min_col_testplan.put("Min_Col_Src_Testcase_" + i, "");
                                min_col_testplan.put("Min_Col_Trg_Testcase_" + i, "");
                            }
                            //need to update qgen class

                        }

                        System.out.println("Min of Col Test Plan :" + min_col_testplan);

                    }

                    if (item.toString().equals("sum_num_cols")) {
                        String trgType = "";
                        String srcType = "";
                        System.out.println("Sum Count Data:");
                        DBConnectionManager dbConManager1 = new DBConnectionManager();
                        CSVSQLEngine cSVSQLEngine = new CSVSQLEngine();
                        Connection srccon = null;

                        //Connection Type
                        if (this.getSourceType().equalsIgnoreCase("ff")) {
                            System.out.println("Paths: " + srcRTmpPath);
                            csvengine = new CSVSQLEngine();
                            if (srcRTmpPath != null && !srcRTmpPath.isEmpty()) {

                                srccon = csvengine.getFFConn(new File(srcRTmpPath).getParent());
                            } else {
                                srccon = csvengine.getFFConn(new File(this.getDBNameFromUI("src")).getParent());
                            }
                        } else {
                            srccon = dbConManager1.getDBConFromFile(stmConData.get("*Source Host Name"));
                        }

                        List colType = null;
                        SqlParser sqlParser = new SqlParser();
                        //src and trg
                        for (int i = 0; i < src_table.length; i++) {

                            if (!src_table[i].equalsIgnoreCase("#INPROGRESS")) {

                                if (stmTransData.get(i).getSourceColumnSpec().isEmpty()) {

                                    colType = dbConManager1.getColType(srccon, src_table[i]);
                                } else {

                                    colType = new ArrayList();
                                    colType.add(stmTransData.get(i).getSourceColumnSpec().trim());
                                }

                                if (colType.get(0).toString().toLowerCase().contains("number") || colType.get(0).toString().toLowerCase().contains("decimal") || colType.get(0).toString().toLowerCase().contains("integer")) {

                                    sum_num_testplan.put("Sum_Col_Src_Testcase_" + i, qgen.getSumColQueries(this.getDBNameFromUI("src"), this.getTableNameFromUI("src").replaceAll("#", ""), src_table[i], this.getSourceType(), "src", colType.toString().replace("[", "").replace("]", ""), stmComSplRule.get("common_rule"), srcIncRule));
                                    sum_num_testplan.put("Sum_Col_Trg_Testcase_" + i, qgen.getSumColQueries(this.getDBNameFromUI("trg"), this.getTableNameFromUI("trg"), trg_table[i], this.getTargetType(), "trg", colType.toString().replace("[", "").replace("]", ""), stmComSplRule.get("target_rule"), trgIncRule));
                                } else {

                                    sum_num_testplan.put("Sum_Col_Src_Testcase_" + i, "");
                                    sum_num_testplan.put("Sum_Col_Trg_Testcase_" + i, "");
                                }
                            } else {
                                sum_num_testplan.put("Sum_Col_Src_Testcase_" + i, "");
                                sum_num_testplan.put("Sum_Col_Trg_Testcase_" + i, "");
                            }

                        }

                        System.out.println("Sum of Col Test Plan :" + sum_num_testplan);

                    }

                    if (item.toString().equals("tot_dat_val") || item.toString().equals("fst_1000_dat_val") || item.toString().equals("lst_1000_dat_val") || item.toString().equals("incr_dat_val")) {
                        System.out.println("Advance Count Data:");
                        String[] da = getSrcTransRuleData(transDataTbl);

                        STMDataValidationSrcQueryGenerator datsrctranqry = new STMDataValidationSrcQueryGenerator();

                        if (getTableNameFromUI("src").equalsIgnoreCase("#multiple_tables") || commonRuletxtar.getText().toLowerCase().contains("join")) {
                            /*
                             * Step to create query when data is fetched from
                             * Multiple Tables
                             */
                            datasrcqry = datsrctranqry.getSrcDataValidationQuery(stmSrcTranData, commonRuletxtar.getText(), stmComSplRule.get("source_key"), srcIncRule);
                        } else {
                            System.out.println("Src Validation data :");
                            datasrcqry = datsrctranqry.getSrcDataValidationQuery(da, getTableNameFromUI("src").replaceAll("#", ""), getDBNameFromUI("src"), commonRuletxtar.getText().toLowerCase(), getSourceType(), stmComSplRule.get("source_key"), srcIncRule);
                        }
                        STMDataValidationTrgQueryGenerator dattrgTranqry = new STMDataValidationTrgQueryGenerator();
                        if (getTargetType().equalsIgnoreCase("db")) {
                            System.out.println("Trg Validation data DB:");
                            datatrgqry = dattrgTranqry.getFinalDataSelectQuery(da, this.getDBNameFromUI("trg") + "." + this.getTableNameFromUI("trg"), stmComSplRule.get("target_rule"), stmComSplRule.get("target_key"), trgIncRule);
                        } else {
                            System.out.println("Trg Validation data FF:");
                            datatrgqry = dattrgTranqry.getFinalDataSelectQuery(da, "\"" + this.getTableNameFromUI("trg") + "\"", stmComSplRule.get("target_rule"), stmComSplRule.get("target_key"), trgIncRule);

                        }

                        cmpl_data_testplan.put("Compl_Data_Src_Testcase", datasrcqry);
                        cmpl_data_testplan.put("Compl_Data_Trg_Testcase", datatrgqry);
                        System.out.println("Source Data validation Queries :" + datasrcqry);
                        System.out.println("Target Data validation Queries :" + datatrgqry);

                    }

                }

            } catch (JSQLParserException ex) {

                ex.printStackTrace();
                new ExceptionUI(ex);
            }
        }
    }

    public void progressCompletedImage() {

        proggif_image.setImage(new Image(getClass().getResourceAsStream("/icon/progcmpleted.gif")));

    }

    public void progressLoadingImage() {

        proggif_image.setImage(new Image(getClass().getResourceAsStream("/icon/proggif.gif")));

    }

    //Method to execute and fetch the data of the test plan --Adithyathrows IOException, ClassNotFoundException, SQLException, Exception 
    public ObservableList execueteTestPlan(ObservableList dataStore, String item, String srcQuery, String trgQuery, List srcHeader, List trgHeader, Connection srcCon, Connection trgCon, String srcType, String trgType) throws Exception {

        //Display execution parameters
        //System.out.println("execueteTestPlan Called");
        ObservableList srcResult = null;
        ObservableList trgResult = null;
        DBConnectionManager dbConManager = null;
        Connection srccon1 = null, trgcon1 = null;
        try {
            SqlParser sqlParser = new SqlParser();
            if (tabautotesting.isSelected()) {
                if (srcType.equalsIgnoreCase("db") && trgType.equalsIgnoreCase("db")) {
                    System.out.println("Exec Start");
                    dbConManager = new DBConnectionManager();
                    srccon1 = dbConManager.getDBConFromFile(stmConData.get("*Source Host Name"));
                    trgcon1 = dbConManager.getDBConFromFile(stmConData.get("*Target Host Name"));
                    System.out.println("Exec Connection End");
                    srcResult = dbConManager.getDataFromQuery(srccon1, srcQuery);
                    trgResult = dbConManager.getDataFromQuery(trgcon1, trgQuery);
                    System.out.println("Got Data: ");

                } //db to file
                else if (srcType.equalsIgnoreCase("db") && trgType.equalsIgnoreCase("ff")) {
                    System.out.println(" DB and Flat File  Data fetching");
                    dbConManager = new DBConnectionManager();
                    srccon1 = dbConManager.getDBConFromFile(stmConData.get("*Source Host Name"));
                    srcResult = dbConManager.getDataFromQuery(srccon1, srcQuery);
                    csvengine = new CSVSQLEngine();

                    if (trgRTmpPath != null && !trgRTmpPath.isEmpty()) {
                        if (item.equals("dup_cnts")) {
                            trgResult = csvengine.getFFDuplicateCount(csvengine.getFFConn(new File(trgRTmpPath).getParent() + "?schema=" + FILEPATH), trgQuery);
                        } else {
                            trgResult = csvengine.getFFTableData(new File(trgRTmpPath).getParent() + "?schema=" + FILEPATH, trgQuery);
                        }

                    } else {
                        if (item.equals("dup_cnts")) {
                            trgResult = csvengine.getFFDuplicateCount(csvengine.getFFConn(new File(getDBNameFromUI("trg")).getParent() + "?schema=" + FILEPATH), trgQuery);
                        } else {
                            trgResult = csvengine.getFFTableData(new File(getDBNameFromUI("trg")).getParent() + "?schema=" + FILEPATH, trgQuery);
                        }
                    }

                }//file to db
                else if (srcType.equalsIgnoreCase("ff") && trgType.equalsIgnoreCase("db")) {
                    System.out.println("Flat File and DB Data fetching");
                    dbConManager = new DBConnectionManager();
                    trgcon1 = dbConManager.getDBConFromFile(stmConData.get("*Target Host Name"));
                    trgResult = dbConManager.getDataFromQuery(trgcon1, trgQuery);
                    csvengine = new CSVSQLEngine();

                    if (srcRTmpPath != null && !srcRTmpPath.isEmpty()) {

                        if (item.equals("dup_cnts")) {
                            srcResult = csvengine.getFFDuplicateCount(csvengine.getFFConn(new File(srcRTmpPath).getParent() + "?schema=" + FILEPATH), srcQuery);
                        } else {
                            srcResult = csvengine.getFFTableData(new File(srcRTmpPath).getParent() + "?schema=" + FILEPATH, srcQuery);
                        }
                    } else {

                        if (item.equals("dup_cnts")) {
                            srcResult = csvengine.getFFDuplicateCount(csvengine.getFFConn(new File(getDBNameFromUI("src")).getParent() + "?schema=" + FILEPATH), srcQuery);
                        } else {
                            srcResult = csvengine.getFFTableData(new File(getDBNameFromUI("src")).getParent() + "?schema=" + FILEPATH, srcQuery);
                        }
                    }
                }//file to file
                else if (srcType.equalsIgnoreCase("ff") && trgType.equalsIgnoreCase("ff")) {
                    csvengine = new CSVSQLEngine();
                    System.out.println("Flat File Data Fetching");

                    if (trgRTmpPath != null && !trgRTmpPath.isEmpty()) {
                        if (item.equals("dup_cnts")) {
                            trgResult = csvengine.getFFDuplicateCount(csvengine.getFFConn(new File(trgRTmpPath).getParent() + "?schema=" + FILEPATH), trgQuery);
                        } else {
                            trgResult = csvengine.getFFTableData(new File(trgRTmpPath).getParent() + "?schema=" + FILEPATH, trgQuery);
                        }

                    } else {
                        if (item.equals("dup_cnts")) {
                            trgResult = csvengine.getFFDuplicateCount(csvengine.getFFConn(new File(getDBNameFromUI("trg")).getParent() + "?schema=" + FILEPATH), trgQuery);
                        } else {
                            trgResult = csvengine.getFFTableData(new File(getDBNameFromUI("trg")).getParent() + "?schema=" + FILEPATH, trgQuery);
                        }
                    }

                    if (srcRTmpPath != null && !srcRTmpPath.isEmpty()) {
                        System.out.println("Src R TMp : " + srcRTmpPath);
                        if (item.equals("dup_cnts")) {
                            srcResult = csvengine.getFFDuplicateCount(csvengine.getFFConn(new File(srcRTmpPath).getParent() + "?schema=" + FILEPATH), srcQuery);
                        } else {
                            srcResult = csvengine.getFFTableData(new File(srcRTmpPath).getParent() + "?schema=" + FILEPATH, srcQuery);
                        }
                    } else {
                        System.out.println("YI : " + getDBNameFromUI("src"));
                        if (item.equals("dup_cnts")) {
                            srcResult = csvengine.getFFDuplicateCount(csvengine.getFFConn(new File(this.getDBNameFromUI("src")).getParent() + "?schema=" + FILEPATH), srcQuery);
                        } else {
                            srcResult = csvengine.getFFTableData(new File(this.getDBNameFromUI("src")).getParent() + "?schema=" + FILEPATH, srcQuery);
                        }
                    }
                }

            } else {
                if (srcType.equalsIgnoreCase("db")) {
                    dbConManager = new DBConnectionManager();
                    srccon1 = dbConManager.getDBConFromFile(stmConData.get("*Source Host Name"));
                    srcResult = dbConManager.getDataFromQuery(srccon1, srcQuery);

                } else {
                    csvengine = new CSVSQLEngine();
                    if (srcRTmpPath != null && !srcRTmpPath.isEmpty()) {
                        System.out.println("Src R TMp : " + srcRTmpPath);
                        if (item.equals("dup_cnts")) {
                            srcResult = csvengine.getFFDuplicateCount(csvengine.getFFConn(new File(srcRTmpPath).getParent() + "?schema=" + FILEPATH), srcQuery);
                        } else {
                            srcResult = csvengine.getFFTableData(new File(srcRTmpPath).getParent() + "?schema=" + FILEPATH, srcQuery);
                        }
                    } else {
                        System.out.println("YI : " + getDBNameFromUI("src"));
                        if (item.equals("dup_cnts")) {
                            srcResult = csvengine.getFFDuplicateCount(csvengine.getFFConn(new File(this.getDBNameFromUI("src")).getParent() + "?schema=" + FILEPATH), srcQuery);
                        } else {
                            srcResult = csvengine.getFFTableData(new File(this.getDBNameFromUI("src")).getParent() + "?schema=" + FILEPATH, srcQuery);
                        }
                    }
                }
            }
            System.out.println("Item: " + item);

            if (item.equals("total_cnts")) {
                System.out.println("Total Count Data:");

                srcCnt = srcResult.get(0).toString().replaceAll("\\[", "").replaceAll("\\]", "");
                trgCnt = trgResult.get(0).toString().replaceAll("\\[", "").replaceAll("\\]", "");
                bean = new TotalCountBean();
                bean.srcCnt.setValue(srcCnt);
                bean.trgCnt.setValue(trgCnt);
                bean.totCnt.setValue(bean.getSrcCnt().equals(bean.getTrgCnt()) ? "Passed" : "Failed");

                dataStore.add(bean);

            } else if (item.equals("tot_dat_val") || item.equals("incr_dat_val")) {
                //System.out.println("Src Data : " + srcResult.toString().replaceAll("\\[", "").replaceAll("\\]", ""));
                //System.out.println("Target Data: " + trgResult.toString().replaceAll("\\[", "").replaceAll("\\]", ""));
                srcCmplResult = srcResult;
                if (trgResult != null) {
                    trgCmplResult = trgResult;
                }
//                dataStore.add(srcCmplResult);

            } else if (item.equals("dup_cnts")) {

                System.out.println("dup_cnts: " + srcResult.toString().replaceAll("\\[", "").replaceAll("\\]", ""));
                System.out.println("dup_cnts: " + trgResult.toString().replaceAll("\\[", "").replaceAll("\\]", ""));
//            srcCnt = srcResult.get(0).toString().replaceAll("\\[", "").replaceAll("\\]", "");
//            trgCnt = trgResult.get(0).toString().replaceAll("\\[", "").replaceAll("\\]", "");

//            CountsMaxMinBean countsMaxMinBean = new CountsMaxMinBean();
//            countsMaxMinBean.srcCol.setValue(sqlParser.getColumnNamefromQuery(srcQuery.replaceAll("/", ".")));
////            countsMaxMinBean.srcColCount.setValue(Integer.toString(Integer.parseInt(srcCnt) - Integer.parseInt(srcResult.toString().replaceAll("\\[", "").replaceAll("\\]", ""))));
//            countsMaxMinBean.srcColCount.setValue(srcResult.toString().replaceAll("\\[", "").replaceAll("\\]", ""));
//            countsMaxMinBean.trgCol.setValue(sqlParser.getColumnNamefromQuery(trgQuery.replaceAll("/", ".")));
                String src = "";
                System.out.println("srcResult.size(): " + srcResult.size());
                if (srcResult.size() > 1) {
                    int count = 0;
                    for (int i = 0; i < srcResult.size(); i++) {
                        System.out.println("Count: " + count);

                        System.out.println("SRC Value: " + srcResult.get(i).toString().replaceAll("\\[", "").replaceAll("\\]", ""));
                        System.out.println("Value: " + Integer.parseInt(srcResult.get(i).toString().replaceAll("\\[", "").replaceAll("\\]", "")));
                        count += Integer.parseInt(srcResult.get(i).toString().replaceAll("\\[", "").replaceAll("\\]", ""));
                    }
                    System.out.println("COunt: Data " + count);
                    src = Integer.toString(count);
                } else if (srcResult.isEmpty()) {
                    src = "0";
                } else {
                    src = srcResult.get(0).toString().replace("[", "").replace("]", "");
                }

                String srcCol = "";
                String srcColCal = sqlParser.getColumnNamefromQuery(srcQuery.replaceAll("/", "."));
                if (this.getDbTypeName("trg").equalsIgnoreCase("bigquery")) {
                    srcCol = srcColCal.substring(6, srcColCal.length() - 1);
                } else {
                    srcCol = srcColCal;
                }
                CountsMaxMinBean countsMaxMinBean = new CountsMaxMinBean();
                countsMaxMinBean.srcCol.setValue(srcCol);

                countsMaxMinBean.srcColCount.setValue(src);
//                countsMaxMinBean.srcColCount.setValue(srcResult.size() > 1 ? Integer.toString(srcResult.size()) : srcResult.get(0).toString());

                String trgCol = "";
                String trgColCal = sqlParser.getColumnNamefromQuery(trgQuery.replaceAll("/", "."));
                if (this.getDbTypeName("trg").equalsIgnoreCase("bigquery")) {
                    trgCol = trgColCal.substring(6, trgColCal.length() - 1);
                } else {
                    trgCol = trgColCal;
                }
                countsMaxMinBean.trgCol.setValue(trgCol);
//System.out.println("Target Col: "+trgCol);
                String trg = "";
                if (trgResult.size() > 1) {

                    int trgcount = 0;
                    for (int i = 0; i < trgResult.size(); i++) {

                        trgcount += Integer.parseInt(trgResult.get(i).toString().replaceAll("\\[", "").replaceAll("\\]", ""));
                    }
                    trg = Integer.toString(trgcount);
                } else if (trgResult.isEmpty()) {
                    trg = "0";
                } else {
                    trg = trgResult.get(0).toString().replace("[", "").replace("]", "");
                }
//            countsMaxMinBean.trgColCount.setValue(Integer.toString(Integer.parseInt(trgCnt) - Integer.parseInt(trgResult.toString().replaceAll("\\[", "").replaceAll("\\]", ""))));
                countsMaxMinBean.trgColCount.setValue(trg);
                countsMaxMinBean.result.setValue(countsMaxMinBean.getSrcColCount().equals(countsMaxMinBean.getTrgColCount()) ? "Passed" : "Failed");

                dataStore.add(countsMaxMinBean);

            } else //if (!item.equals("dup_cnts") && !item.equals("tot_dat_val") && !item.equals("total_cnts")) {
            {
                CountsMaxMinBean countsMaxMinBean = new CountsMaxMinBean();
                String srcValue = srcResult.toString().replaceAll("\\[", "").replaceAll("\\]", "");
                String trgValue = trgResult.toString().replaceAll("\\[", "").replaceAll("\\]", "");
                countsMaxMinBean.srcCol.setValue(sqlParser.getColumnNamefromQuery(srcQuery.replaceAll("/", ".")));
                countsMaxMinBean.srcColCount.setValue(srcValue.isEmpty() ? "0" : srcValue);
                countsMaxMinBean.trgCol.setValue(sqlParser.getColumnNamefromQuery(trgQuery.replaceAll("/", ".")));
                countsMaxMinBean.trgColCount.setValue(trgValue.isEmpty() ? "0" : trgValue);
                countsMaxMinBean.result.setValue(countsMaxMinBean.getSrcColCount().equals(countsMaxMinBean.getTrgColCount()) ? "Passed" : "Failed");

                dataStore.add(countsMaxMinBean);

            }

        } catch (Exception ex) {
            throw ex;
        } finally {
            if (srccon1 != null) {
                srccon1.close();
            }
            if (trgcon1 != null) {
                trgcon1.close();
            }
        }

        return dataStore;
    }

//Methods to call the executeTestPlan method to fetch the Data --Adithya
    public void processTestPlan(List checkOptionData) {

        System.out.println("processTestPlan called");

//      DBConnectionManager dbConManager = new DBConnectionManager();
        Connection srccon = null;//dbConManager.getDBConFromFile(stmConData.get("*Source Host Name"));
        Connection trgcon = null;//dbConManager.getDBConFromFile(stmConData.get("*Target Host Name"));

        for (Object item : checkOptionData) {

            if (item.toString().equals("total_cnts")) {
                //Calling the Plan Execution

                totalCounts_tbl_view.getItems().clear();
                Task task = new Task<Void>() {
                    @Override
                    public Void call() throws ClassNotFoundException, SQLException, Exception {
                        //non ui code
                        System.out.println("Total Count Exec");
                        ObservableList totalResult = execueteTestPlan(total_cnt_testplan_data, item.toString(), total_cnt_testplan.get("Total_Cnt_Src_Testcase").toString(), total_cnt_testplan.get("Total_Cnt_Trg_Testcase").toString(), src_table, trg_table, srccon, trgcon, getSourceType(), getTargetType());
                        Platform.runLater(new Runnable() {
                            public void run() {
                                //ui code

                                progstatus_label.setText("Executing Count Test Plan");
                                progressLoadingImage();
                                totalCounts_tbl_view.setItems(totalResult);
                                CountTableRowHighlighter();
                            }
                        });
                        return null;
                    }
                };

                task.setOnRunning(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {

                        progstatus_label.setText("Executing Count Test Plan");
                        progressLoadingImage();

                    }
                });

                task.setOnSucceeded(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {
                        testResultSelect.setDisable(false);
                        progstatus_label.setText("Execution Completed");
                        progressCompletedImage();
                        resultSavebtn.setDisable(false);
                        resultClearbtn.setDisable(false);

                    }
                });

                task.setOnFailed(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {
                        testResultSelect.setDisable(false);
                        progstatus_label.setText("Execution Failed");
                        progressCompletedImage();
                        resultSavebtn.setDisable(false);
                        resultClearbtn.setDisable(false);
//                        new ExceptionUI(ex);
                        Throwable throwable = task.getException();
                        Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, throwable);
                        new ExceptionUI(new Exception(throwable.getMessage()));
                    }
                });

                Thread t = new Thread(task);
                t.setDaemon(true);
                t.start();

            }

            if (item.toString().equals("null_cnts")) {
                totalCounts_null_tbl_view.getItems().clear();

                Task task = new Task<Void>() {
                    @Override
                    public Void call() throws ClassNotFoundException, SQLException, Exception {
                        //non ui code

                        for (int j = 0; j < null_cnt_testplan.size() / 2; j++) {
                            //Calling the Plan Execution
                            if (!null_cnt_testplan.get("Null_Cnt_Src_Testcase_" + j).isEmpty()) {
                                System.out.println("Null Data :" + null_cnt_testplan.get("Null_Cnt_Src_Testcase_" + j));
                                execueteTestPlan(null_cnt_testplan_data, item.toString(), null_cnt_testplan.get("Null_Cnt_Src_Testcase_" + j), null_cnt_testplan.get("Null_Cnt_Trg_Testcase_" + j), src_table, trg_table, srccon, trgcon, getSourceType(), getTargetType());
                                //ObservableList nullCntRst = callNullExecutePlan(item.toString(), src_table, trg_table, srccon, trgcon, srcfile, trgfile);
                                Platform.runLater(new Runnable() {
                                    public void run() {
                                        //ui code
                                        BasicTestResulTableRowHighlighter(totalCounts_null_tbl_view);
                                        totalCounts_null_tbl_view.setItems(null_cnt_testplan_data);
                                        progstatus_label.setText("Executing Null Test Plan");
                                        progressLoadingImage();

                                    }
                                });

                            }

                        }
                        return null;
                    }
                };

                task.setOnRunning(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {

                        progstatus_label.setText("Executing Null Test Plan");
                        progressLoadingImage();

                    }
                });

                task.setOnSucceeded(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {
                        testResultSelect.setDisable(false);
                        progstatus_label.setText("Execution Completed");
                        progressCompletedImage();
                        resultSavebtn.setDisable(false);
                        resultClearbtn.setDisable(false);

                    }
                });

                task.setOnFailed(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent arg0) {
                        progstatus_label.setText("Failed with Exception");
                        progressCompletedImage();
                        Throwable throwable = task.getException();
                        Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, throwable);
                        new ExceptionUI(new Exception(throwable.getMessage()));
                    }
                });

                Thread t = new Thread(task);
                t.setDaemon(true);
                t.start();

            }

            if (item.toString().equals("not_null_cnts")) {
                totalCountsnot_null_tbl_view.getItems().clear();

                Task task = new Task<Void>() {
                    @Override
                    public Void call() throws ClassNotFoundException, SQLException, Exception {
                        //non ui code
                        System.out.println("Not null: ");

                        for (int j = 0; j < notnull_cnt_testplan.size() / 2; j++) {
                            //Calling the Plan Execution
                            if (!notnull_cnt_testplan.get("NotNull_Cnt_Src_Testcase_" + j).isEmpty()) {
                                execueteTestPlan(notnull_cnt_testplan_data, item.toString(), notnull_cnt_testplan.get("NotNull_Cnt_Src_Testcase_" + j), notnull_cnt_testplan.get("NotNull_Cnt_Trg_Testcase_" + j), src_table, trg_table, srccon, trgcon, getSourceType(), getTargetType());

                                //ObservableList rslt = callNotNullExecutePlan(item.toString(), src_table, trg_table, srccon, trgcon, getSourceType(), getTargetType());
                                Platform.runLater(new Runnable() {
                                    public void run() {
                                        //ui code
                                        BasicTestResulTableRowHighlighter(totalCountsnot_null_tbl_view);
                                        totalCountsnot_null_tbl_view.setItems(notnull_cnt_testplan_data);
                                        progstatus_label.setText("Executing Not Null Test Plan");
                                        progressLoadingImage();
                                    }
                                });

                            }
                        }

                        return null;
                    }
                };

                task.setOnRunning(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {

                        progstatus_label.setText("Executing Not Null Test Plan");
                        progressLoadingImage();
                        System.out.println("Progress : " + task.getProgress());

                    }
                });

                task.setOnSucceeded(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {
                        testResultSelect.setDisable(false);
                        progstatus_label.setText("Execution Completed");
                        progressCompletedImage();
                        resultSavebtn.setDisable(false);
                        resultClearbtn.setDisable(false);

                    }
                });
                task.setOnFailed(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent arg0) {
                        progstatus_label.setText("Failed with Exception");
                        progressCompletedImage();
                        Throwable throwable = task.getException();
                        Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, throwable);
                        new ExceptionUI(new Exception(throwable.getMessage()));
                    }
                });

                Thread t = new Thread(task);
                t.setDaemon(true);
                t.start();
            }

            if (item.toString()
                    .equals("dst_cnts")) {
                countDistinct_tbl_view.getItems().clear();

                Task task = new Task<Void>() {
                    @Override
                    public Void call() throws ClassNotFoundException, SQLException, Exception {
                        //non ui code
                        System.out.println("Distinct Test Plan: ");

                        for (int j = 0; j < dst_cnt_testplan.size() / 2; j++) {
                            //Calling the Plan Execution

                            if (!dst_cnt_testplan.get("Dst_Cnt_Src_Testcase_" + j).isEmpty()) {
                                execueteTestPlan(dst_cnt_testplan_data, item.toString(), dst_cnt_testplan.get("Dst_Cnt_Src_Testcase_" + j), dst_cnt_testplan.get("Dst_Cnt_Trg_Testcase_" + j), src_table, trg_table, srccon, trgcon, getSourceType(), getTargetType());

//                            ObservableList rslt = callDistinctExecutePlan(item.toString(), src_table, trg_table, srccon, trgcon, getSourceType(), getTargetType());
                                Platform.runLater(new Runnable() {
                                    public void run() {
                                        //ui code

                                        BasicTestResulTableRowHighlighter(countDistinct_tbl_view);
                                        countDistinct_tbl_view.setItems(dst_cnt_testplan_data);
                                        progstatus_label.setText("Executing Distinct Test Plan");
                                        progressLoadingImage();
                                    }
                                });
                            }
                        }
                        return null;
                    }
                };

                task.setOnRunning(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {

                        progstatus_label.setText("Executing Distinct Test Plan");
                        progressLoadingImage();

                    }
                });

                task.setOnSucceeded(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {
                        testResultSelect.setDisable(false);
                        progstatus_label.setText("Execution Completed");
                        progressCompletedImage();
                        resultSavebtn.setDisable(false);
                        resultClearbtn.setDisable(false);

                    }
                });

                task.setOnFailed(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent arg0) {
                        progstatus_label.setText("Failed with Exception");
                        progressCompletedImage();
                        Throwable throwable = task.getException();
                        Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, throwable);
                        new ExceptionUI(new Exception(throwable.getMessage()));
                    }
                });

                Thread t = new Thread(task);
                t.setDaemon(true);
                t.start();
            }
            if (item.toString()
                    .equals("dup_cnts")) {

                countDupli_tbl_view.getItems().clear();
                Task task = new Task<Void>() {
                    @Override
                    public Void call() throws ClassNotFoundException, SQLException, Exception {

                        System.out.println("Dup Test Plan");

                        for (int j = 0; j < dup_cnt_testplan.size() / 2; j++) {
                            //Calling the Plan Execution
                            if (!dup_cnt_testplan.get("Dup_Cnt_Src_Testcase_" + j).isEmpty()) {
                                execueteTestPlan(dup_cnt_testplan_data, item.toString(), dup_cnt_testplan.get("Dup_Cnt_Src_Testcase_" + j), dup_cnt_testplan.get("Dup_Cnt_Trg_Testcase_" + j), src_table, trg_table, srccon, trgcon, getSourceType(), getTargetType());

//                            ObservableList rslt = callDupExecutePlan(item.toString(), src_table, trg_table, srccon, trgcon, srcfile, trgfile);
                                Platform.runLater(new Runnable() {
                                    public void run() {
                                        //ui code

                                        BasicTestResulTableRowHighlighter(countDupli_tbl_view);
                                        countDupli_tbl_view.setItems(dup_cnt_testplan_data);
                                        progstatus_label.setText("Executing Duplicate Test Plan");
                                        progressLoadingImage();

                                    }
                                });
                            }
                        }
                        return null;
                    }
                };

                task.setOnRunning(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {

                        progstatus_label.setText("Executing Duplicate Test Plan");
                        progressLoadingImage();

                    }
                });

                task.setOnSucceeded(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {
                        testResultSelect.setDisable(false);
                        progstatus_label.setText("Execution Completed");
                        progressCompletedImage();
                        resultSavebtn.setDisable(false);
                        resultClearbtn.setDisable(false);

                    }
                });

                task.setOnFailed(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent arg0) {
                        progstatus_label.setText("Failed with Exception");
                        progressCompletedImage();
                        Throwable throwable = task.getException();
                        Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, throwable);
                        new ExceptionUI(new Exception(throwable.getMessage()));
                    }
                });

                Thread t = new Thread(task);
                t.setDaemon(true);
                t.start();

            }
            if (item.toString()
                    .equals("sum_num_cols")) {
                countNumerics_tbl_view.getItems().clear();
                Task task = new Task<Void>() {
                    @Override
                    public Void call() throws ClassNotFoundException, SQLException, Exception {
                        //non ui code
                        System.out.println("sum_num_testplan: " + sum_num_testplan.size() / 2);

                        for (int j = 0; j < sum_num_testplan.size() / 2; j++) {
                            //Calling the Plan Execution
                            if (!sum_num_testplan.get("Sum_Col_Src_Testcase_" + j).isEmpty()) {
                                execueteTestPlan(sum_num_testplan_data, item.toString(), sum_num_testplan.get("Sum_Col_Src_Testcase_" + j), sum_num_testplan.get("Sum_Col_Trg_Testcase_" + j), src_table, trg_table, srccon, trgcon, getSourceType(), getTargetType());

//                            ObservableList rslt = callSumCountExecutePlan(item.toString(), src_table, trg_table, srccon, trgcon, srcfile, trgfile);
                                Platform.runLater(new Runnable() {
                                    public void run() {
                                        //ui code

                                        BasicTestResulTableRowHighlighter(countNumerics_tbl_view);
                                        countNumerics_tbl_view.setItems(sum_num_testplan_data);
                                        progstatus_label.setText("Executing Sum Test Plan");
                                        progressLoadingImage();
                                    }
                                });
                            }
                        }
                        return null;
                    }
                };

                task.setOnRunning(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {

                        progstatus_label.setText("Executing Sum Test Plan");
                        progressLoadingImage();

                    }
                });

                task.setOnSucceeded(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {
                        testResultSelect.setDisable(false);
                        progstatus_label.setText("Execution Completed");
                        progressCompletedImage();
                        resultSavebtn.setDisable(false);
                        resultClearbtn.setDisable(false);

                    }
                });

                task.setOnFailed(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent arg0) {
                        progstatus_label.setText("Failed with Exception");
                        progressCompletedImage();
                        Throwable throwable = task.getException();
                        Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, throwable);
                        new ExceptionUI(new Exception(throwable.getMessage()));
                    }
                });

                Thread t = new Thread(task);
                t.setDaemon(true);
                t.start();
            }
            if (item.toString().equals("max_cols")) {

                max_tbl_view.getItems().clear();

                Task task = new Task<Void>() {
                    @Override
                    public Void call() throws ClassNotFoundException, SQLException, Exception {
                        System.out.println("Max Col: ");

                        for (int j = 0; j < max_col_testplan.size() / 2; j++) {
                            //Calling the Plan Execution
                            if (!max_col_testplan.get("Max_Col_Src_Testcase_" + j).isEmpty()) {
                                execueteTestPlan(max_col_testplan_data, item.toString(), max_col_testplan.get("Max_Col_Src_Testcase_" + j), max_col_testplan.get("Max_Col_Trg_Testcase_" + j), src_table, trg_table, srccon, trgcon, getSourceType(), getTargetType());

//                            ObservableList rslt = callMaxCountExecutePlan(item.toString(), src_table, trg_table, srccon, trgcon, srcfile, trgfile);
                                Platform.runLater(new Runnable() {
                                    public void run() {
                                        //ui code

                                        BasicTestResulTableRowHighlighter(max_tbl_view);

                                        max_tbl_view.setItems(max_col_testplan_data);
                                        progstatus_label.setText("Executing Max Test Plan");
                                        progressLoadingImage();
                                    }
                                });
                            }
                        }
                        return null;
                    }
                };

                task.setOnRunning(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {

                        progstatus_label.setText("Executing Max Test Plan");
                        progressLoadingImage();

                    }
                });

                task.setOnSucceeded(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {
                        testResultSelect.setDisable(false);
                        progstatus_label.setText("Execution Completed");
                        progressCompletedImage();
                        resultSavebtn.setDisable(false);
                        resultClearbtn.setDisable(false);

                    }
                });

                task.setOnFailed(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent arg0) {
                        progstatus_label.setText("Failed with Exception");
                        progressCompletedImage();
                        Throwable throwable = task.getException();
                        Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, throwable);
                        new ExceptionUI(new Exception(throwable.getMessage()));
                    }
                });

                Thread t = new Thread(task);
                t.setDaemon(true);
                t.start();
            }

            if (item.toString().equals("min_cols")) {

                min_tbl_view.getItems().clear();

                Task task = new Task<Void>() {
                    @Override
                    public Void call() throws ClassNotFoundException, SQLException, Exception {
                        //non ui code
                        System.out.println("Min Test Case: ");

                        for (int j = 0; j < min_col_testplan.size() / 2; j++) {
                            //Calling the Plan Execution
                            if (!min_col_testplan.get("Min_Col_Src_Testcase_" + j).isEmpty()) {
                                execueteTestPlan(min_col_testplan_data, item.toString(), min_col_testplan.get("Min_Col_Src_Testcase_" + j), min_col_testplan.get("Min_Col_Trg_Testcase_" + j), src_table, trg_table, srccon, trgcon, getSourceType(), getTargetType());

//                            ObservableList rslt = callMinCountExecutePlan(item.toString(), src_table, trg_table, srccon, trgcon, srcfile, trgfile);
                                Platform.runLater(new Runnable() {
                                    public void run() {
                                        //ui code

                                        BasicTestResulTableRowHighlighter(min_tbl_view);

                                        min_tbl_view.setItems(min_col_testplan_data);

                                        progstatus_label.setText("Executing Min Test Plan");
                                        progressLoadingImage();
                                    }
                                });
                            }
                        }
                        return null;
                    }
                };

                task.setOnRunning(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {

                        progstatus_label.setText("Executing Min Test Plan");
                        progressLoadingImage();

                    }
                });

                task.setOnSucceeded(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {
                        testResultSelect.setDisable(false);
                        progstatus_label.setText("Execution Completed");
                        progressCompletedImage();
                        resultSavebtn.setDisable(false);
                        resultSavebtn.setDisable(false);

                    }
                });

                task.setOnFailed(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent arg0) {
                        progstatus_label.setText("Failed with Exception");
                        progressCompletedImage();
                        Throwable throwable = task.getException();
                        Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, throwable);
                        new ExceptionUI(new Exception(throwable.getMessage()));
                    }
                });

                Thread t = new Thread(task);
                t.setDaemon(true);
                t.start();
            }
            /*
             * Fetch the Complete records
             */
            if (item.toString().equals("tot_dat_val") || item.toString().equals("incr_dat_val")) {
                Task task = new Task<Void>() {
                    @Override
                    public Void call() throws ClassNotFoundException, SQLException, Exception {
                        //non ui code
                        //Calling the Plan Execution - sourceData_tbl_view not useful and need to pass a param
                        execueteTestPlan(cmpl_data_tesplan_data, item.toString(), cmpl_data_testplan.get("Compl_Data_Src_Testcase"), cmpl_data_testplan.get("Compl_Data_Trg_Testcase"), src_table, trg_table, srccon, trgcon, getSourceType(), getTargetType());
//                         execueteTestPlan(cmpl_data_tesplan_data, item.toString(), "select 8 from dual", cmpl_data_testplan.get("Compl_Data_Trg_Testcase"), src_table, trg_table, srccon, trgcon, getSourceType(), getTargetType());
                        long start = new Date().getTime();
//                        ArrayList srcremovedList = (ArrayList) CollectionUtils.removeAll(srcCmplResult, trgCmplResult);
//                        ArrayList trgremovedList = (ArrayList) CollectionUtils.removeAll(trgCmplResult, srcCmplResult);
                        long end = new Date().getTime();
                        System.out.println("Time Takend to process: " + (end - start) / 1000);
                        unMatched = compareSTMValidation(srcCmplResult, trgCmplResult);
//                        unMatched = FXCollections.observableArrayList();
//                        unMatched.add((ArrayList) CollectionUtils.removeAll(srcCmplResult, trgCmplResult));
//                        unMatched.add((ArrayList) CollectionUtils.removeAll(trgCmplResult, srcCmplResult));
                        Platform.runLater(new Runnable() {
                            public void run() {
//                                    //ui code
                                sourceData_tbl_view.getItems().clear();
                                sourceData_tbl_view.getColumns().clear();
                                targetData_tbl_view.getItems().clear();
                                targetData_tbl_view.getColumns().clear();
                                setColumnsTableView(sourceData_tbl_view, Arrays.asList(getSourceColumnData()));
                                setColumnsTableView(targetData_tbl_view, Arrays.asList(getTargetColumnData()));

                                sourceData_tbl_view.setItems(srcCmplResult);
                                targetData_tbl_view.setItems(trgCmplResult);

                                unMatchsourceData_tbl_view.getItems().clear();
                                unMatchtargetData_tbl_view.getItems().clear();
                                unMatchsourceData_tbl_view.getColumns().clear();
                                unMatchtargetData_tbl_view.getColumns().clear();

                                setColumnsTableView(unMatchsourceData_tbl_view, Arrays.asList(getSourceColumnData()));
                                setColumnsTableView(unMatchtargetData_tbl_view, Arrays.asList(getTargetColumnData()));
//unMatchsourceData_tbl_view.setItems(FXCollections.observableArrayList(srcremovedList));
//unMatchtargetData_tbl_view.setItems(FXCollections.observableArrayList(trgremovedList));

//
//                                ObservableList src = FXCollections.observableArrayList(unMatched.get(0));
//                                ObservableList trg = FXCollections.observableArrayList(unMatched.get(1));
                                unMatchsourceData_tbl_view.setItems(FXCollections.observableList((List) unMatched.get(0)));
                                unMatchtargetData_tbl_view.setItems(FXCollections.observableList((List) unMatched.get(1)));
//                                unMatchsourceData_tbl_view.setItems(FXCollections.observableList((List) unMatched.get(0)));
//                                unMatchtargetData_tbl_view.setItems(FXCollections.observableList((List) unMatched.get(1)));

                            }
                        });
                        return null;
                    }
                };

                task.setOnRunning(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {

                        progstatus_label.setText("Executing Complete Test Plan");
                        progressLoadingImage();

                    }
                });

                task.setOnSucceeded(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent event) {
//                        if (unMatchsourceData_tbl_view.getItems().size() == 0 && unMatchtargetData_tbl_view.getItems().size() == 0) {
////                            dataValid_status_lbl.setText("Matched");
////                            dataValid_status_lbl.setStyle("-fx-background-color: green");
//                        } else {
//                            dataValid_status_lbl.setText("Unmatched");
//                            dataValid_status_lbl.setStyle("-fx-background-color: red");
//                        }

                        ObservableList dataVdlSmrydtls = FXCollections.observableArrayList();
//                        String d1 = "Total Count,"+srcCmplResult.size()+","+trgCmplResult.size()+"false";

                        DataVdlSummry dataVdlSummry = new DataVdlSummry();

                        dataVdlSummry.valditionType.set("Total Count");
                        dataVdlSummry.srcCnt.setValue(Integer.toString(srcCmplResult.size()));
                        dataVdlSummry.trgCnt.setValue(Integer.toString(trgCmplResult.size()));
                        dataVdlSummry.result.setValue((srcCmplResult.size() == trgCmplResult.size()) ? "Passed" : "Failed");

                        dataVdlSmrydtls.add(dataVdlSummry);

                        DataVdlSummry dataVdlSummry1 = new DataVdlSummry();
                        dataVdlSummry1.valditionType.set("UnMatched Count");
                        dataVdlSummry1.srcCnt.setValue(Integer.toString(unMatchsourceData_tbl_view.getItems().size()));
                        dataVdlSummry1.trgCnt.setValue(Integer.toString(unMatchsourceData_tbl_view.getItems().size()));

                        dataVdlSummry1.result.setValue((unMatchsourceData_tbl_view.getItems().size() == unMatchsourceData_tbl_view.getItems().size()) ? "Passed" : "Failed");
                        dataVdlSmrydtls.add(dataVdlSummry1);

                        dataVdlsmry.setItems(dataVdlSmrydtls);

                        DataValidationHighLighter(dataVdlsmry);
                        progstatus_label.setText("Execution Completed");
                        progressCompletedImage();
                        resultSavebtn.setDisable(false);
                        resultClearbtn.setDisable(false);
                    }
                });

                task.setOnFailed(new EventHandler<WorkerStateEvent>() {
                    @Override
                    public void handle(WorkerStateEvent arg0) {
                        progstatus_label.setText("Failed with Exception");
                        progressCompletedImage();
                        Throwable throwable = task.getException();
                        Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, throwable);
                        System.out.println("Issue: " + cmpl_data_testplan.get("Compl_Data_Src_Testcase") + "  " + cmpl_data_testplan.get("Compl_Data_Trg_Testcase"));
                        new ExceptionUI(new Exception(throwable.getMessage()));

                    }
                });
                Thread t = new Thread(task);
                t.setDaemon(true);
                t.start();

            }

        }

    }

    private void incRulegenerator() {
        if (stmConData.get("*Load Strategy").toLowerCase().contains("incremental")) {
            incrementalCondition = stmData.getIncrementalCondition(this.getDbTypeName("src"), this.getDbTypeName("trg"));
            System.out.println("Data: " + incrementalCondition.get(0) + " :: " + incrementalCondition.get(1));
        } else {
            incrementalCondition = new ArrayList<>();
        }

    }

    /*
     * Method to Upload the STM File --Adithya
     */
    @FXML
    public void etlStmFileUploadBtn() {

        System.out.println("etlStmFileUploadBtn called");

        //now focus to automated tab
//        SingleSelectionModel<Tab> selectionModel = tbpanemanautotest.getSelectionModel();
//        selectionModel.select(tabautotesting);
        //show file dailog
        Stage mainstage = (Stage) mainvbox.getScene().getWindow();
        stmFile = fileChooser.showOpenDialog(mainstage);

        //Checking the File input
        System.out.println("File Path: " + stmFile);

        //File Validation
        if (stmFile != null) {
            try {
                stmData = new STMExcellDataProcess(stmFile.toString());

                stmConData = stmData.getConnectionData();
                stmComSplRule = stmData.getComSpecialRule();
                stmTransData = stmData.getSTMTranData();
                stmSrcTranData = stmData.getSrcTranRuleqry();
//                if (stmConData.get("Load Strategy").toLowerCase().contains("incremental")) {
//                    incrementalCondition = stmData.getIncrementalCondition(this.getDbTypeName("src"), this.getDbTypeName("trg"));
//                    System.out.println("Data: " + incrementalCondition.get(0) + " :: " + incrementalCondition.get(1));
//                } else {
//                    incrementalCondition = new ArrayList<>();
//                }
//                System.out.println("Incremental Condition Size: " + incrementalCondition.size());
                stmData.getExecptionData(); //To get Execption

                checkHostName(); //Checking the Host Name for STM 
                setSTMBasicsUI(); //Setting the Basics STM Details

                if (tabautotesting.isSelected()) {
                    /*
                     * Setting the Connection Data to the FX Fields --Adithya
                     * 14-05-2017
                     */

                    if (!stmTransData.isEmpty() && !stmConData.isEmpty()) {

                        transDataTbl.setItems(stmTransData); //Populating the STM Data
                        incRulegenerator();
                        autoresultrunbt.setDisable(false);

                        setTestScenarios(false);

                    }
                } else {
                    logger.info("STM data");
                    logger.info(stmTransData.toString());
                    transDataLoadTbl.setItems(stmTransData);
                    commonRuletxtarLoad.setText(stmComSplRule.get("common_rule").toString().trim());
                    sourceKeyColtxtarLoad.setText(stmComSplRule.get("source_key").toString().trim());
                    targetKeyColtxtarLoad.setText(stmComSplRule.get("target_key").toString().trim());
                    stm_conTitle_txt_fieldLoad.setText(stmConData.get("*Title").toString());
                    stm_conAut_txt_fieldLoad.setText(stmConData.get("*Author").toString());
                    stm_conVer_txt_fieldLoad.setText(stmConData.get("Version & History").toString());
                    autoLoadbtn.setDisable(false);
                    incRulegenerator();
                }
            } catch (Exception ex) {
                Logger.getLogger(MainStageController.class
                        .getName()).log(Level.SEVERE, null, ex);
                new ExceptionUI(ex);
            }

        }
    }

    /*
     * Logic for Dynamic Test scenarios
     */
    public void setTestScenarios(boolean value) {
        chkcmpltdata.setDisable(value);
        chkdstcnts.setDisable(value);
        chkdupcnts.setDisable(value);
        chkfst1000.setDisable(value);
        chklst1000.setDisable(value);
        chkincrdata.setDisable(value);
        chkmax.setDisable(value);
        chkmin.setDisable(value);
        chkncnts.setDisable(value);
        chknncnts.setDisable(value);
        chkschtest.setDisable(value);
        chktcnts.setDisable(value);
        chksumnum.setDisable(value);
    }

    /*
     * Method to set Columns Dynamically to the Multiple Table View --Adithya
     * 30-04-2017
     */
    public void setColumnsTableView(TableView tableView, List tableColumns) {
        System.out.println("setTableView method Called");

        for (Object colname : tableColumns) {

            tabPane_tbl_columns = new TableColumn(colname.toString());

            final int j = tableColumns.indexOf(colname);

            tableView.getColumns().add(tabPane_tbl_columns);

            tabPane_tbl_columns.setCellValueFactory(new Callback<TableColumn.CellDataFeatures<ObservableList, String>, ObservableValue<String>>() {
                public ObservableValue<String> call(TableColumn.CellDataFeatures<ObservableList, String> param) {
                    final int j = tableColumns.indexOf(colname);
                    String val = param.getValue().get(j) == null ? "null" : param.getValue().get(j).toString();
                    return new SimpleStringProperty(val);
                }
            });

        }
    }

    //table clear function
    @FXML
    public void stmTblClear() {

        System.out.println("stmTblClear called");
        transDataTbl.getItems().clear();
        tfsrcconname.clear();
        tftrgconname.clear();
        stm_conTitle_txt_field.clear();
        stm_conAut_txt_field.clear();
        stm_conVer_txt_field.clear();

        commonRuletxtar.clear();
        sourceKeyColtxtar.clear();
        targetKeyColtxtar.clear();
        autoresultrunbt.setDisable(true);
        resultSavebtn.setDisable(true);
        resultClearbtn.setDisable(true);

        sourceData_tbl_view.getItems().clear();
        targetData_tbl_view.getItems().clear();
        unMatchsourceData_tbl_view.getItems().clear();
        unMatchtargetData_tbl_view.getItems().clear();
        progstatus_label.setText("Let's Get Started");
        progressCompletedImage();
//        dataValid_status_lbl.setText("Nothing to show");
        testResultSelect.setDisable(true);

    }

    private void setConnName(TextField connPoint, String connName, String dbName, String tableName, String connType) throws FileNotFoundException, IOException {
        System.out.println("Textfiled: " + connPoint);
        System.out.println("connName: " + connName);
        System.out.println("dbName: " + dbName);
        System.out.println("tableName: " + tableName);
        System.out.println("connType: " + connType);
        if (connType.equalsIgnoreCase("db")) {
            //Processing DB Connection Name
            if (dbName.isEmpty() || tableName.isEmpty()) {
                List<String> connData = readFF("conn/" + connName + ".con");

                connPoint.setText(connName + "::" + connData.get(1).substring(connData.get(1).lastIndexOf("/") + 1, connData.get(1).length()) + "::#MULTIPLETABLES");
            } else {

                connPoint.setText(connName + "::" + dbName + "::" + tableName);
            }

        } else {
            //Processing File Connection name

            List connData = readFF("files/" + connType.toLowerCase() + "/" + connName);

            if (connData.get(0).toString().equalsIgnoreCase("local")) {
//                File fileName = new File(connData.get(1).toString().trim());
//                System.out.println("File Name: " + fileName.getParent());
//                connPoint.setText("FlatFile::local::" + fileName.getParent().replace("\\", "/") + "::" + fileName.getName());
                connPoint.setText("FlatFile::local::" + connData.get(1).toString());
            } else {
                String connectType = connData.get(1).toString();
                String hostName = connData.get(2).toString();

//                File fileName = new File(connData.get(3).toString().trim());
//                System.out.println(fileName.getParent());
//                String fileSchema = "";
//                if (fileName.getParent() != null) {
//                    fileSchema = fileName.getParent().replace("\\", "/");
//                } else {
//                    fileSchema = "/";
//                }
//                connPoint.setText("FlatFile::" + connectType + "@" + hostName + "::" +fileSchema+ "::" + fileName.getName());
                connPoint.setText("FlatFile::" + connectType + "@" + hostName + "::" + connData.get(3).toString());
            }

        }
    }

    //Setting the Basics STM details to the UI
    public void setSTMBasicsUI() throws FileNotFoundException, IOException {

        //Populating Target Connection
//        if (stmConData.get("Target File Type (csv, txt etc)").toString().equalsIgnoreCase("db")) {
//
//            setConnName(tftrgconname, stmConData.get("*Target Host Name").toString().trim(), stmConData.get("*Target DB/File Name").toString().trim(), stmConData.get("Target File Location").toString().trim(), stmConData.get("Target File Type (csv, txt etc)").toString());
//        } else {
//            setConnName(tftrgconname, stmConData.get("*Target Host Name").toString().trim(), stmConData.get("*Target DB/File Name").toString().trim(), stmConData.get("Target File Location").toString().trim(), stmConData.get("Target File Type (csv, txt etc)").toString());
//        }
        setConnName(tftrgconname, stmConData.get("*Target Host Name").toString().trim(), stmConData.get("*Target DB/File Name").toString().trim(), stmConData.get("Target File Location").toString().trim(), stmConData.get("*Target File Type (csv, txt etc)").toString());
        setConnName(tfsrcconname, stmConData.get("*Source Host Name").toString().trim(), stmConData.get("*Source DB/File Name").toString().trim(), stmConData.get("Source File Location").toString().trim(), stmConData.get("*Source File Type (csv, txt etc)").toString());
        //Populating Source Connection
//        if (stmConData.get("Source File Type (csv, txt etc)").toString().equalsIgnoreCase("db")) {
//            
//            tfsrcconname.setText(stmConData.get("*Source Host Name").toString().trim() + "::" + stmConData.get("*Source DB/File Name").toString().trim() + "::" + stmConData.get("Source File Location").toString().trim());
//
//        } else {
//            
//            this.srcfile = stmConData.get("Source File Location").toString().trim() + "/" + stmConData.get("*Source DB/File Name").toString().trim();
//            tfsrcconname.setText("FlatFile::" + stmConData.get("Source File Location").toString().trim() + "::" + stmConData.get("*Source DB/File Name").toString().trim());
//
//        }

        commonRuletxtar.setText(stmComSplRule.get("common_rule").toString().trim());
        sourceKeyColtxtar.setText(stmComSplRule.get("source_key").toString().trim());
        targetKeyColtxtar.setText(stmComSplRule.get("target_key").toString().trim());
        stm_conTitle_txt_field.setText(stmConData.get("*Title").toString());
        stm_conAut_txt_field.setText(stmConData.get("*Author").toString());
        stm_conVer_txt_field.setText(stmConData.get("Version & History").toString());
    }

    //Checking the Connection Name for the Database/File from the STM
    private void checkHostName() throws Exception {
        int i = 0;
        System.out.println("checkHostName Called");
        if (stmConData.get("*Target File Type (csv, txt etc)").toString().equalsIgnoreCase("db"))//&& stmConData.get("Source File Type (csv, txt etc)").toString().equalsIgnoreCase("db")) {
        {
            File f = new File("conn/" + stmConData.get("*Target Host Name").toString() + ".con");
            if (!f.exists() && !f.isFile()) {
                exceptionValue.append((++i) + ". " + stmConData.get("*Target Host Name").toString() + " DB Connection not found\n");
            }
        }

        if (stmConData.get("*Source File Type (csv, txt etc)").toString().equalsIgnoreCase("db")) {
            File f = new File("conn/" + stmConData.get("*Source Host Name").toString() + ".con");
            if (!f.exists() && !f.isFile()) {
                exceptionValue.append((++i) + ". " + stmConData.get("*Source Host Name").toString() + " DB Connection not found\n");
            }
        }

        if (!stmConData.get("*Target File Type (csv, txt etc)").toString().equalsIgnoreCase("db"))//&& stmConData.get("Source File Type (csv, txt etc)").toString().equalsIgnoreCase("db")) {
        {
            File f = new File("files/" + stmConData.get("*Target File Type (csv, txt etc)").toString().toLowerCase() + "/" + stmConData.get("*Target Host Name").toString());
            if (!f.exists() && !f.isFile()) {
                System.out.println("File Path: " + f.toString());
                exceptionValue.append((++i) + ". " + stmConData.get("*Target Host Name").toString() + " File not found\n");

            }
        }

        if (!stmConData.get("*Source File Type (csv, txt etc)").toString().equalsIgnoreCase("db")) {
            File f = new File("files/" + stmConData.get("*Source File Type (csv, txt etc)").toString().toLowerCase() + "/" + stmConData.get("*Source Host Name").toString());
            if (!f.exists() && !f.isFile()) {
                exceptionValue.append((++i) + ". " + stmConData.get("*Source Host Name").toString() + " File not found\n");

            }
        }

        if (!exceptionValue.toString().isEmpty()) {
            String exceptionData = exceptionValue.toString();
            exceptionValue.delete(0, exceptionValue.length());
            if (exceptionValue.toString().split("\n").length == 1) {
                throw new Exception(exceptionData.toString());
            } else {
                throw new Exception("Invalid Connection Data in the STM \n" + exceptionData.toString());
            }
        }

    }

    //Getting the Target columns from the STM
    public String[] getTargetColumnData() {

        System.out.println("getTargetColumnData called");

        ObservableList<STMBean> sblist = transDataTbl.getItems();
        String[] trgColData = new String[sblist.size()];
        int i = 0;
        for (STMBean sb : sblist) {
            trgColData[i++] = sb.getTargetColumnName();
        }

        return trgColData;
    }

    public String[] getSourceColumnData() {

        System.out.println("getTargetColumnData called");

        ObservableList<STMBean> sblist = transDataTbl.getItems();
        String[] srcColData = new String[sblist.size()];
        int i = 0;
        for (STMBean sb : sblist) {
            srcColData[i++] = sb.getTargetColumnName() + "_src";
        }

        return srcColData;
    }

    //Getting the Source Transformation from the STM
    public String[] getSrcTransRuleData(TableView tableView) {
        System.out.println("getSrcTransRuleData called");
        ObservableList<STMBean> sblist = tableView.getItems();

        String[] srcTransData = new String[sblist.size()];
        int i = 0;
        for (STMBean sb : sblist) {
//            System.out.println("Src Transformation: " + sb.getColumnTransRule());
            if (!sb.getColumnTransRule().equalsIgnoreCase("#INPROGRESS")) {
//                System.out.println("Data"+sb.getColumnTransRule() + " \n " + commonRuletxtar.getText());

                if (sb.getColumnTransRule().contains("where")) {
                    srcTransData[i++] = sb.getColumnTransRule();//+ " \n " + commonRuletxtar.getText().replace("where", "and");
                } else {
                    srcTransData[i++] = sb.getColumnTransRule();//+ " \n " + commonRuletxtar.getText();
                }

            } else {
                srcTransData[i++] = sb.getColumnTransRule();

            }

        }

        return srcTransData;
    }

    //Method to Check only One Check box in Advance tab
    private void configureCheckBox(CheckBox checkBox) {
        logger.info("Data Validation Scenario Checkbox Handling");
        if (checkBox.isSelected()) {
            selectedCheckBoxes.add(checkBox);
        } else {
            unselectedCheckBoxes.add(checkBox);
        }

        checkBox.selectedProperty().addListener((obs, wasSelected, isNowSelected) -> {
            if (isNowSelected) {
                unselectedCheckBoxes.remove(checkBox);
                selectedCheckBoxes.add(checkBox);
            } else {
                selectedCheckBoxes.remove(checkBox);
                unselectedCheckBoxes.add(checkBox);
            }

        });

    }

    //Method to Highlight the row based on the result for Total Counts    
    public void CountTableRowHighlighter() {
        totalCounts_tbl_view.setRowFactory(row -> new TableRow<TotalCountBean>() {
            @Override
            public void updateItem(TotalCountBean item, boolean empty) {
                super.updateItem(item, empty);

                if (item == null || empty) {

                    setStyle("");
                } else {
                    //Now 'item' has all the info of the Person in this row
//                    !item.totCnt.getValue()
                    if (item.totCnt.getValue().equalsIgnoreCase("Failed")) {
                        //We apply now the changes in all the cells of the row
                        //setStyle("-fx-background-color: #ff9999");
                        setStyle("-fx-background-color: red");

                    }

                }
            }
        });

    }

    //Method to Highlight the row based on the result for Basic Testing scenarios execpt Total Counts    
    public void BasicTestResulTableRowHighlighter(TableView tblview) {
        tblview.setRowFactory(row -> new TableRow<CountsMaxMinBean>() {
            @Override
            public void updateItem(CountsMaxMinBean item, boolean empty) {
                super.updateItem(item, empty);

                if (item == null || empty) {
                    setStyle("");
                } else {

                    //Now 'item' has all the info of the Person in this row
                    if (item.result.getValue().equalsIgnoreCase("Failed")) {
                        //We apply now the changes in all the cells of the row
                        //setStyle("-fx-background-color: #ff9999");
                        setStyle("-fx-background-color: red");

                    } else {
                        setStyle("");
                    }
                }
            }
        });

    }

    public void DataValidationHighLighter(TableView tblview) {
        tblview.setRowFactory(row -> new TableRow<DataVdlSummry>() {
            @Override
            public void updateItem(DataVdlSummry item, boolean empty) {
                super.updateItem(item, empty);

                if (item == null || empty) {
                    setStyle("");
                } else {

                    //Now 'item' has all the info of the Person in this row
                    if (item.result.getValue().equalsIgnoreCase("Failed")) {
                        //We apply now the changes in all the cells of the row
                        //setStyle("-fx-background-color: #ff9999");
                        setStyle("-fx-background-color: red");

                    } else {
                        setStyle("");
                    }
                }
            }
        });

    }

    //Method to Highlight the row based on the result for Summary Testing scenarios execpt Total Counts    
    public void summaryTestResulTableRowHighlighter(TableView tblview) {
        tblview.setRowFactory(row -> new TableRow<ManualSummaryBean>() {
            @Override
            public void updateItem(ManualSummaryBean item, boolean empty) {
                super.updateItem(item, empty);

                if (item == null || empty) {
                    setStyle("");
                } else {

                    //Now 'item' has all the info of the Person in this row
                    if (item.result.getValue().equalsIgnoreCase("Failed") || item.result.getValue().contains("Failed")) {
                        //We apply now the changes in all the cells of the row
                        //setStyle("-fx-background-color: #ff9999");
                        setStyle("-fx-background-color: red");

                    } else {
                        setStyle("");
                    }
                }
            }
        });

    }

    @FXML
    public void clearResultButton(ActionEvent ae) {
        logger.info("Clearing Data verification Tables");
        Map<String, TableView> resultTableList1 = getSelectedResultTableList(this.getSelectedTestCases());
        System.out.println("Clear Option: " + resultTableList1);
        for (Map.Entry<String, TableView> entry : resultTableList1.entrySet()) {
            String key = entry.getKey();
            TableView value = entry.getValue();

            value.getItems().clear();

        }

        resultSavebtn.setDisable(true);
        resultClearbtn.setDisable(true);

        progstatus_label.setText("Let's Get Started");
        dataVdlsmry.getItems().clear();
        progressCompletedImage();
//        dataValid_status_lbl.setText("Nothing to show");

        logger.info("Cleared Data verification Tables");
    }

    //Storing the tableview into List
    public Map<String, TableView> getSelectedResultTableList(List checkOptions) {
        logger.info("Selecting the Test Scenarios");
        resultTableList.clear();
        for (Object checkOption : checkOptions) {
            if (checkOption.equals("total_cnts")) {
                resultTableList.put("total_cnts", totalCounts_tbl_view);
            }

            if (checkOption.toString().equals("null_cnts")) {
                resultTableList.put("null_cnts", totalCounts_null_tbl_view);
            }

            if (checkOption.toString().equals("not_null_cnts")) {
                resultTableList.put("not_null_cnts", totalCountsnot_null_tbl_view);
            }

            if (checkOption.toString()
                    .equals("dst_cnts")) {
                resultTableList.put("dst_cnts", countDistinct_tbl_view);

            }

            if (checkOption.toString()
                    .equals("dup_cnts")) {
                resultTableList.put("dup_cnts", countDupli_tbl_view);
            }

            if (checkOption.toString()
                    .equals("max_cols")) {
                resultTableList.put("max_cols", max_tbl_view);
            }

            if (checkOption.toString()
                    .equals("min_cols")) {
                resultTableList.put("min_cols", min_tbl_view);
            }

            if (checkOption.toString()
                    .equals("sum_num_cols")) {
                resultTableList.put("sum_num_cols", countNumerics_tbl_view);
            }

            if (checkOption.toString()
                    .equals("tot_dat_val")) {
                resultTableList.put("cmpl_data_src", sourceData_tbl_view);
                resultTableList.put("cmpl_data_trg", targetData_tbl_view);
                resultTableList.put("cmpl_un_data_src", unMatchsourceData_tbl_view);
                resultTableList.put("cmpl_un_data_trg", unMatchtargetData_tbl_view);
                resultTableList.put("cmpl_vdl_summary", dataVdlsmry);
            }

        }
        logger.info("Selected the Test Scenarios");
        return resultTableList;
    }

    //Storing the tableview into List
    public Map<String, ObservableList> getSelectedResultTableData(List checkOptions) {
        resultTableListData.clear();
        logger.info("Loading Test Scenarios to data saving or for other data");
        for (Object checkOption : checkOptions) {
            if (checkOption.equals("total_cnts")) {

                resultTableListData.put("total_cnts", total_cnt_testplan_data);
            }

            if (checkOption.toString().equals("null_cnts")) {

                resultTableListData.put("null_cnts", null_cnt_testplan_data);
            }

            if (checkOption.toString().equals("not_null_cnts")) {

                resultTableListData.put("not_null_cnts", notnull_cnt_testplan_data);
            }

            if (checkOption.toString()
                    .equals("dst_cnts")) {

                resultTableListData.put("dst_cnts", dst_cnt_testplan_data);

            }

            if (checkOption.toString()
                    .equals("dup_cnts")) {

                resultTableListData.put("dup_cnts", dup_cnt_testplan_data);
            }

            if (checkOption.toString()
                    .equals("max_cols")) {

                resultTableListData.put("max_cols", max_col_testplan_data);
            }

            if (checkOption.toString()
                    .equals("min_cols")) {

                resultTableListData.put("min_cols", min_col_testplan_data);
            }

            if (checkOption.toString()
                    .equals("sum_num_cols")) {

                resultTableListData.put("sum_num_cols", sum_num_testplan_data);
            }

        }
        logger.info("Loaded Test Scenarios to data saving or for other data");
        return resultTableListData;
    }

    String resultsExport = "";

    @FXML
    public void saveResultBtn(ActionEvent ae) {
        Stage mainstage = (Stage) mainvbox.getScene().getWindow();
        DirectoryChooser directoryChooser = new DirectoryChooser();
        FileChooser fc = new FileChooser();
        File dirName = null;

        if (tabmantesting.isSelected()) {
            if (stmFile != null) {
                String file = stmFile.getName();
                String fileName = file.substring(0, file.lastIndexOf(".") + 1) + "_results.xls";
                dirName = directoryChooser.showDialog(mainstage);
                if (dirName != null) {
                    resultsExport = dirName.getAbsolutePath() + "/" + fileName;
                }

            } else {
                dirName = fc.showOpenDialog(mainstage);
                if (dirName != null) {
                    resultsExport = dirName.getAbsolutePath();
                }
            }
        } else {
            String file = stmFile.getName();
            String fileName = file.substring(0, file.lastIndexOf(".") + 1) + "_results.xls";
            dirName = directoryChooser.showDialog(mainstage);
            if (dirName != null) {
                resultsExport = dirName.getAbsolutePath() + "/" + fileName;
            }
        }

        if (!resultsExport.isEmpty()) {
            saveTestResults(getSelectedTestCases());
        }

    }

    public void saveTestResults(List checkOptions) {
        logger.info("Saving Test data to excel");
        Map<String, TableView> resultTableList = getSelectedResultTableList(checkOptions);
        XSSFWorkbook wbook = new XSSFWorkbook();
        XSSFSheet shSheet;
        XSSFSheet tmpSheet;
        FileOutputStream xlsFile = null;
        if (stmFile != null) //create excel sheet
        {
            try {

                xlsFile = new FileOutputStream(resultsExport);

                int mapIndex = 0;
                System.out.println("Result Set: " + resultTableList);
                for (Map.Entry<String, TableView> entry : resultTableList.entrySet()) {
                    String key = entry.getKey();
                    TableView value = entry.getValue();

                    int k = 0, j = 0;

                    shSheet = wbook.createSheet(key.toUpperCase());

                    System.out.println("Key: " + key);
                    System.out.println("srccolnames" + value.getColumns());

                    String[] colNames = {"Source TestCase", "Source Value", "Target TestCase", "Target Value", "Result"};
                    int rowNum = 0;
                    if (!key.contains("data")) {
                        Row row = shSheet.createRow(rowNum++);
                        System.out.println("Col Lenght: " + colNames.length);
                        for (int l = 0; l < colNames.length; l++) {
//                            System.out.println("colName: " + colNames[l]);
//                                jxl.write.Label labTemp = new jxl.write.Label(l, 0, colNames[l]);
                            org.apache.poi.ss.usermodel.Cell cell = row.createCell(l);

                            cell.setCellValue(colNames[l]);

                        }
                        for (int m = 0; m < value.getItems().size(); m++) {

                            row = shSheet.createRow(rowNum++);
                            String itemData = value.getItems().get(m).toString();

//                            System.out.println(value.getItems().get(m).toString());
                            String[] itemDataList = itemData.split(",");
                            for (int l = 0; l < itemDataList.length; l++) {
                                String data = itemDataList[l];
//                                String  data = itemData[l].toString();
                                org.apache.poi.ss.usermodel.Cell cell = row.createCell(l);
                                cell.setCellValue(data.toString());
                            }

                        }

                    } else {

                        //For Complete Data
                        List cmpltColNames;
                        System.out.println("Key Data: " + key);
                        XSSFFont cellFont = wbook.createFont();
                        cellFont.setBold(true);
                        cellFont.setFamily(FontFamily.ROMAN);
                        cellFont.setFontHeight(14);
                        Row row = shSheet.createRow(rowNum++);
                        if (key.contains("src")) {
                            cmpltColNames = src_table;
                        } else {
                            cmpltColNames = trg_table;
                        }

                        ObservableList<TableColumn> cmplColList = value.getColumns();

                        for (int l = 0; l < cmplColList.size(); l++) {

//                            System.out.println("colName: " + cmplColList.get(l).getText());
//                                jxl.write.Label labTemp = new jxl.write.Label(l, 0, colNames[l]);
                            org.apache.poi.ss.usermodel.Cell cell = row.createCell(l);

                            cell.setCellValue(cmplColList.get(l).getText());

                        }
                        for (int m = 0; m < value.getItems().size(); m++) {

                            Row row1 = shSheet.createRow(rowNum++);
                            List itemData = (List) value.getItems().get(m);
//                            System.out.println(value.getItems().get(m).toString());

//                            String[] itemDataList = itemData.split(",");
                            for (int l = 0; l < itemData.size(); l++) {
                                String data = "";
                                if (itemData.get(l) != null) {
                                    data = itemData.get(l).toString();
                                }

                                org.apache.poi.ss.usermodel.Cell cell = row1.createCell(l);
                                cell.setCellValue(data);
//                          
                            }

                        }

                    }

                }

                wbook.write(xlsFile);
                wbook.close();
                logger.info("Saved Test data to excel: " + xlsFile);
                new AlertUI("Results Saved Successfully");

            } catch (IOException ex) {
                Logger.getLogger(MainStageController.class
                        .getName()).log(Level.SEVERE, null, ex);
                logger.error(ex.toString());
                new ExceptionUI(ex);

            }

        }

    }

    @FXML
    private void testcaseRunButtonAction(ActionEvent event) {

        System.out.println("Clicked - RunButtonAction");
        automatedTestCase();
        manualviewsummarytable.getItems().clear();
        manualsrctabview.getItems().clear();
        manualtrgtabview.getItems().clear();
        manualunmatchedtrgtabview.getItems().clear();
        manualunmatchedsrctabview.getItems().clear();

    }

    private void automatedTestCase() {
        logger.info("Processing Automated Manual Test Cases");
        progstatus_label.setText("Load the Test Cases..");
        progressLoadingImage();

        System.out.println("Entered - automatedTestCase");

        //progress display
        //clearing the manual tables
        //initialing tmp variables
        String srcconn = "";
        String trgconn = "";
        Sheet sh = null;
        int row;

        //clear manual textarea of src and trg
        manualjtasrcquery.setText("");
        manualjtatrgquery.setText("");

        String fname = "";
        String fpath = "";
        String testname = "";
        String testcaseid = "";
        String srcqry = "";
        String trgqry = "";

        //now focus to manaul tab
        SingleSelectionModel<Tab> selectionModel = tbpanemanautotest.getSelectionModel();
        selectionModel.select(tabmantesting);

        SingleSelectionModel<Tab> selectionModel2 = manualResultTabpane.getSelectionModel();
        selectionModel2.select(manualResultSummaryTab);
//        LoadManualTestCaseUI loadTestCase = new LoadManualTestCaseUI(lfftv, mainvbox);
//        List fileWhereClause = loadTestCase.getFileWhereClauseData();

        fileWhereClause = loadManualTestCaseUI(mainvbox);

        Workbook wb = null;
        FileInputStream fs = null;
        try {
            if (!fileWhereClause.isEmpty()) {

                System.out.println("SS: " + fileWhereClause.get(1).toString());
//            Stage mainstage = (Stage) mainvbox.getScene().getWindow();
                File file = new File(fileWhereClause.get(0).toString());

                if (file != null) {
                    FileReader fr = null;
                    fname = file.getAbsoluteFile().getName();
                    fpath = file.getAbsoluteFile().getAbsolutePath();

                    //System.out.println("file absolute path :" + fpath);
                    //reading the file content
                    fs = new FileInputStream(fpath);
                    wb = Workbook.getWorkbook(fs);
                    sh = wb.getSheet(0);

                    testname = sh.getCell(1, 0).getContents();
                    //System.out.println("Auto Test Name : " + testname);

                    srcconn = sh.getCell(1, 5).getContents();
                    trgconn = sh.getCell(1, 4).getContents();
                    File srcf = null, trgf = null;
                    if (srcconn.toLowerCase().contains("#")) {
                        srcf = new File("conn/" + srcconn + ".con");
                    } else {
                        srcf = new File("files/" + srcconn.substring(srcconn.lastIndexOf(".") + 1, srcconn.length()) + "/" + srcconn);

                    }

                    if (trgconn.toLowerCase().contains("#")) {
                        trgf = new File("conn/" + trgconn + ".con");
                    } else {
                        trgf = new File("files/" + trgconn.substring(trgconn.lastIndexOf(".") + 1, trgconn.length()) + "/" + trgconn);
                    }
                    System.out.println("SRC: " + srcf);
                    System.out.println("TRG: " + trgf);
                    if (!srcf.exists() && !srcf.isFile()) {
                        logger.error("Source Host doesn't exists: " + srcf);
                        progstatus_label.setText("Error src con file not found");
                        progressCompletedImage();
                        new ExceptionUI(new Exception("Source Host Connection not found"));
                    } else if (!trgf.exists() && !trgf.isFile()) {
                        logger.error("Target Host doesn't exists: " + trgf);
                        progstatus_label.setText("Error trg con file not found");
                        progressCompletedImage();
                        new ExceptionUI(new Exception("Target Host Connection not found"));
                    } else {
                        srcRTmpPath = "";
                        trgRTmpPath = "";
                        if (trgconn.toLowerCase().contains("#")) {
                            setConnName(tftrgconname, trgconn, "", "", "db");
                        } else {
                            setConnName(tftrgconname, trgconn, "", "", trgconn.substring(trgconn.lastIndexOf(".") + 1, trgconn.length()));
                            if (tftrgconname.getText().toLowerCase().contains("@")) {
                                logger.info("Checking the Target Remote file");
                                trgRTmpPath = getRemoteFile(tftrgconname, "files/" + trgconn.substring(trgconn.lastIndexOf(".") + 1, trgconn.length()) + "/" + trgconn);
                            }
                        }

                        if (srcconn.toLowerCase().contains("#")) {
                            setConnName(tfsrcconname, srcconn, "", "", "db");
                        } else {
                            setConnName(tfsrcconname, srcconn, "", "", srcconn.substring(srcconn.lastIndexOf(".") + 1, srcconn.length()));
                            logger.info("Setting Manual Connection : " + tfsrcconname.getText() + " :: " + srcconn);
                            if (tfsrcconname.getText().toLowerCase().contains("@")) {
                                logger.info("Checking the Source Remote file");
                                srcRTmpPath = getRemoteFile(tfsrcconname, "files/" + srcconn.substring(srcconn.lastIndexOf(".") + 1, srcconn.length()) + "/" + srcconn);
                            }
                        }

                        int totalNoOfRows = sh.getRows();

                        this.setTestCaseRowSize(totalNoOfRows - 7);
                        String tcid = null;
                        String sql;
                        SqlParser sp;
                        Formatter f;
                        String formatted_sql_code;
                        srctcidmap = new HashMap<String, String>();
                        trgtcidmap = new HashMap<String, String>();
                        for (row = 7; row < totalNoOfRows; row++) {

                            //System.out.println("Auto Test Case Line[1] : " + sh.getRow(row));
                            tcid = sh.getCell(0, row).getContents();
                            manualjtasrcquery.appendText("---------------------------\n");
                            manualjtasrcquery.appendText("Test Case Id : " + tcid);
                            manualjtasrcquery.appendText("\n");
                            manualjtasrcquery.appendText("---------------------------");
                            //verify the sql syntax
                            sql = sh.getCell(2, row).getContents();
//                            sql = sql + " "+fileWhereClause.get(1).toString();
                            sql = setWhereCondition(sql, fileWhereClause.get(1).toString(), fileWhereClause.get(3).toString(), fileWhereClause.get(5).toString(), fileWhereClause.get(7).toString());
                            sp = new SqlParser();
                            sp.checkSqlQuery(sql);
                            f = new BasicFormatterImpl();
                            formatted_sql_code = f.format(sql);
                            srctcidmap.put(tcid, formatted_sql_code);
                            manualjtasrcquery.appendText(formatted_sql_code);
                            manualjtasrcquery.appendText("\n");
                            manualjtatrgquery.appendText("---------------------------\n");
                            manualjtatrgquery.appendText("Test Case Id : " + tcid);
                            manualjtatrgquery.appendText("\n");
                            manualjtatrgquery.appendText("---------------------------");
                            //verify the sql syntax
                            sql = sh.getCell(1, row).getContents();
                            sql = setWhereCondition(sql, fileWhereClause.get(2).toString(), fileWhereClause.get(4).toString(), fileWhereClause.get(6).toString(), fileWhereClause.get(8).toString());
//                            sql = sql +" "+ fileWhereClause.get(2).toString();
                            sp.checkSqlQuery(sql);
                            formatted_sql_code = f.format(sql);
                            trgtcidmap.put(tcid, formatted_sql_code);
                            manualjtatrgquery.appendText(formatted_sql_code);
                            manualjtatrgquery.appendText("\n");
                            System.out.println("srct : " + srctcidmap.get(tcid));
                            System.out.println("trgt : " + trgtcidmap.get(tcid));

                            String srcConnName = "", trgConnName = "";
                            if (getSourceType().equalsIgnoreCase("db") && getTargetType().equalsIgnoreCase("db")) {
                                System.out.println("Exec Start");
                                srcConnName = srcconn;
                                trgConnName = trgconn;

                            } //db to file
                            else if (getSourceType().equalsIgnoreCase("db") && getTargetType().equalsIgnoreCase("ff")) {
                                System.out.println(" DB and Flat File  Data fetching");
                                srcConnName = srcconn;
                                if (trgRTmpPath != null && !trgRTmpPath.isEmpty()) {
                                    trgConnName = trgRTmpPath;
                                } else {
                                    trgConnName = tftrgconname.getText().substring(tftrgconname.getText().lastIndexOf("::") + 2, tftrgconname.getText().length());
                                }

                            }//file to db
                            else if (getSourceType().equalsIgnoreCase("ff") && getTargetType().equalsIgnoreCase("db")) {
                                System.out.println("Flat File and DB Data fetching");
                                trgConnName = trgconn;
                                if (srcRTmpPath != null && !srcRTmpPath.isEmpty()) {
                                    srcConnName = srcRTmpPath;
                                } else {
                                    srcConnName = tfsrcconname.getText().substring(tfsrcconname.getText().lastIndexOf("::") + 2, tfsrcconname.getText().length());
                                }
                            }//file to file
                            else {

                                System.out.println("Flat File Data Fetching");

                                if (trgRTmpPath != null && !trgRTmpPath.isEmpty()) {
                                    trgConnName = trgRTmpPath;
                                } else {
                                    trgConnName = tftrgconname.getText().substring(tftrgconname.getText().lastIndexOf("::") + 2, tftrgconname.getText().length());
                                }

                                if (srcRTmpPath != null && !srcRTmpPath.isEmpty()) {
                                    srcConnName = srcRTmpPath;
                                } else {
                                    srcConnName = tfsrcconname.getText().substring(tfsrcconname.getText().lastIndexOf("::") + 2, tfsrcconname.getText().length());
                                }
                            }

                            System.out.println("SRCConn: " + srcConnName);
                            System.out.println("TRGConn: " + trgConnName);
                            automateDBUIThread(srcConnName, srctcidmap.get(tcid), trgConnName, trgtcidmap.get(tcid), tcid);
                        }

//                        for (row = 7; row < totalNoOfRows; row++) {
//                            tcid = sh.getCell(0, row).getContents();
//                            if (!srcconn.contains("FlatFile::") && !trgconn.contains("FlatFile::")) {
//                                srctcidmap.put(tcid, sh.getCell(2, row).getContents());
//                                trgtcidmap.put(tcid, sh.getCell(1, row).getContents());
//                                automateDBUIThread(srcconn, sh.getCell(2, row).getContents(), trgconn, sh.getCell(1, row).getContents(), tcid);
//                            } else {
//                                srctcidmap.put(tcid, sh.getCell(2, row).getContents());
//                                trgtcidmap.put(tcid, sh.getCell(1, row).getContents());
//                                //automateFFUIThread(srcconn, sh.getCell(2, row).getContents(), trgconn, sh.getCell(1, row).getContents(), tcid);
//                            }
//                        }
                        manualClear.setDisable(false);
                        manualRun.setDisable(false);
                        manualSave.setDisable(false);

                        //progress display
                        progstatus_label.setText("Loading Test Cases completed");
                        progressCompletedImage();

                    }

                }
            } else {
                progstatus_label.setText("Lets Get Started");
                progressCompletedImage();
            }
            //System.out.println("Row Value :"+row+" Testcases :"+totalNoOfRows);
        } catch (NullPointerException ex) {
            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
            progstatus_label.setText("Lets Get Started");
            progressCompletedImage();
        } catch (Exception ex) {
            Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
            new ExceptionUI(ex);
        } finally {
            progstatus_label.setText("Lets Get Started");
            progressCompletedImage();
            try {
                fs.close();
                wb.close();
            } catch (IOException ex) {
                Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, ex);
                new ExceptionUI(ex);
            }
        }

    }

    public void setTestCaseRowSize(int rowsize) {

        this.testcaserowsize = rowsize;

    }

    public int getTestCaseRowSize() {

        return this.testcaserowsize;
    }

    public void automateDBUIThread(String srcconn, String srcqry, String trgconn, String trgqry, String tcid) {

        Task task = new Task<Void>() {
            @Override
            public Void call() throws SQLException, IOException, FileNotFoundException, ClassNotFoundException, Exception {
                //NOne ui code
                //System.out.println(tcid + " " + trgqry + " " + srcqry);
                //call db con mgr and get results 
                ObservableList<ObservableList> srcdata, trgdata;

                if (srcconn.toLowerCase().contains("#")) {
                    DBConnectionManager dbConManager = new DBConnectionManager();
                    Connection srccon = dbConManager.getDBConFromFile(srcconn);
                    srcdata = dbConManager.getDataFromQuery(srccon, srcqry);
                } else {
                    CSVSQLEngine cSVSQLEngine = new CSVSQLEngine();
                    logger.info("Src Conn Name: " + srcconn);
                    srcdata = cSVSQLEngine.getFFTableData(srcconn, srcqry);
                }

                if (trgconn.toLowerCase().contains("#")) {
                    DBConnectionManager dbConManager = new DBConnectionManager();
                    Connection trgcon = dbConManager.getDBConFromFile(trgconn);
                    trgdata = dbConManager.getDataFromQuery(trgcon, trgqry);
                } else {
                    CSVSQLEngine cSVSQLEngine = new CSVSQLEngine();
                    trgdata = cSVSQLEngine.getFFTableData(trgconn, trgqry);
                }

//                DBConnectionManager dbConManager1 = new DBConnectionManager();
//                DBConnectionManager dbConManager2 = new DBConnectionManager();
//                Connection srccon = dbConManager1.getDBConFromFile(srcconn);
//                Connection trgcon = dbConManager2.getDBConFromFile(trgconn);
//
//                srcdata = dbConManager1.getDataFromQuery(srccon, srcqry);
//                trgdata = dbConManager2.getDataFromQuery(trgcon, trgqry);
                //adding data bean
                ManualSummaryBean msb = new ManualSummaryBean();
                ObservableList<ManualSummaryBean> atcdata = FXCollections.observableArrayList();;

                //testcase pass / fail logic
                if (srcdata.containsAll(trgdata) && trgdata.containsAll(srcdata)) {

                    String[] srclen = srcqry.toLowerCase().split("count\\(");
                    String[] trglen = srcqry.toLowerCase().split("count\\(");
                    if (srclen.length == 2 && trglen.length == 2) {
                        msb.testcaseid.setValue(tcid.toString());
                        msb.result.setValue("Passed --> Count(Src : " + srcdata.get(0).get(0) + ",Trg : " + trgdata.get(0).get(0) + ")");
                    } else {
                        msb.testcaseid.setValue(tcid.toString());
                        msb.result.setValue("Passed");
                    }

                } else {
                    String[] srclen = srcqry.toLowerCase().split("count\\(");
                    String[] trglen = srcqry.toLowerCase().split("count\\(");
                    if (srclen.length == 2 && trglen.length == 2) {
                        msb.testcaseid.setValue(tcid.toString());
                        msb.result.setValue("Failed --> Count(Src : " + srcdata.get(0).get(0) + ",Trg : " + trgdata.get(0).get(0) + ")");
                    } else {
                        msb.testcaseid.setValue(tcid.toString());
                        msb.result.setValue("Failed");
                    }
                }
                atcdata.add(msb);

                Platform.runLater(new Runnable() {
                    public void run() {
                        //add results to table
                        manualviewsummarytable.getItems().addAll(atcdata);
                        summaryTestResulTableRowHighlighter(manualviewsummarytable);
                        //progress status
                        progstatus_label.setText("Executing Test Cases");
                        progressLoadingImage();

                        if (manualviewsummarytable.getItems().size() == getTestCaseRowSize()) {

                            //progress status
                            progstatus_label.setText("Execution Completed");
                            progressCompletedImage();

                        }

                    }

                });

                return null;
            }
        };

        task.setOnRunning(new EventHandler<WorkerStateEvent>() {
            @Override
            public void handle(WorkerStateEvent event) {
                //progress status
                progstatus_label.setText("Execution Test Cases");
                progressLoadingImage();

            }
        });

        task.setOnSucceeded(new EventHandler<WorkerStateEvent>() {
            @Override
            public void handle(WorkerStateEvent event) {

                if (manualviewsummarytable.getItems().size() == getTestCaseRowSize()) {
                    //progress status
                    progstatus_label.setText("Execution Completed");
                    progressCompletedImage();

                } else {

                    //progress status
                    progstatus_label.setText("Execution Completed with Errors");
                    progressCompletedImage();
                }
            }

        });

        task.setOnFailed(new EventHandler<WorkerStateEvent>() {
            @Override
            public void handle(WorkerStateEvent arg0) {
                progstatus_label.setText("Failed with Exception");
                progressCompletedImage();
                Throwable throwable = task.getException();
                Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, throwable);
                new ExceptionUI(new Exception(throwable.getMessage()));
            }
        });

        Thread t = new Thread(task);
        t.setDaemon(true);
        t.start();
    }

    //Getting Connection details
    public String getManualConnDetails(TextField connUI, String connName, String connType, String connArea) throws IOException, ParseException {
        String connValue = "";
        logger.info("Manual Conn: " + connName);
        if (connType.equalsIgnoreCase("db")) {
            connValue = this.getConnNameFromUI(connArea);
        } else {
            if (connUI.getText().toLowerCase().contains("@")) {
                connValue = new File(getRemoteFile(connUI, connName)).getParent() + "?schema=" + FILEPATH;
            } else {
                connValue = new File(this.getConnNameFromUI(connArea)).getParent() + "?schema=" + FILEPATH;
            }

        }

        System.out.println("Connvalue: " + connValue);
        return connValue;
    }

    private void processSinglTestcase(String tcid) throws Exception {
        SqlParser sqlParser = new SqlParser();
        System.out.println("Entered  - executeSinglTestcase");

        //System.out.println("Testcase Row Num:" +tcid +":" +tcidmap.get(tcid));
        String trgqry = trgtcidmap.get(tcid);
        String srcqry = srctcidmap.get(tcid);

//        System.out.println("trgqry : " + trgqry);
//        System.out.println("srcqry : " + srcqry);
        //now focus to manaul tab
//        SingleSelectionModel<Tab> selectionModel = manualResultTabpane.getSelectionModel();
//        selectionModel.select(manualsrcdttab);
        if (srcExec.isSelected() && !trgExec.isSelected()) {
            SingleSelectionModel<Tab> selectionModel = manualResultTabpane.getSelectionModel();
            selectionModel.select(manualsrcdttab);
        } else if (!srcExec.isSelected() && trgExec.isSelected()) {
            SingleSelectionModel<Tab> selectionModel = manualResultTabpane.getSelectionModel();
            selectionModel.select(manualtrgdttab);
        } else {
            SingleSelectionModel<Tab> selectionModel = manualResultTabpane.getSelectionModel();
            selectionModel.select(manualsrcdttab);
        }

        System.out.println("Src: " + tfsrcconname.getText());
        System.out.println("trg: " + tftrgconname.getText());
        //Lets execute single TestPlan
//        String srcCon = tfsrcconname.getText();
//        String trgCon = tftrgconname.getText();

        String srcCon = "", trgCon = "";
        if (this.getSourceType().equalsIgnoreCase("db")) {
            System.out.println("Source DB");
            srcCon = getManualConnDetails(tfsrcconname, "", this.getSourceType(), "src");
        } else {
            System.out.println("Source FF");
            srcCon = getManualConnDetails(tfsrcconname, "files/" + tfsrcconname.getText().substring(tfsrcconname.getText().lastIndexOf(".") + 1, tfsrcconname.getText().length()) + "" + tfsrcconname.getText().substring(tfsrcconname.getText().lastIndexOf("/"), tfsrcconname.getText().length()), this.getSourceType(), "src");
        }

        if (this.getTargetType().equalsIgnoreCase("db")) {
            System.out.println("Target DB");
            trgCon = getManualConnDetails(tftrgconname, "", this.getTargetType(), "trg");
        } else {
            System.out.println("Target FF");
//            trgCon = getManualConnDetails(tftrgconname, "files/" + this.getTableNameFromUI("trg") + "." + tftrgconname.getText().substring(tftrgconname.getText().lastIndexOf("/"), tftrgconname.getText().length()), this.getTargetType(), "trg");
            trgCon = getManualConnDetails(tftrgconname, "files/" + tftrgconname.getText().substring(tftrgconname.getText().lastIndexOf(".") + 1, tftrgconname.getText().length()) + "" + tftrgconname.getText().substring(tftrgconname.getText().lastIndexOf("/"), tftrgconname.getText().length()), this.getTargetType(), "trg");
        }

        System.out.println("Src Connection: " + srcCon);
        System.out.println("Trg Connection: " + trgCon);

        if (manualTestCaseExec) {
            manualTestCaseExec = false;
            //sqlParser.checkSqlQuery(srcqry);
            System.out.println("Src Connection: " + srcCon);
            executeSingleTestcase(srcCon, srcqry, manualsrctabview);
            //sqlParser.checkSqlQuery(trgqry);
            System.out.println("Trg Connection: " + trgCon);
            executeSingleTestcase(trgCon, trgqry, manualtrgtabview);
            if (srcExec.isSelected() && trgExec.isSelected()) {
                SingleSelectionModel<Tab> selectionModel = manualResultTabpane.getSelectionModel();
                selectionModel.select(manualsrcdttab);
            }
            if (srcExec.isSelected()) {
                SingleSelectionModel<Tab> selectionModel = manualResultTabpane.getSelectionModel();
                selectionModel.select(manualsrcdttab);
            }
            if (trgExec.isSelected()) {
                SingleSelectionModel<Tab> selectionModel = manualResultTabpane.getSelectionModel();
                selectionModel.select(manualtrgdttab);
            }

        }

        if (srcExec.isSelected() && trgExec.isSelected()) {
            if ((!srcCon.isEmpty() && !srcqry.isEmpty()) && (!trgCon.isEmpty() && !trgqry.isEmpty())) {
                //sqlParser.checkSqlQuery(srcqry);
                System.out.println("Src Connection: " + srcCon);
                executeSingleTestcase(srcCon, srcqry, manualsrctabview);
            } else {
                throw new Exception("Select Source/Target query to execute");
            }
        }
        if (srcExec.isSelected()) {
            if (!srcCon.isEmpty() && !srcqry.isEmpty()) {
                //sqlParser.checkSqlQuery(srcqry);
                System.out.println("Src Connection: " + srcCon);
                executeSingleTestcase(srcCon, srcqry, manualsrctabview);
            } else {
                throw new Exception("Select Source query to execute");
            }
        }

        if (trgExec.isSelected()) {
            if (!trgCon.isEmpty() && !trgqry.isEmpty()) {
                //sqlParser.checkSqlQuery(trgqry);
                System.out.println("Trg Connection: " + trgCon);
                executeSingleTestcase(trgCon, trgqry, manualtrgtabview);
            } else {
                throw new Exception("Select Target query to execute");
            }
        }
        manualClear.setDisable(false);
        manualRun.setDisable(false);
        manualSave.setDisable(false);

    }

    public void addColToTable(List colslist, TableView tbl) {
        tbl.getColumns().clear();
        //adding call to the tables
        TableColumn col;
        Object colname;
        for (Iterator it = colslist.iterator(); it.hasNext();) {
            colname = it.next();
            col = new TableColumn(colname.toString());
            final int j = colslist.indexOf(colname);
            col.setCellValueFactory(new Callback<TableColumn.CellDataFeatures<ObservableList, String>, ObservableValue<String>>() {
                public ObservableValue<String> call(TableColumn.CellDataFeatures<ObservableList, String> param) {
                    return new SimpleStringProperty(param.getValue().get(j) == null ? "null" : param.getValue().get(j).toString());
                }
            });

            tbl.getColumns().addAll(col);

        }

    }

    private void executeSingleTestcase(String con, String qry, TableView tblView) {
        manualsrctabview.getItems().clear();
        manualtrgtabview.getItems().clear();
        manualunmatchedtrgtabview.getItems().clear();
        manualunmatchedsrctabview.getItems().clear();
        tblView.getItems().clear();
        tblView.getColumns().clear();
        Task task = new Task<Void>() {
            @Override
            public Void call() throws SQLException, IOException, FileNotFoundException, ClassNotFoundException, Exception {
                //NOne ui code
                System.out.println("Execute Single Test Plan");
                System.out.println("Conn: " + con);
                //call db con mgr and get results 
                ObservableList<ObservableList> data;
                List colslist;
                if (con.toLowerCase().contains("#")) {
                    DBConnectionManager dbConManager1 = new DBConnectionManager();

                    Connection srccon = dbConManager1.getDBConFromFile(con);
                    data = dbConManager1.getDataFromQuery(srccon, qry);
                    colslist = dbConManager1.getColNames(srccon, qry);
                } else {
                    CSVSQLEngine cSVSQLEngine = new CSVSQLEngine();
                    data = cSVSQLEngine.getFFTableData(con, qry);
                    colslist = cSVSQLEngine.getFFColumns(con, qry);
                }

                //getting data 
                System.out.println("Connection Name: " + con);
//                cSVSQLEngine.getFFConn(getConnNameFromUI("src"));
//                ObservableList<ObservableList> data = cSVSQLEngine.getFFTableData(getConnNameFromUI("src"), qry);
                //getting cols

                //System.out.println("Srccolslist "+srccolslist);
                //System.out.println("Trgcolslist "+trgcolslist);
                srcColList = new ArrayList();
//                List colslist = cSVSQLEngine.getFFColumns(getConnNameFromUI("src"), qry);
                srcColList.addAll(colslist);

                Platform.runLater(new Runnable() {
                    public void run() {
                        //adding call to the tables
                        addColToTable(colslist, tblView);

                        //add results to table
                        tblView.setItems(data);

                        //progress status
                        progstatus_label.setText("Executing Test Cases");
                        progressLoadingImage();

                    }

                });
                return null;
            }
        };

        task.setOnRunning(new EventHandler<WorkerStateEvent>() {
            @Override
            public void handle(WorkerStateEvent event) {
                //progress status
                progstatus_label.setText("Execution Test Cases");
                progressLoadingImage();

            }
        });

        task.setOnSucceeded(new EventHandler<WorkerStateEvent>() {
            @Override
            public void handle(WorkerStateEvent event) {
                //progress status
                progstatus_label.setText("Execution Completed");
                progressCompletedImage();
                //call compare results
//                compareSingelTestcase(manualsrctabview, manualtrgtabview);
                if (manualsrctabview.getItems().size() > 0 && manualtrgtabview.getItems().size() > 0) {
                    System.out.println("Processing Comparision");
                    compareSingelTestcase(manualsrctabview, manualtrgtabview);
                }

            }

        });

        task.setOnFailed(new EventHandler<WorkerStateEvent>() {
            @Override
            public void handle(WorkerStateEvent arg0) {
                progstatus_label.setText("Failed with Exception");
                progressCompletedImage();
                Throwable throwable = task.getException();
                Logger.getLogger(MainStageController.class.getName()).log(Level.SEVERE, null, throwable);
                new ExceptionUI(new Exception(throwable.getMessage()));
            }
        });

        Thread t = new Thread(task);
        t.setDaemon(true);
        t.start();

    }

    private void compareSingelTestcase(TableView manualsrctabview, TableView manualtrgtabview) {
        System.out.println("Compare Single Test Case");
        manualunmatchedsrctabview.getItems().clear();
        manualunmatchedsrctabview.getColumns().clear();
        manualunmatchedtrgtabview.getItems().clear();
        manualunmatchedtrgtabview.getColumns().clear();
//        if (!manualsrctabview.getItems().isEmpty() && !manualtrgtabview.getItems().isEmpty()) {
        System.out.println("Compare Single Test Case");
        List<String> srcList = new ArrayList<>();
        for (Object s : manualsrctabview.getColumns()) {
            TableColumn s1 = (TableColumn) s;
            srcList.add(s1.getText());
        }
        System.out.println("Src List: " + srcList);
        List<String> trgList = new ArrayList<>();
        for (Object s : manualtrgtabview.getColumns()) {
            TableColumn s1 = (TableColumn) s;
            trgList.add(s1.getText());
        }
        System.out.println("Trg List: " + trgList);
        addColToTable(srcList, manualunmatchedsrctabview);
        addColToTable(trgList, manualunmatchedtrgtabview);

        ObservableList srcresult = manualsrctabview.getItems();
        ObservableList trgresult = manualtrgtabview.getItems();

        //Start: breaking the list into chunks: Snehasish
        long processStart = new Date().getTime();
        ComparisonResultModel responseModel = null;
        if (srcresult.size() > 0 && trgresult.size() > 0) {
            long start = new Date().getTime();

            ComparisonProcessor processor = new ComparisonProcessor();
            processor.setSrcInput(srcresult);
            processor.setTrgInput(trgresult);
            responseModel = processor.process();

            long end = new Date().getTime();

            System.out.println("TIME TAKEN: " + (end - start) / 1000);
        }

        if (responseModel != null) {
            ObservableList src = FXCollections.observableArrayList(responseModel.getSrclist());
            ObservableList trg = FXCollections.observableArrayList(responseModel.getTrglist());
            manualunmatchedsrctabview.setItems(src);
            manualunmatchedtrgtabview.setItems(trg);
        }

        long processEnd = new Date().getTime();

        System.out.println("TIME process: " + (processEnd - processStart) / 1000);
        //End: breaking the list into chunks: Snehasish
    }

    private ObservableList compareSTMValidation(ObservableList stmsrctabview, ObservableList stmtrgtabview) {
        logger.info("Compare STM Data Validation");
        System.out.println("Compare STM Data Validation");

        long processStart = new Date().getTime();
        ComparisonResultModel responseModel = null;
        if (stmsrctabview.size() > 0 && stmtrgtabview.size() > 0) {
            long start = new Date().getTime();

            ComparisonProcessor processor = new ComparisonProcessor();
            processor.setSrcInput(stmsrctabview);
            processor.setTrgInput(stmtrgtabview);
            responseModel = processor.process();

            long end = new Date().getTime();

            System.out.println("TIME TAKEN: " + (end - start) / 1000);
        }

        ObservableList list = FXCollections.observableArrayList();
        if (responseModel != null) {
//            ObservableList src = FXCollections.observableArrayList(responseModel.getSrclist());
//            ObservableList trg = FXCollections.observableArrayList(responseModel.getTrglist());
//            manualunmatchedsrctabview.setItems(src);
//            manualunmatchedtrgtabview.setItems(trg);

            list.add(responseModel.getSrclist());
            list.add(responseModel.getTrglist());
        } else {
            list.add(FXCollections.observableArrayList());
            list.add(FXCollections.observableArrayList());
        }

        long processEnd = new Date().getTime();

        System.out.println("TIME process: " + (processEnd - processStart) / 1000);
        //End: breaking the list into chunks: Snehasish

        logger.info("Compare STM Data Validation Completed");
        return list;
    }

    @FXML
    public void manualRunButtonAction(ActionEvent ae) {
        fileWhereClause = null;
        try {
            srctcidmap = new HashMap<String, String>();
            trgtcidmap = new HashMap<String, String>();

            srctcidmap.put("1", manualjtasrcquery.getSelectedText());
            trgtcidmap.put("1", manualjtatrgquery.getSelectedText());

//            System.out.println("DatL :" + srctcidmap);
//            System.out.println("Datadf :" + trgtcidmap);
            setTestCaseRowSize(1);

            processSinglTestcase("1");
        } catch (Exception ex) {
            logger.info("manualRunButtonAction: ", ex);
            new ExceptionUI(ex);
        }
    }

    @FXML
    public void manualClearButtonAction(ActionEvent ae
    ) {
        manualviewsummarytable.getItems().clear();
        manualsrctabview.getItems().clear();
        manualtrgtabview.getItems().clear();
        manualunmatchedtrgtabview.getItems().clear();
        manualunmatchedsrctabview.getItems().clear();
        manualClear.setDisable(true);
        manualSave.setDisable(true);
    }

    public Map<String, TableView> getAllManualTable() {
        Map<String, TableView> tableViewKV = new HashMap<String, TableView>();

        if (!manualviewsummarytable.getItems().isEmpty()) {
            tableViewKV.put("MANUAL_SUMMARY", manualviewsummarytable);
        }

        if (!manualsrctabview.getItems().isEmpty()) {
            tableViewKV.put("MANUAL_SOURCE", manualsrctabview);
        }

        if (!manualtrgtabview.getItems().isEmpty()) {
            tableViewKV.put("MANUAL_TARGET", manualtrgtabview);
        }

        if (!manualunmatchedsrctabview.getItems().isEmpty()) {
            tableViewKV.put("MANUAL_UNMATCHED_SOURCE", manualunmatchedsrctabview);
        }

        if (!manualunmatchedtrgtabview.getItems().isEmpty()) {
            tableViewKV.put("MANUAL_UNMATCHED_TARGET", manualunmatchedtrgtabview);
        }

        return tableViewKV;
    }

    /**
     * Method to Store the Manual Result to the Excel Sheet
     */
    @FXML
    public void manualSaveButton(ActionEvent ae) {

        Stage mainstage = (Stage) mainvbox.getScene().getWindow();
        DirectoryChooser directoryChooser = new DirectoryChooser();
        FileChooser fc = new FileChooser();
        File dirName = null;
        resultsExport = "";
        if (fileWhereClause != null) {
            if (fileWhereClause.get(0) != null) {
                String file = fileWhereClause.get(0).toString().replace("\\", "/");
                String fileName = file.substring(file.lastIndexOf("/"), file.lastIndexOf(".") + 1) + "_results.xls";
                dirName = directoryChooser.showDialog(mainstage);
                if (dirName != null) {
                    resultsExport = dirName.getAbsolutePath() + "/" + fileName;
                }

            } else {
                dirName = directoryChooser.showDialog(mainstage);
                if (dirName != null) {
                    resultsExport = dirName.getAbsolutePath() + "/Manual_TestCase_" + new SimpleDateFormat("YYYY_MM_dd_HH_MM_SS").format(new Date()) + ".xls";
                }
            }
        } else {
            dirName = directoryChooser.showDialog(mainstage);
            if (dirName != null) {
                resultsExport = dirName.getAbsolutePath() + "/Manual_TestCase_" + new SimpleDateFormat("YYYY_MM_dd_HH_MM_SS").format(new Date()) + ".xls";
            }
        }

        if (!resultsExport.isEmpty()) {

            Map<String, TableView> tableData = getAllManualTable();
            XSSFWorkbook wbook = new XSSFWorkbook();
            XSSFSheet shSheet;
            XSSFSheet tmpSheet;
            FileOutputStream xlsFile = null;
            int sheetIndex = 0;
            if (!tableData.isEmpty()) {
                try {
                    xlsFile = new FileOutputStream(resultsExport);

//                System.out.println("File Name: " + esbStmFile.getName().substring(0, esbStmFile.getName().lastIndexOf('.')));
                    int mapIndex = 0;

                    for (Map.Entry<String, TableView> entry : tableData.entrySet()) {
                        String key = entry.getKey();
                        TableView value = entry.getValue();

                        int k = 0, j = 0;

                        shSheet = wbook.createSheet(key.toUpperCase());

                        System.out.println("srccolnames" + value.getColumns());
                        int rowIndex = 0;
                        if (key.equalsIgnoreCase("MANUAL_SUMMARY")) {
                            ObservableList<TableColumn<ManualSummaryBean, ?>> manualSrcCol = manualviewsummarytable.getColumns();
                            XSSFFont font = wbook.createFont();
                            font.setBold(true);
                            font.setFamily(FontFamily.ROMAN);
                            font.setFontHeight(14);
                            Row row = shSheet.createRow(rowIndex++);
                            for (int l = 0; l < manualSrcCol.size(); l++) {

                                org.apache.poi.ss.usermodel.Cell cell = row.createCell(l);
                                cell.setCellValue(manualSrcCol.get(l).getText());
//                                  
                            }
                            for (int m = 0; m < value.getItems().size(); m++) {

                                String itemData = value.getItems().get(m).toString();
                                itemData.replace("[", "").replace("]", "");
                                System.out.println(value.getItems().get(m).toString());

                                String[] itemDataList = itemData.split("#");
                                Row row1 = shSheet.createRow(m + 1);
                                System.out.println("Lenght: " + itemDataList.length);
                                for (int l = 0; l < itemDataList.length; l++) {
                                    String data = itemDataList[l];
                                    org.apache.poi.ss.usermodel.Cell cell = row1.createCell(l);
                                    cell.setCellValue(data);

                                }

                            }

                        } else {

                            //For Complete Data
                            List cmpltColNames;

                            XSSFFont cellFont = wbook.createFont();
                            cellFont.setBold(true);
                            cellFont.setFamily(FontFamily.ROMAN);
                            cellFont.setFontHeight(14);
                            Row row = shSheet.createRow(rowIndex++);

                            if (key.contains("src")) {
                                cmpltColNames = src_table;
                            } else {
                                cmpltColNames = trg_table;
                            }
                            ObservableList<TableColumn> cmpltColNames1 = value.getColumns();

                            for (int l = 0; l < cmpltColNames1.size(); l++) {
                                org.apache.poi.ss.usermodel.Cell cell = row.createCell(l);

                                cell.setCellValue(cmpltColNames1.get(l).getText());
                            }

                            for (int m = 0; m < value.getItems().size(); m++) {

                                Row row1 = shSheet.createRow(m + 1);
                                List itemData = (List) value.getItems().get(m);

//                                System.out.println("Item Data: " + itemData);
                                for (int l = 0; l < itemData.size(); l++) {
                                    String data = null;
                                    if (itemData.get(l) != null) {
                                        data = itemData.get(l).toString();
                                    } else {
                                        data = "";
                                    }
//                                    System.out.println("Dat: " + data);

                                    org.apache.poi.ss.usermodel.Cell cell = row1.createCell(l);
                                    cell.setCellValue(data);

                                }

                            }

                        }

                    }

                    wbook.write(xlsFile);
                    wbook.close();

                    new AlertUI("Results Saved Successfully");

                } catch (IOException ex) {
                    Logger.getLogger(MainStageController.class
                            .getName()).log(Level.SEVERE, null, ex);
                    new ExceptionUI(ex);

                }
            }
        }
    }

    public String setWhereCondition(String sql, String whereClause, String groupClause, String havingClause, String orderBy) {
        StringBuffer sqlBuffer = new StringBuffer();
        boolean querywhereUpdate = false;
//        String sqlLower = sql.toLowerCase();
        System.out.println("SQL: " + sql);
        System.out.println("Having: " + sql.toLowerCase().lastIndexOf("having"));
        System.out.println("Group: " + sql.toLowerCase().lastIndexOf("group by"));
        System.out.println("Order by: " + sql.toLowerCase().lastIndexOf("order by"));

        System.out.println("Where: " + whereClause + " Group Clause: " + groupClause + " having: " + havingClause + " order by: " + orderBy);

        int selectCount = 0;
        Pattern p = Pattern.compile("select");
        Matcher m = p.matcher(sql.toLowerCase());
        while (m.find()) {
            selectCount++;
        }

        int withCount = 0;
        Pattern p1 = Pattern.compile("with");
        Matcher m1 = p1.matcher(sql.toLowerCase());
        while (m1.find()) {
            withCount++;
        }
        System.out.println("Select Count: " + selectCount + " # With Count: " + withCount);
        if (selectCount == 1 && withCount == 0) {
            //where Clause updation
            if (!whereClause.isEmpty()) {

                if (sql.toLowerCase().contains("group by")) {

                    if (!sql.toLowerCase().contains("where")) {
                        sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("group by")) + " where " + whereClause + " " + sql.toLowerCase().substring(sql.toLowerCase().lastIndexOf("group by"), sql.length())).toString();
                        sqlBuffer.delete(0, sqlBuffer.length());
//                        System.out.println("Updated where: " + sqlLower);

                    } else {
                        sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("group by")) + " and " + whereClause + " " + sql.toLowerCase().substring(sql.toLowerCase().lastIndexOf("group by"), sql.length())).toString();
                        sqlBuffer.delete(0, sqlBuffer.length());
//                        System.out.println("Updated where: " + sqlLower);
                    }
                    querywhereUpdate = true;
                }
//                System.out.println("where: " + querywhereUpdate);
                if (querywhereUpdate == false) {
                    if (sql.toLowerCase().contains("having")) {

                        if (!sql.toLowerCase().contains("where")) {
                            sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("having")) + " where " + whereClause + " " + sql.toLowerCase().substring(sql.toLowerCase().lastIndexOf("having"), sql.length())).toString();
                            sqlBuffer.delete(0, sqlBuffer.length());
//                            System.out.println("Updated where: " + sqlLower);
                        } else {
                            sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("having")) + " and " + whereClause + " " + sql.toLowerCase().substring(sql.toLowerCase().lastIndexOf("having"), sql.length())).toString();
                            sqlBuffer.delete(0, sqlBuffer.length());
//                            System.out.println("Updated where: " + sqlLower);
                        }
                        querywhereUpdate = true;
                    }

                }
                System.out.println("where: " + querywhereUpdate);
                if (querywhereUpdate == false) {
                    if (sql.toLowerCase().contains("order by")) {

                        if (!sql.toLowerCase().contains("where")) {
                            sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("order by")) + " where " + whereClause + " " + sql.toLowerCase().substring(sql.toLowerCase().lastIndexOf("order by"), sql.length())).toString();
                            sqlBuffer.delete(0, sqlBuffer.length());
//                            System.out.println("Updated where: " + sqlLower);
                        } else {
                            sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("order by")) + " and " + whereClause + " " + sql.toLowerCase().substring(sql.toLowerCase().lastIndexOf("order by"), sql.length())).toString();
                            sqlBuffer.delete(0, sqlBuffer.length());
//                            System.out.println("Updated where: " + sqlLower);
                        }
                        querywhereUpdate = true;

                    }
                }
                System.out.println("where: " + querywhereUpdate);
                if (querywhereUpdate == false) {

                    if (!sql.toLowerCase().contains("where")) {
                        sql = sqlBuffer.append(sql + " where " + whereClause).toString();
                        sqlBuffer.delete(0, sqlBuffer.length());
//                        System.out.println("Updated where: " + sqlLower);
                    } else {
                        sql = sqlBuffer.append(sql + " and " + whereClause).toString();
                        sqlBuffer.delete(0, sqlBuffer.length());
//                        System.out.println("Updated where: " + sqlLower);
                    }
                    querywhereUpdate = true;

                }
            }
            //End of where Clause Updation
            boolean groupClauseUpdate = false;
            //Group Clause Clause updation
            if (!groupClause.isEmpty()) {

                if (sql.toLowerCase().contains("having")) {
                    if (sql.toLowerCase().contains("group by")) {
                        sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("having")) + " , " + groupClause + " " + sql.toLowerCase().substring(sql.toLowerCase().lastIndexOf("having"), sql.length())).toString();
                        sqlBuffer.delete(0, sqlBuffer.length());
//                        System.out.println("Updated Group: " + sqlLower);
                    } else {
                        sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("having")) + " group by " + groupClause + " " + sql.toLowerCase().substring(sql.toLowerCase().lastIndexOf("having"), sql.length())).toString();
                        sqlBuffer.delete(0, sqlBuffer.length());
//                        System.out.println("Updated Group: " + sqlLower);
                    }
                    groupClauseUpdate = true;
                }
                if (groupClauseUpdate == false) {
                    if (sql.toLowerCase().contains("order by")) {
                        if (sql.toLowerCase().contains("group by")) {
                            sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("order by")) + " , " + groupClause + " " + sql.toLowerCase().substring(sql.toLowerCase().lastIndexOf("order by"), sql.length())).toString();
                            sqlBuffer.delete(0, sqlBuffer.length());
//                            System.out.println("Updated Group: " + sqlLower);
                        } else {
                            sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("order by")) + " group by " + groupClause + " " + sql.toLowerCase().substring(sql.toLowerCase().lastIndexOf("order by"), sql.length())).toString();
                            sqlBuffer.delete(0, sqlBuffer.length());
//                            System.out.println("Updated Group: " + sqlLower);
                        }
                        groupClauseUpdate = true;
                    } else {
                        if (sql.toLowerCase().contains("group by")) {
                            sql = sqlBuffer.append(sql + " , " + groupClause).toString();
                            sqlBuffer.delete(0, sqlBuffer.length());
//                            System.out.println("Updated Group: " + sqlLower);
                        } else {
                            sql = sqlBuffer.append(sql + " group by " + groupClause).toString();
                            sqlBuffer.delete(0, sqlBuffer.length());
//                            System.out.println("Updated Group: " + sqlLower);
                        }
                    }
                }
            }
            //End of Group Clause Updation
            boolean havingClauseUpdate = false;
            //Group havin Clause updation
            if (!havingClause.isEmpty()) {

                if (sql.toLowerCase().contains("order by")) {
                    if (sql.toLowerCase().contains("having")) {
                        sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("order by")) + " and  " + havingClause + " " + sql.toLowerCase().substring(sql.toLowerCase().lastIndexOf("order by"), sql.length())).toString();
                        sqlBuffer.delete(0, sqlBuffer.length());
//                        System.out.println("Updated Having: " + sqlLower);
                    } else {
                        sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("order by")) + " having " + havingClause + " " + sql.toLowerCase().substring(sql.toLowerCase().lastIndexOf("order by"), sql.length())).toString();
                        sqlBuffer.delete(0, sqlBuffer.length());
//                        System.out.println("Updated Having: " + sqlLower);
                    }

                } else {
                    if (sql.toLowerCase().contains("having")) {
                        sql = sqlBuffer.append(sql + " and " + havingClause).toString();
                        sqlBuffer.delete(0, sqlBuffer.length());
//                        System.out.println("Updated Having: " + sqlLower);
                    } else {
                        sql = sqlBuffer.append(sql + " having " + havingClause).toString();
                        sqlBuffer.delete(0, sqlBuffer.length());
//                        System.out.println("Updated Having: " + sqlLower);
                    }
                }
            }
            //End of having Clause Updation

            //Group order by Clause updation
            if (!orderBy.isEmpty()) {

                if (sql.toLowerCase().contains("order by")) {
                    if (sql.toLowerCase().contains("asc")) {
                        if (orderBy.contains("asc")) {
                            sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("asc")) + " , " + orderBy).toString();
                            sqlBuffer.delete(0, sqlBuffer.length());
//                            System.out.println("Updated Order by: " + sqlLower);
                        } else {
                            sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("asc")) + " , " + orderBy + " " + sql.toLowerCase().substring(sql.toLowerCase().lastIndexOf("asc"), sql.length())).toString();
                            sqlBuffer.delete(0, sqlBuffer.length());
//                            System.out.println("Updated Order by: " + sqlLower);
                        }
                    } else if (sql.toLowerCase().contains("desc")) {
                        if (orderBy.contains("desc")) {
                            sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("desc")) + " , " + orderBy).toString();
                            sqlBuffer.delete(0, sqlBuffer.length());
//                            System.out.println("Updated Order by: " + sqlLower);
                        } else {
                            sql = sqlBuffer.append(sql.substring(0, sql.toLowerCase().lastIndexOf("desc")) + " , " + orderBy + " " + sql.toLowerCase().substring(sql.toLowerCase().lastIndexOf("desc"), sql.length())).toString();
                            sqlBuffer.delete(0, sqlBuffer.length());
//                            System.out.println("Updated Order by: " + sqlLower);
                        }
                    } else {
                        sql = sqlBuffer.append(sql + " ," + orderBy).toString();
                        sqlBuffer.delete(0, sqlBuffer.length());
//                        System.out.println("Updated Order by: " + sqlLower);
                    }
                } else {
                    sql = sqlBuffer.append(sql + " order by " + orderBy).toString();
                    sqlBuffer.delete(0, sqlBuffer.length());
//                    System.out.println("Updated Order by: " + sqlLower);
                }

            }
        } else {

        }
        //End of order by Clause Updation
//        System.out.println("SQL Buffer: " + sqlBuffer);
        return sql;
    }

    public List loadManualTestCaseUI(VBox mainvbox) {

        dialog = new Dialog<>();

        dialog.setTitle("Test Case Document");
        dialog.setHeaderText("Load the Test Case File");

        // Get the Stage.
        Stage stage = (Stage) dialog.getDialogPane().getScene().getWindow();

        // Add a custom icon.
        stage.getIcons().add(new Image(this.getClass().getResource("/icon/filesicon.png").toString()));

        // Set the icon (must be included in the project).
        dialog.setGraphic(new ImageView(new Image(getClass().getResourceAsStream("/icon/filesicon.png"))));

        // Set the button types.
        loginButtonType = new ButtonType("ADD", ButtonBar.ButtonData.OK_DONE);
        test = new Button("Test");
        test.setMinWidth(100);
        dialog.getDialogPane().getButtonTypes().addAll(loginButtonType, ButtonType.CANCEL);
        // Create the username and password labels and fields.
        GridPane grid = new GridPane();
        grid.setHgap(5);
        grid.setVgap(5);
        grid.setPadding(new Insets(20, 10, 10, 10));
        Label msgLable = new Label("Choose:");
        localfilebt = new Button();
        localfilebt.setText("Upload File");
        localfilebt.setMaxWidth(150);

        whereClausesrc = new TextArea();
        whereClausesrc.setPromptText("Ex: col1 = 'value1' and col2 = 'value2 ");
        whereClausesrc.setMaxWidth(250);
        whereClausesrc.setMaxHeight(50);
        whereClausesrc.setDisable(true);
        whereClausetrg = new TextArea();
        whereClausetrg.setPromptText("Ex: col1 = 'value1' and col2 = 'value2");
        whereClausetrg.setMaxWidth(250);
        whereClausetrg.setMaxHeight(50);
        whereClausetrg.setDisable(true);

        groupBysrc = new TextArea();
        groupBysrc.setPromptText("Ex: having col1 = 'value1' order by colname");
        groupBysrc.setMaxWidth(250);
        groupBysrc.setMaxHeight(30);
        groupBysrc.setDisable(true);

        groupBytrg = new TextArea();
        groupBytrg.setPromptText("Ex: having col1 = 'value1' order by colname");
        groupBytrg.setMaxWidth(250);
        groupBytrg.setMaxHeight(30);
        groupBytrg.setDisable(true);

        havingSrc = new TextArea();
        havingSrc.setPromptText("Ex: having col1 = 'value1' order by colname");
        havingSrc.setMaxWidth(250);
        havingSrc.setMaxHeight(30);
        havingSrc.setDisable(true);

        havingTrg = new TextArea();
        havingTrg.setPromptText("Ex: having col1 = 'value1' order by colname");
        havingTrg.setMaxWidth(250);
        havingTrg.setMaxHeight(30);
        havingTrg.setDisable(true);

        orderBySrc = new TextArea();
        orderBySrc.setPromptText("Ex: having col1 = 'value1' order by colname");
        orderBySrc.setMaxWidth(250);
        orderBySrc.setMaxHeight(30);
        orderBySrc.setDisable(true);

        orderByTrg = new TextArea();
        orderByTrg.setPromptText("Ex: having col1 = 'value1' order by colname");
        orderByTrg.setMaxWidth(250);
        orderByTrg.setMaxHeight(30);
        orderByTrg.setDisable(true);

        grid.add(msgLable, 0, 1);

        grid.add(localfilebt, 2, 1);

        grid.add(new Label("Source Where Condition:"), 0, 3);
        grid.add(whereClausesrc, 0, 4, 2, 1);
        grid.add(new Label("Target Where Condition:"), 2, 3);
        grid.add(whereClausetrg, 2, 4, 2, 1);
//        grid.add(new Label("Having & Order by Condition:"), 0, 6);
        grid.add(new Label("Source Group By Condition:"), 0, 7);
        grid.add(groupBysrc, 0, 8, 2, 1);
        grid.add(new Label("Target Group By Condition:"), 2, 7);
        grid.add(groupBytrg, 2, 8, 2, 1);
        grid.add(new Label("Source Having Condition:"), 0, 11);
        grid.add(havingSrc, 0, 12, 2, 1);
        grid.add(new Label("Target Having Condition:"), 2, 11);
        grid.add(havingTrg, 2, 12, 2, 1);
        grid.add(new Label("Source Order By Condition:"), 0, 15);
        grid.add(orderBySrc, 0, 16, 2, 1);
        grid.add(new Label("Target Order By Condition:"), 2, 15);
        grid.add(orderByTrg, 2, 16, 2, 1);

        msglabel = new Label("Choose Test Case File");
        grid.add(msglabel, 0, 19, 2, 1);

        // Enable/Disable login button depending on whether a username was entered.
        Node loginButton = dialog.getDialogPane().lookupButton(loginButtonType);
        loginButton.setDisable(true);
        test.setDisable(true);

        localfilebt.textProperty().addListener((observable, oldValue, newValue) -> {

            if (!filetypecmb.getValue().toString().trim().isEmpty() && !hosturl.getText().trim().isEmpty() && !jarpath.getText().trim().isEmpty()) {
                //loginButton.setDisable(newValue.trim().isEmpty());
                test.setDisable(newValue.trim().isEmpty());
                loginButton.setDisable(true);
                whereClausesrcStatus.setDisable(false);
                msglabel.setText("Message: Please Test to enable Add button");
            }

        });

        dialog.getDialogPane().setContent(grid);

        //localfilebt
        localfilebt.setOnAction((ActionEvent event) -> {

            String fname = "";
            String fpath = "";
            try {

                Stage mainstage = (Stage) mainvbox.getScene().getWindow();
                file = fileChooser.showOpenDialog(mainstage);
                FileReader fr = null;
                fname = file.getAbsoluteFile().getName();
                fpath = file.getAbsoluteFile().getAbsolutePath();
            } catch (Exception ex) {
                // new ExceptionUI(ex);
            }
            if (file != null) {
                if (file.toString().contains(".xls")) {
//                    msglabel.setStyle("-fx-text-fill: black");
//                    msglabel.setTooltip(new Tooltip(fpath));
                    msglabel.setText("");
                    msgLable.setText("File: " + fname);
                    test.setDisable(false);
                    whereClausesrc.setDisable(false);
                    whereClausetrg.setDisable(false);
                    groupBysrc.setDisable(false);
                    groupBytrg.setDisable(false);
                    havingSrc.setDisable(false);
                    havingTrg.setDisable(false);
                    orderBySrc.setDisable(false);
                    orderByTrg.setDisable(false);
                    dialog.getDialogPane().lookupButton(loginButtonType).setDisable(false);

                } else {
                    msglabel.setStyle("-fx-text-fill: red");
                    msglabel.setText("Message: Please Load Valid the File");
                }
            } else {
                msglabel.setStyle("-fx-text-fill: red");
                msglabel.setText("Message: Please Load the File");
            }

        });
        List fileWhereClause = new ArrayList();
        // Convert the result to a username-password-pair when the login button is clicked.
        dialog.setResultConverter(dialogButton -> {
            if (dialogButton == loginButtonType) {

                fileWhereClause.add(file);
                fileWhereClause.add(whereClausesrc.getText());
                fileWhereClause.add(whereClausetrg.getText());
                fileWhereClause.add(groupBysrc.getText());
                fileWhereClause.add(groupBytrg.getText());
                fileWhereClause.add(havingSrc.getText());
                fileWhereClause.add(havingTrg.getText());
                fileWhereClause.add(orderBySrc.getText());
                fileWhereClause.add(orderByTrg.getText());
                return new Pair<>("", "");
            }
            return null;
        });

        Optional<Pair<String, String>> result = dialog.showAndWait();
        if (fileWhereClause.isEmpty()) {
            return null;
        }

        return fileWhereClause;
    }

    public List<String> readFF(String connPath) throws FileNotFoundException, IOException {
        logger.info("Connection Path: " + connPath);
        File connFile = new File(connPath);
        FileReader fileReader = new FileReader(connFile);
        BufferedReader br = new BufferedReader(fileReader);
        List<String> fileData = new ArrayList();
        String data = null;
        while ((data = br.readLine()) != null) {
            fileData.add(data);
        }

        return fileData;
    }
    boolean radioChk = false;
    boolean fromChk = false;
    boolean toChk = false;

    private void showInrRule(List inrCol, List inrRuleData) {
        incDiaLog = new Dialog<>();
        incDiaLog.setTitle("Incremental Rule Condition");
        System.out.println("Processing UI");
        Stage stage = (Stage) incDiaLog.getDialogPane().getScene().getWindow();

        incOkBut = new ButtonType("OK", ButtonBar.ButtonData.OK_DONE);
        incDiaLog.getDialogPane().getButtonTypes().addAll(incOkBut, ButtonType.CANCEL);
        GridPane grid = new GridPane();
        grid.setHgap(5);
        grid.setVgap(5);
        grid.setPadding(new Insets(20, 10, 10, 10));
        GridPane gridInter = new GridPane();
        gridInter.setHgap(5);
        gridInter.setVgap(5);
        gridInter.setPadding(new Insets(20, 10, 10, 10));
        RadioButton colCheck = null;
        ToggleGroup group = new ToggleGroup();

        int row = 1;
        int col = 0;
        Text gridInterhead = new Text("Incremental Columns ");
        gridInterhead.setFont(javafx.scene.text.Font.font("Times New Roman", 16));
        gridInter.add(gridInterhead, 0, 0);
        gridInter.setStyle("-fx-border: 2px solid; -fx-border-color: black;");
        for (int i = 0; i < inrCol.size(); i++) {
            colCheck = new RadioButton(inrCol.get(i).toString());
            colCheck.setUserData(inrCol.get(i).toString().trim() + ":" + inrCol.get(i + 1).toString().trim());
            colCheck.setToggleGroup(group);
            gridInter.add(colCheck, col++, row);
            i++;
            if (col == 4) {
                col = 0;
                row++;
            }
        }

        LocalDateTimePicker fromDateUI = new LocalDateTimePicker();
        LocalDateTimePicker toDateUI = new LocalDateTimePicker();

        row = 0;
        grid.add(gridInter, 0, row, 3, 1);
        grid.add(new Text(), 0, row++, 3, 1);

        Text from = new Text("From ");
        from.setFont(javafx.scene.text.Font.font("Times New Roman", 16));

        Text to = new Text("To ");
        to.setFont(javafx.scene.text.Font.font("Times New Roman", 16));
        row++;
        grid.add(from, 0, row);
        grid.add(new Separator(Orientation.VERTICAL), 1, row);
        grid.add(to, 2, row);
        row++;
        grid.add(fromDateUI, 0, row);
        grid.add(new Separator(Orientation.VERTICAL), 1, row);
        grid.add(toDateUI, 2, row);
        Button check = new Button("Check");
        grid.add(check, 2, row + 1);
        Node loginButton = incDiaLog.getDialogPane().lookupButton(incOkBut);
        loginButton.setDisable(true);
        incDiaLog.getDialogPane().setContent(grid);

        group.selectedToggleProperty().addListener(new ChangeListener<Toggle>() {
            public void changed(ObservableValue<? extends Toggle> ov, Toggle old_toggle, Toggle new_toggle) {

                if (group.getSelectedToggle() != null) {

                    radioChk = true;

                }

            }
        });

        check.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                if (radioChk == true && fromDateUI.getLocalDateTime() != null && toDateUI.getLocalDateTime() != null) {
                    loginButton.setDisable(false);
                }
            }

        });
        incDiaLog.setResultConverter(dialogButton
                -> {
            if (dialogButton == incOkBut) {
                return new Pair<>("", "");
            }
            return null;
        }
        );
        Optional<Pair<String, String>> result = incDiaLog.showAndWait();

        result.ifPresent(dbconstring
                -> {
            System.out.println("Clicked - Ok Button");

            inrRuleData.add(group.selectedToggleProperty().getValue().getUserData());
            inrRuleData.add(fromDateUI.getLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            inrRuleData.add(toDateUI.getLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        }
        );
    }

}
