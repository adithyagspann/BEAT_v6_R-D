/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved. * */
package beat;

import com.jcraft.jsch.JSchException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableRow;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.DataFormat;
import javafx.scene.input.Dragboard;
import javafx.scene.input.TransferMode;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.util.Pair;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import parser.XMLParser;
import remoteutility.FTPEngine;

/**
 *
 * @author Ravindra
 */
public class FlatFileConnectionUI {

    private static final DataFormat SERIALIZED_MIME_TYPE = new DataFormat("application/x-java-serialized-object");
    private final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FlatFileConnectionUI.class);
// Create the custom dialog.
    private Dialog<Pair<String, String>> dialog;
    private ButtonType loginButtonType;
    private Button test;
    private Button localfilebt;
    private TextField hosturl;
    private TextField username;
    private PasswordField password;
    private ComboBox filetypecmb;
    private TextField jarpath;
    private Connection conn = null;
    private Label msglabel;
    private LoadFlatFilesTreeView lctv;
    private CheckBox remotefilecb;

//file chooser
    final FileChooser fileChooser = new FileChooser();
    File file, fremote;
    private ComboBox hostType;
    private FTPEngine ftpEngine;
    private Button metaProSet;
    private Dialog<Pair<String, String>> metaDataDialog;
    private Button addColumn;
    private ButtonType metaDataButton;
    private Button delColumn;
    private GridPane metaDataGrid;
    private TableView tableView;
    private TextField columnNameTF;
    private ComboBox columnTypeCmb;
    private TextField columnSizeTF;
    private TextField fileExtn;
    private ComboBox<Object> seperator;
    private TextField otherSep;
    private Label otherLabel;
    private ObservableList<FFColMetaData> csvMeta;

    public List<String> readFF(String connPath) throws FileNotFoundException, IOException {
        LOGGER.info("Reading the Flat File data : " + connPath);
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

    FlatFileConnectionUI(LoadFlatFilesTreeView lctv, VBox mainvbox, String connName) {
        LOGGER.info("Building Flat File UI");
        try {
            this.lctv = lctv;

            dialog = new Dialog<>();

            dialog.setTitle("FlatFile Connection");
            dialog.setHeaderText("Add the Flat File Connection");

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

            localfilebt = new Button();
            localfilebt.setText("Local File");
            localfilebt.setMaxWidth(150);
            remotefilecb = new CheckBox("Remote File");
            //remotefilecb.setDisable(true);
            hosturl = new TextField();
            hosturl.setPromptText("Enter the FTP / SFTP server address ");
            hosturl.setDisable(true);
            hostType = new ComboBox();
            hostType.setPromptText("Choose Remote Type");

            hostType.setItems(getRemoteTypes());

            hostType.setDisable(true);
            username = new TextField();
            username.setPromptText("Username");
            username.setDisable(true);
            password = new PasswordField();
            password.setPromptText("Password");
            password.setDisable(true);
            jarpath = new TextField();
            jarpath.setPromptText("Please Enter Remote path : /home/myfolder/file.csv");
            jarpath.setDisable(true);
            filetypecmb = new ComboBox();
            filetypecmb.setPromptText("Choose File Type");
//            filetypecmb.setDisable(true);
            ObservableList ftypelist = FXCollections.observableArrayList();
//            ftypelist.add("TXT");
            ftypelist.add("CSV");

//            ftypelist.add("XLS");
//            ftypelist.add("XLSX");
            ftypelist.add("JSON");
            ftypelist.add("XML");
            ftypelist.add("Others");
            filetypecmb.setItems((ObservableList) ftypelist);
            metaProSet = new Button("Set MetaData");
            metaProSet.setDisable(true);
            grid.add(new Label("Choose:"), 0, 0);
            grid.add(remotefilecb, 1, 0);
            grid.add(localfilebt, 2, 0);
            grid.add(new Label("Host URL:"), 0, 1);
            grid.add(hosturl, 1, 1, 2, 1);
            grid.add(new Label("port:"), 0, 2);
            grid.add(hostType, 1, 2, 2, 1);
            grid.add(new Label("UserName:"), 0, 3);
            grid.add(username, 1, 3, 2, 1);
            grid.add(new Label("Password:"), 0, 4);
            grid.add(password, 1, 4, 2, 1);
            grid.add(new Label("Choose File type: "), 0, 5);
            grid.add(filetypecmb, 1, 5, 2, 1);
            setTableMeta("");
//            metaDataGrid.setVisible(false);
//            gridCmb.add(metaDataGrid, 1, 0);
            grid.add(metaDataGrid, 1, 6, 2, 1);
            grid.add(new Label("Remote Path:"), 0, 7);
            grid.add(jarpath, 1, 7, 2, 1);
            msglabel = new Label("Message: Please Test to enable Add button");
            grid.add(msglabel, 0, 8, 2, 1);
            grid.add(test, 2, 8);

            // Enable/Disable login button depending on whether a username was entered.
            Node loginButton = dialog.getDialogPane().lookupButton(loginButtonType);
            loginButton.setDisable(true);
            test.setDisable(true);
            if (!connName.isEmpty()) {
                LOGGER.info("Loading existing FF " + connName + " for modifications");
                List<String> connData = readFF("files/" + connName.substring(connName.lastIndexOf(".") + 1, connName.length()) + "/" + connName);

                switch (connData.get(0).toLowerCase()) {
                    case "local":
                        if (connData.size() == 2) {
                            jarpath.setText(connData.get(1));
                            filetypecmb.setValue(connName.substring(connName.lastIndexOf(".") + 1, connName.length()).toUpperCase());
                            jarpath.setDisable(false);
                        } else {
                            jarpath.setText(connData.get(1));
                            filetypecmb.setValue(connName.substring(connName.lastIndexOf(".") + 1, connName.length()).toUpperCase());

                            jarpath.setDisable(false);
                        }
                        break;
                    case "remote":
                        break;
                }

                if (connData.size() == 2) {
                    jarpath.setText(connData.get(1));
                    filetypecmb.setValue(connName.substring(connName.lastIndexOf(".") + 1, connName.length()).toUpperCase());
                    jarpath.setDisable(false);
                } else if (connData.size() == 3) {

                } else {
                    remotefilecb.setSelected(true);
                    hosturl.setText(connData.get(2));
                    hosturl.setDisable(false);
                    hostType.setValue(connData.get(1));
                    hostType.setDisable(false);
                    username.setText(connData.get(4));
                    username.setDisable(false);
                    password.setText(connData.get(5));
                    password.setDisable(false);
                    filetypecmb.setValue(connName.substring(connName.lastIndexOf(".") + 1, connName.length()).toUpperCase());
                    jarpath.setText(connData.get(3));
                    jarpath.setDisable(false);
                }
                test.setDisable(false);
            }
//            else {
//                LOGGER.info("Creating new FF ");
//                filetypecmb.valueProperty().addListener((observable, oldValue, newValue) -> {
//                    
//                    if (filetypecmb.getValue().toString().equals("CSV")) {
//                        test.setDisable(false);
//                    } else {
//                        metaProSet.setDisable(false);
//                        setTableMeta();
//                    }
//
////                    if ((filetypecmb.getValue().toString().equals("TXT") || filetypecmb.getValue().toString().equals("CSV") || filetypecmb.getValue().toString().equals("JSON") || filetypecmb.getValue().toString().equals("XLSX") || filetypecmb.getValue().toString().equals("XML") || filetypecmb.getValue().toString().equals("XLS"))) {
////                        test.setDisable(false);
////                    } else {
////                        test.setDisable(true);
////                    }
//                });
//            }
            // Do some validation (using the Java 8 lambda syntax).
            remotefilecb.selectedProperty().addListener((observable, oldValue, newValue) -> {

                if (remotefilecb.isSelected()) {
                    hosturl.setDisable(false);
                    username.setDisable(false);
                    hostType.setDisable(false);
                    password.setDisable(false);
                    jarpath.setDisable(false);
                } else {
                    hosturl.setDisable(true);
                    username.setDisable(true);
                    hostType.setDisable(true);
                    password.setDisable(true);
                    jarpath.setDisable(true);
                }

            });

            localfilebt.textProperty().addListener((observable, oldValue, newValue) -> {

                if (!filetypecmb.getValue().toString().trim().isEmpty() && !hosturl.getText().trim().isEmpty() && !jarpath.getText().trim().isEmpty()) {
                    //loginButton.setDisable(newValue.trim().isEmpty());

                    test.setDisable(newValue.trim().isEmpty());
                    loginButton.setDisable(true);
                    msglabel.setText("Message: Please Test to enable Add button");
                }

            });

            hosturl.textProperty().addListener((observable, oldValue, newValue) -> {
                System.out.println("Get: " + filetypecmb.getValue().toString());

                if (!localfilebt.getText().trim().isEmpty() && !filetypecmb.getValue().toString().trim().isEmpty() && !jarpath.getText().trim().isEmpty()) {
                    //loginButton.setDisable(newValue.trim().isEmpty());
                    test.setDisable(newValue.trim().isEmpty());
                    loginButton.setDisable(true);
                    msglabel.setText("Message: Please Test to enable Add button");
                }

            });

            jarpath.textProperty().addListener((observable, oldValue, newValue) -> {

                if (!hosturl.getText().trim().isEmpty() && !filetypecmb.getValue().toString().trim().isEmpty()) {
                    //loginButton.setDisable(newValue.trim().isEmpty());
                    test.setDisable(newValue.trim().isEmpty());
                    loginButton.setDisable(true);
                    msglabel.setText("Message: Please Test to enable Add button");
                }

            });

            dialog.getDialogPane().setContent(grid);

            //localfilebt
            localfilebt.setOnAction((ActionEvent event) -> {
                LOGGER.info("Loading the local file");
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
                    msglabel.setStyle("-fx-text-fill: black");
                    msglabel.setTooltip(new Tooltip(fpath));
                    msglabel.setText("Message: " + fname);
                } else {
                    msglabel.setStyle("-fx-text-fill: red");
                    msglabel.setText("Message: Unmatched File Format");
                }

            });
            metaProSet.setOnAction((ActionEvent event) -> {
                LOGGER.info("Processing to set meta properties");
                setTableMeta(filetypecmb.getValue().toString());
            });
            //test file connection
            test.setOnAction((ActionEvent event) -> {
                LOGGER.info("Testing the Loaded file using File type");
                try {

                    if (remotefilecb.isSelected()) {
                        //Remote File Checking
                        if (checkFTPfile() == true) {
                            loginButton.setDisable(false);
                        } else {
                            loginButton.setDisable(true);
                        }
                    } else {

                        if (file == null) {
                            file = new File(jarpath.getText());
                        }
                        String ext = file.getAbsolutePath().substring(file.getAbsolutePath().lastIndexOf("."), file.getAbsolutePath().length());
//                        if (file.isFile() && (file.getAbsolutePath().toLowerCase().contains(filetypecmb.getValue().toString().toLowerCase()) || file.getAbsolutePath().toLowerCase().contains(fileExtn.getText().toLowerCase()))) {
                        if (file.isFile() && (ext.equalsIgnoreCase(filetypecmb.getValue().toString().toLowerCase()) || ext.equalsIgnoreCase(fileExtn.getText().toLowerCase()))) {
                            boolean dataCheck = false;
                            if (filetypecmb.getValue().toString().equalsIgnoreCase("csv")) {
                                dataCheck = true;
                            } else {
                                if (!tableView.getItems().isEmpty()) {
                                    dataCheck = true;
                                }
                            }

                            LOGGER.info("File has been verified with file type successfully");
                            if (dataCheck) {
                                msglabel.setStyle("-fx-text-fill: green");
                                msglabel.setTooltip(new Tooltip(file.getAbsolutePath()));
                                msglabel.setText("Message: Format Matched ..." + file.getName());
                                loginButton.setDisable(false);
                            } else {
                                LOGGER.info("File has failed while verifing with file type ");
                                msglabel.setStyle("-fx-text-fill: red");
                                msglabel.setText("Message: Unmatched File Format");
                            }
                        } else {
                            LOGGER.info("File has failed while verifing with file type ");
                            msglabel.setStyle("-fx-text-fill: red");
                            msglabel.setText("Message: Unmatched File Format");
                        }
                    }
                } catch (Exception ex) {
                    LOGGER.error(ex.toString());
                    new ExceptionUI(ex);
                }

            });

            filetypecmb.valueProperty().addListener((observable, oldValue, newValue) -> {

                if (filetypecmb.getValue().toString().equalsIgnoreCase("CSV")) {
                    test.setDisable(false);

                    metaDataGrid.setDisable(true);
                } else if (filetypecmb.getValue().toString().equalsIgnoreCase("XML") || filetypecmb.getValue().toString().equalsIgnoreCase("JSON")) {
                    metaDataGrid.setDisable(true);
                } else {
                    test.setDisable(false);

                    metaDataGrid.setDisable(false);

                }
//                if ((filetypecmb.getValue().toString().equals("TXT") || filetypecmb.getValue().toString().equals("CSV") || filetypecmb.getValue().toString().equals("JSON") || filetypecmb.getValue().toString().equals("XLSX") || filetypecmb.getValue().toString().equals("XML") || filetypecmb.getValue().toString().equals("XLS"))) {
//                    test.setDisable(false);
//                } else {
//                    test.setDisable(true);
//                }

            });

            // Convert the result to a username-password-pair when the login button is clicked.
            dialog.setResultConverter(dialogButton -> {
                if (dialogButton == loginButtonType) {
                    return new Pair<>(hosturl.getText(), username.getText() + ";;" + password.getText());
                }
                return null;
            });

            Optional<Pair<String, String>> result = dialog.showAndWait();

            result.ifPresent(dbconstring -> {
                System.out.println("Clicked - Add Button");
                String schemaFile = "";
                try {
                    if (!filetypecmb.getValue().toString().equalsIgnoreCase("csv")) {
                        if (remotefilecb.isSelected()) {

                            File rfile = new File(jarpath.getText());
                            schemaFile = storeMetaData(rfile.getName());

                        } else {
                            schemaFile = storeMetaData(file.getName());
                        }
                    }

                    String fileType = "";
                    if (filetypecmb.getValue().toString().equalsIgnoreCase("csv")) {
                        fileType = filetypecmb.getValue().toString();
                    } else if (filetypecmb.getValue().toString().equalsIgnoreCase("xml") || filetypecmb.getValue().toString().equalsIgnoreCase("json")) {

                    } else {
                        fileType = fileExtn.getText().replace(".", "");
                    }

                    if (remotefilecb.isSelected()) {

                        File rfile = new File(jarpath.getText());
                        LOGGER.info("Storing/Updating Remote Flat File: " + rfile.getAbsolutePath());
                        new SaveFF(rfile.getName(), hostType.getSelectionModel().getSelectedItem().toString(), hosturl.getText(), username.getText(), password.getText(), jarpath.getText(), fileType, schemaFile, lctv);

                    } else {
                        System.out.println("File Name: " + file.getAbsolutePath());
                        LOGGER.info("Storing/Updating Local Flat File: " + file.getAbsolutePath());
                        new SaveFF(file.getName(), file.getAbsolutePath().replace("\\", "/"), fileType, schemaFile, lctv);
                    }
                } catch (ParserConfigurationException | SAXException | TransformerException | IOException ex) {
                    Logger.getLogger(FlatFileConnectionUI.class.getName()).log(Level.SEVERE, null, ex);
                    new ExceptionUI(ex);
                }
            });
        } catch (IOException ex) {
//            Logger.getLogger(FlatFileConnectionUI.class.getName()).log(Level.SEVERE, null, ex);
            LOGGER.info(ex.toString());
            new ExceptionUI(ex);
        }

    }

    private boolean checkFTPfile() throws JSchException, IOException {
        LOGGER.info("Checking FTP File " + jarpath.getText() + " on server " + hosturl.getText());
        if (hostType.getSelectionModel().getSelectedItem().toString().equalsIgnoreCase("ftp")) {

            ftpEngine = new FTPEngine(hosturl.getText().trim(), 21, username.getText().trim(), password.getText().trim());
            if (ftpEngine.checkFileExists(jarpath.getText().trim())) {
                LOGGER.info("File is available on FTP server. Processing for further steps");
                File rfilePath = new File(jarpath.getText());
                if (rfilePath.getAbsolutePath().contains(filetypecmb.getValue().toString().toLowerCase())) {
                    LOGGER.info("Format has been matched");
                    msglabel.setStyle("-fx-text-fill: green");
                    msglabel.setTooltip(new Tooltip(rfilePath.getAbsolutePath()));
                    msglabel.setText("Message: Format Matched ..." + rfilePath.getName());
//                        loginButton.setDisable(false);
                    return true;
                } else {
                    LOGGER.info("Format has not been matched");
                    msglabel.setStyle("-fx-text-fill: red");
                    msglabel.setText("Message: Unmatched File Format");
                }
            } else {
                LOGGER.info("File doesn`t exists");
                msglabel.setStyle("-fx-text-fill: red");
                msglabel.setText("Message: File does not exists");
            }

        } else if (hostType.getSelectionModel().getSelectedItem().toString().equalsIgnoreCase("sftp")) {
            ftpEngine = new FTPEngine(hosturl.getText().trim(), 22, username.getText().trim(), password.getText().trim());
            if (ftpEngine.checkFileExists(jarpath.getText().trim())) {
                LOGGER.info("File is available on FTP server. Processing for further steps");
                File rfilePath = new File(jarpath.getText());
                if (rfilePath.getAbsolutePath().contains(filetypecmb.getValue().toString().toLowerCase())) {
                    LOGGER.info("Format has been matched");
                    msglabel.setStyle("-fx-text-fill: green");
                    msglabel.setTooltip(new Tooltip(rfilePath.getAbsolutePath()));
                    msglabel.setText("Message: Format Matched ..." + rfilePath.getName());
//                        loginButton.setDisable(false);
                    return true;
                } else {
                    LOGGER.info("Format has not been matched");
                    msglabel.setStyle("-fx-text-fill: red");
                    msglabel.setText("Message: Unmatched File Format");
                }
            } else {
                LOGGER.info("File doesn`t exists");
                msglabel.setStyle("-fx-text-fill: red");
                msglabel.setText("Message: File does not exists");
            }
        } else {
            LOGGER.warn("Host Connection type has not been selected");
            new AlertUI("Select Host Connection Type");
        }
        return false;
    }

    private ObservableList getRemoteTypes() throws FileNotFoundException, IOException {
        LOGGER.info("Getting RemoteTypes acceptable by application");
        FileReader fr = null;
        File file = new File("remotetypes");
        fr = new FileReader(file.getAbsoluteFile());
        BufferedReader br = new BufferedReader(fr);
        String line = null;

        ObservableList remoteTypes = FXCollections.observableArrayList();

        while ((line = br.readLine()) != null) {
            remoteTypes.add(line.toUpperCase());
        }

        return remoteTypes;
    }

    public void setTableMeta(String extType) {
//        metaDataDialog = new Dialog<>();
//        metaDataDialog.setTitle("Flat File Metdata");
//        metaDataDialog.setHeaderText("Add the Flat File Metadata");
//
//        // Get the Stage.
//        Stage stage = (Stage) metaDataDialog.getDialogPane().getScene().getWindow();
//
//        // Add a custom icon.
////        stage.getIcons().add(new Image(this.getClass().getResource("/icon/filesicon.png").toString()));
//        metaDataButton = new ButtonType("ADD", ButtonBar.ButtonData.OK_DONE);
//
//        metaDataDialog.getDialogPane().getButtonTypes().addAll(metaDataButton, ButtonType.CANCEL);
//
//        // Set the icon (must be included in the project).
//        metaDataDialog.setGraphic(new ImageView(new Image(getClass().getResourceAsStream("/icon/filesicon.png"))));
        metaDataGrid = new GridPane();
        metaDataGrid.setHgap(5);
        metaDataGrid.setVgap(5);
        metaDataGrid.setPadding(new Insets(20, 10, 10, 10));
        metaDataGrid.setDisable(true);
        //Table to store the Columns Metadata
        tableView = new TableView();
        tableView.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        tableView.setPrefHeight(150);
        tableView.setPrefWidth(400);
        TableColumn columnName = new TableColumn("Column Name");
//        columnName.setPrefWidth(120);
        TableColumn columnType = new TableColumn("Column Type");
//        columnType.setPrefWidth(120);
        TableColumn columnSize = new TableColumn("Column Size");
//        columnSize.setPrefWidth(120);

        columnName.setCellValueFactory(new PropertyValueFactory<>("columnName"));
        columnType.setCellValueFactory(new PropertyValueFactory<>("colType"));
        columnSize.setCellValueFactory(new PropertyValueFactory<>("colSize"));

        tableView.getColumns().addAll(columnName, columnType, columnSize);

        csvMeta = FXCollections.observableArrayList();

        tableView.setItems(csvMeta);

        columnNameTF = new TextField();
        columnNameTF.setPromptText("Column Name");
        columnTypeCmb = new ComboBox();
        columnTypeCmb.setPromptText("Choose Data type");

        ObservableList<String> colTypeList = FXCollections.observableArrayList();
        colTypeList.add("Integer");
        colTypeList.add("Long");
        colTypeList.add("Float");
        colTypeList.add("Double");
        colTypeList.add("BigDecimal");
        colTypeList.add("String");
        colTypeList.add("Date");

        colTypeList.add("Time");
        colTypeList.add("Boolean");
        columnTypeCmb.getItems().addAll(colTypeList);
        columnSizeTF = new TextField();
        columnSizeTF.setPromptText("Column Size");
        columnNameTF.setDisable(true);
        columnTypeCmb.setDisable(true);
        columnSizeTF.setDisable(true);
        otherSep = new TextField();
        otherSep.setPromptText("Enter the Column Seperator");
//        otherSep.setDisable(true);
        otherSep.setVisible(false);

        addColumn = new Button("Add");
        delColumn = new Button("Remove");
        fileExtn = new TextField();
        fileExtn.setPromptText("Enter file extn eg: .txt");
//        fileExtn.setDisable(true);
        seperator = new ComboBox<>();
        seperator.setDisable(true);

        ObservableList<String> seperatorCmb = FXCollections.observableArrayList();
        seperatorCmb.add(",");
        seperatorCmb.add("|");
        seperatorCmb.add("Fixed");
        seperatorCmb.add("Others");
        seperator.getItems().addAll(seperatorCmb);

        seperator.setPromptText("Choose Column Seperator");
//        tableView.setDisable(true);
//        columnNameTF.setDisable(true);
//        columnSizeTF.setDisable(true);
//        columnTypeCmb.setDisable(true);
        metaDataGrid.add(new Label("File Extension"), 0, 0);
        metaDataGrid.add(fileExtn, 1, 0);
        metaDataGrid.add(new Label("Column Seperator"), 0, 1);
        metaDataGrid.add(seperator, 1, 1);
        otherLabel = new Label("Others");
        otherLabel.setVisible(false);
//        metaDataGrid.add(otherLabel, 2, 1);
        metaDataGrid.add(otherSep, 2, 1);
        metaDataGrid.add(tableView, 0, 2, 3, 1);
        metaDataGrid.add(new Label("Column Name"), 0, 3);
        metaDataGrid.add(columnNameTF, 1, 3);
        metaDataGrid.add(new Label("Column Type"), 0, 4);
        metaDataGrid.add(columnTypeCmb, 1, 4);
        metaDataGrid.add(new Label("Column Size"), 0, 5);
        metaDataGrid.add(columnSizeTF, 1, 5);
//        metaDataGrid.add
        metaDataGrid.add(addColumn, 0, 6);
        metaDataGrid.add(delColumn, 1, 6);

        //Seperator Combo Action
        seperator.valueProperty().addListener((observable, oldValue, newValue) -> {
            if (seperator.getValue().toString().equalsIgnoreCase("others")) {
                otherSep.setVisible(true);
                otherLabel.setVisible(true);
                columnNameTF.setDisable(true);
                columnTypeCmb.setDisable(true);
                columnSizeTF.setDisable(true);
                columnSizeTF.clear();
            } else if (seperator.getValue().toString().equalsIgnoreCase("fixed")) {
                otherSep.setVisible(false);
                otherSep.clear();
                otherLabel.setVisible(false);
                columnNameTF.setDisable(false);
                columnTypeCmb.setDisable(false);
                columnSizeTF.setDisable(false);
            } else {
                otherSep.setVisible(false);
                otherSep.clear();
                otherLabel.setVisible(false);
                columnNameTF.setDisable(false);
                columnTypeCmb.setDisable(false);
                columnSizeTF.setDisable(true);
                columnSizeTF.clear();
            }

        });

        //Action on file extension
        fileExtn.textProperty().addListener((observable, oldValue, newValue) -> {

            if (!fileExtn.getText().trim().isEmpty()) {
                seperator.setDisable(false);
            } else {
                seperator.setDisable(true);
            }
        });

        otherSep.textProperty().addListener((observable, oldValue, newValue) -> {
            if (!otherSep.getText().isEmpty()) {
                columnNameTF.setDisable(false);
                columnTypeCmb.setDisable(false);
                columnSizeTF.setDisable(true);
            } else {
                columnNameTF.setDisable(true);
                columnTypeCmb.setDisable(true);
                columnSizeTF.setDisable(true);
            }
        });
        //Adding new Column
        addColumn.setOnAction((ActionEvent event) -> {
            System.out.println("Adding Column");
            if (seperator.getValue().toString().equalsIgnoreCase("fixed")) {
                if (!columnNameTF.getText().isEmpty() && columnTypeCmb != null && !columnSizeTF.getText().isEmpty()) {
                    boolean dataNotExist = checkColumnExistence();

                    for (int i = 0; i < csvMeta.size(); i++) {
                        System.out.println(csvMeta.get(i).getColumnName() + "#" + csvMeta.get(i).getColType() + "#" + csvMeta.get(i).getColSize());
                    }
                    if (dataNotExist) {
                        FFColMetaData metaData = new FFColMetaData(columnNameTF.getText(), columnTypeCmb.getValue().toString(), columnSizeTF.getText());
                        csvMeta.add(metaData);
                    }
                } else {
                    int i = 0;
                    StringBuilder builder = new StringBuilder();
                    if (columnNameTF.getText().isEmpty()) {
                        builder.append(++i).append(". Column Name Cannot be empty");
                    }
                    if (columnTypeCmb.getValue().toString().isEmpty()) {
                        builder.append(++i).append(". Column Type Cannot be empty");
                    }
                    if (columnSizeTF.getText().isEmpty()) {
                        builder.append(++i).append(". Column Size Cannot be empty");
                    }
                    new ExceptionUI(new Exception(builder.toString()));
                }
            } else {
                System.out.println("Getting Delimited");
                if (!columnNameTF.getText().isEmpty() && !columnTypeCmb.getValue().toString().isEmpty()) {
                    boolean dataNotExist = checkColumnExistence();

                    if (dataNotExist) {
                        FFColMetaData metaData = new FFColMetaData(columnNameTF.getText(), columnTypeCmb.getValue().toString(), columnSizeTF.getText());

                        csvMeta.add(metaData);
                    }
                } else {
                    int i = 0;
                    StringBuilder builder = new StringBuilder();
                    if (columnNameTF.getText().isEmpty()) {
                        builder.append(++i).append(". Column Name Cannot be empty");
                    }
                    if (columnTypeCmb.getValue().toString().isEmpty()) {
                        builder.append(++i).append(". Column Type Cannot be empty");
                    }

                    new ExceptionUI(new Exception(builder.toString()));
                }
            }
        });

        delColumn.setOnAction((ActionEvent event) -> {
            FFColMetaData p = (FFColMetaData) tableView.getSelectionModel().getSelectedItem();
            csvMeta.remove(p);
        });

        //Drag & Drop on Table View
        tableView.getSelectionModel().selectedItemProperty().addListener(new ChangeListener() {
            @Override
            public void changed(ObservableValue observable, Object oldValue, Object newValue) {
                FFColMetaData colMeta = (FFColMetaData) newValue;

                if (colMeta != null) {
                    columnNameTF.setText(colMeta.getColumnName());
                    columnTypeCmb.setValue(colMeta.getColType());
                    columnSizeTF.setText(colMeta.getColSize());
                }
            }
        });
        tableView.setRowFactory(tv -> {
            TableRow<FFColMetaData> row = new TableRow<>();

            row.setOnDragDetected(event -> {
                if (!row.isEmpty()) {
                    Integer index = row.getIndex();
                    Dragboard db = row.startDragAndDrop(TransferMode.MOVE);
                    db.setDragView(row.snapshot(null, null));
                    ClipboardContent cc = new ClipboardContent();
                    cc.put(SERIALIZED_MIME_TYPE, index);
                    db.setContent(cc);
                    event.consume();
                }
            });

            row.setOnDragOver(event -> {
                Dragboard db = event.getDragboard();
                if (db.hasContent(SERIALIZED_MIME_TYPE)) {
                    if (row.getIndex() != ((Integer) db.getContent(SERIALIZED_MIME_TYPE)).intValue()) {
                        event.acceptTransferModes(TransferMode.COPY_OR_MOVE);
                        event.consume();
                    }
                }
            });

            row.setOnDragDropped(event -> {
                Dragboard db = event.getDragboard();
                if (db.hasContent(SERIALIZED_MIME_TYPE)) {
                    int draggedIndex = (Integer) db.getContent(SERIALIZED_MIME_TYPE);
//                    TableViewDragRows.Person draggedPerson = tableView.getItems().remove(draggedIndex);
                    FFColMetaData draggedPerson = (FFColMetaData) tableView.getItems().remove(draggedIndex);

                    int dropIndex;

                    if (row.isEmpty()) {
                        dropIndex = tableView.getItems().size();
                    } else {
                        dropIndex = row.getIndex();
                    }

                    tableView.getItems().add(dropIndex, draggedPerson);

                    event.setDropCompleted(true);
                    tableView.getSelectionModel().select(dropIndex);
                    event.consume();
                }
            });

            return row;
        });

//        metaDataDialog.getDialogPane().setContent(metaDataGrid);
//
//        metaDataDialog.setResultConverter(dialogButton -> {
//            if (dialogButton == metaDataButton) {
//                return new Pair<>("", "");
//            }
//            return null;
//        });
//
//        Optional<Pair<String, String>> result = metaDataDialog.showAndWait();
//
//        result.ifPresent(dbconstring -> {
//            System.out.println("Clicked - Add Button");
//
//        });
    }

    private boolean checkColumnExistence() {
        boolean dataNotExist = true;
        for (int i = 0; i < csvMeta.size(); i++) {
            if (csvMeta.get(i).getColumnName().equalsIgnoreCase(columnNameTF.getText())) {
                LOGGER.info("Updating the existing Column");
                FFColMetaData metaData = csvMeta.get(i);
                metaData.setColSize(columnSizeTF.getText());

                metaData.setColType(columnTypeCmb.getValue().toString());
                metaData.setColumnName(columnNameTF.getText());
                csvMeta.remove(i);
                csvMeta.add(i, metaData);
                dataNotExist = false;
                break;
            }
        }
        return dataNotExist;
    }

    private String storeMetaData(String fileName) throws ParserConfigurationException, SAXException, TransformerException, IOException {
        List<List<String>> metaAttrStore = new ArrayList<>();
        List<String> tableMetaAttrStore = new ArrayList<>();
        StringBuffer tableMetaAttr = new StringBuffer();
        tableMetaAttr.append("'name'").append("#").append("'").append(fileName).append("'").append("\n");
        tableMetaAttrStore.add(tableMetaAttr.toString());
        tableMetaAttr = new StringBuffer();
        tableMetaAttr.append("'separator'").append("#").append("'").append(seperator.equals("others") ? otherSep.getText() : seperator.getValue().toString().toLowerCase()).append("'").append("\n");
        tableMetaAttrStore.add(tableMetaAttr.toString());
        tableMetaAttr = new StringBuffer();
        tableMetaAttr.append("'suppressHeaders'").append("#").append("'true'");
        tableMetaAttrStore.add(tableMetaAttr.toString());
        metaAttrStore.add(tableMetaAttrStore);
        List<String> tableColMeta = new ArrayList<>();
        int size = 0;
        int prevIndex = 0;
        for (int i = 0; i < tableView.getItems().size(); i++) {
            FFColMetaData metaData = (FFColMetaData) tableView.getItems().get(i);
            if (metaData.getColSize().isEmpty()) {
                tableColMeta.add(tableView.getItems().get(i).toString());
            } else {

                size += Integer.parseInt(metaData.getColSize());
                if (i == 0) {
                    metaData.setBegin(1);
                    metaData.setEnd(Integer.parseInt(metaData.getColSize()));
                } else if (i == tableView.getItems().size() - 1) {

                    metaData.setBegin(prevIndex + 1);
                    metaData.setEnd(size);
                } else {
                    metaData.setBegin(prevIndex + 1);
                    metaData.setEnd(size + (Integer.parseInt(metaData.getColSize())));
                }
                prevIndex = Integer.parseInt(metaData.getColSize());
                System.out.println("Data Begin: " + metaData.getBegin() + ": end: " + metaData.getEnd());
                tableColMeta.add(metaData.getFixedLenMeta());
            }

        }
        metaAttrStore.add(tableColMeta);
        if (!metaAttrStore.isEmpty() && metaAttrStore.size() == 2) {
            String filePath = new File(System.getProperty("user.dir")).getPath() + "/files/schema.xml";
            XMLParser xMLParser = new XMLParser();
            xMLParser.xmlAttributesWriter(filePath, "schema", "table", "column", metaAttrStore);
            return filePath;
        }
        return "";
    }
}
