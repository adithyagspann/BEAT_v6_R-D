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
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.util.Pair;
import org.slf4j.LoggerFactory;
import remoteutility.FTPEngine;

/**
 *
 * @author Ravindra
 */
public class FlatFileConnectionUI {

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
            ObservableList ftypelist = FXCollections.observableArrayList();
            ftypelist.add("TXT");
            ftypelist.add("CSV");
            ftypelist.add("XLS");
            ftypelist.add("XLSX");
            ftypelist.add("JSON");
            ftypelist.add("XML");
            filetypecmb.setItems((ObservableList) ftypelist);

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
            grid.add(new Label("Choose File type:"), 0, 5);
            grid.add(filetypecmb, 1, 5, 2, 1);
            grid.add(new Label("Remote Path:"), 0, 6);
            grid.add(jarpath, 1, 6, 2, 1);
            msglabel = new Label("Message: Please Test to enable Add button");
            grid.add(msglabel, 0, 7, 2, 1);
            grid.add(test, 2, 7);

            // Enable/Disable login button depending on whether a username was entered.
            Node loginButton = dialog.getDialogPane().lookupButton(loginButtonType);
            loginButton.setDisable(true);
            test.setDisable(true);
            if (!connName.isEmpty()) {
                LOGGER.info("Loading existing FF " + connName + " for modifications");
                List<String> connData = readFF("files/" + connName.substring(connName.lastIndexOf(".") + 1, connName.length()) + "/" + connName);

                if (connData.size() == 2) {
                    jarpath.setText(connData.get(1));
                    filetypecmb.setValue(connName.substring(connName.lastIndexOf(".") + 1, connName.length()).toUpperCase());
                    jarpath.setDisable(false);
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

            } else {
                LOGGER.info("Creating new FF ");
                filetypecmb.valueProperty().addListener((observable, oldValue, newValue) -> {

                    if ((filetypecmb.getValue().toString().equals("TXT") || filetypecmb.getValue().toString().equals("CSV") || filetypecmb.getValue().toString().equals("JSON") || filetypecmb.getValue().toString().equals("XLSX") || filetypecmb.getValue().toString().equals("XML") || filetypecmb.getValue().toString().equals("XLS"))) {
                        test.setDisable(false);
                    } else {
                        test.setDisable(true);
                    }

                });
            }
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

                        if (file.isFile() && file.getAbsolutePath().contains(filetypecmb.getValue().toString())) {
                            LOGGER.info("File has been verified with file type successfully");

                            msglabel.setStyle("-fx-text-fill: green");
                            msglabel.setTooltip(new Tooltip(file.getAbsolutePath()));
                            msglabel.setText("Message: Format Matched ..." + file.getName());
                            loginButton.setDisable(false);
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

                if ((filetypecmb.getValue().toString().equals("TXT") || filetypecmb.getValue().toString().equals("CSV") || filetypecmb.getValue().toString().equals("JSON") || filetypecmb.getValue().toString().equals("XLSX") || filetypecmb.getValue().toString().equals("XML") || filetypecmb.getValue().toString().equals("XLS"))) {
                    test.setDisable(false);
                } else {
                    test.setDisable(true);
                }

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

                if (remotefilecb.isSelected()) {

                    File rfile = new File(jarpath.getText());
                    LOGGER.info("Storing/Updating Remote Flat File: " + rfile.getAbsolutePath());
                    new SaveFF(rfile.getName(), hostType.getSelectionModel().getSelectedItem().toString(), hosturl.getText(), username.getText(), password.getText(), jarpath.getText(), filetypecmb.getValue().toString(), lctv);

                } else {
                    System.out.println("File Name: " + file.getAbsolutePath());
                    LOGGER.info("Storing/Updating Local Flat File: " + file.getAbsolutePath());
                    new SaveFF(file.getName(), file.getAbsolutePath().replace("\\", "/"), filetypecmb.getValue().toString(), lctv);
                }

            });
        } catch (IOException ex) {
            Logger.getLogger(FlatFileConnectionUI.class.getName()).log(Level.SEVERE, null, ex);
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

}
