/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved.* */
package beat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonBar.ButtonData;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.GridPane;
import javafx.stage.Stage;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ravindra
 */
public class AddDBConnectionUI {

// Create the custom dialog.
    private Dialog<Pair<String, String>> dialog;
    private ButtonType loginButtonType;
    private Button test;
    private TextField dbname;
    private TextField hosturl;
    private TextField username;
    private PasswordField password;
    private TextField driverclass;
    private TextField jarpath;
    private Connection conn = null;
    private Label msglabel;
    private ComboBox databasetype;
    private LoadConnectionsTreeView lctv;
    private final static Logger logger = LoggerFactory.getLogger(AddDBConnectionUI.class);
    
    AddDBConnectionUI(LoadConnectionsTreeView lctv, String connName) throws IOException {
        logger.info("Building DB Connection UI");
        this.lctv = lctv;
        
        dialog = new Dialog<>();
        
        dialog.setTitle("DB Connection");
        dialog.setHeaderText("Add the DB Connection");

        // Get the Stage.
        Stage stage = (Stage) dialog.getDialogPane().getScene().getWindow();

        // Add a custom icon.
        stage.getIcons().add(new Image(this.getClass().getResource("/icon/dbadd.png").toString()));

        // Set the icon (must be included in the project).
        dialog.setGraphic(new ImageView(new Image(getClass().getResourceAsStream("/icon/dbadd.png"))));

        // Set the button types.
        loginButtonType = new ButtonType("ADD", ButtonData.OK_DONE);
        test = new Button("Test");
        test.setMinWidth(100);
        dialog.getDialogPane().getButtonTypes().addAll(loginButtonType, ButtonType.CANCEL);
        // Create the username and password labels and fields.
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20, 30, 10, 10));
        
        databasetype = new ComboBox(getDBTypeList());
        databasetype.setMaxWidth(300);
        databasetype.setPromptText("Select Database Type");
        dbname = new TextField();
        dbname.setMinWidth(300);
        dbname.setPromptText("Name the Connection");
        hosturl = new TextField();
        hosturl.setPromptText("Enter the JDBC url with port address to connect");
        username = new TextField();
        username.setPromptText("Username");
        password = new PasswordField();
        password.setPromptText("Password");
        jarpath = new TextField();
        jarpath.setPromptText("Please enter the path of jar");
        driverclass = new TextField();
        driverclass.setPromptText("Please Enter drive class : com.mysql.jdbc.Driver");
        
        int x = 0, y = 0;
        
        grid.add(new Label("Select Database:"), x, y);
        grid.add(databasetype, x + 1, y);
        grid.add(new Label("Connection Name:"), x, y + 1);
        grid.add(dbname, x + 1, y + 1);
        grid.add(new Label("Host URL:"), x, y + 2);
        grid.add(hosturl, x + 1, y + 2);
        grid.add(new Label("UserName:"), x, y + 3);
        grid.add(username, x + 1, y + 3);
        grid.add(new Label("Password:"), x, y + 4);
        grid.add(password, x + 1, y + 4);
        grid.add(new Label("Driver Class:"), x, y + 5);
        grid.add(driverclass, x + 1, y + 5);
        grid.add(new Label("JDBC JAR Path :"), x, y + 6);
        grid.add(jarpath, x + 1, y + 6);
        msglabel = new Label("Message: Please Test to enable Add button");
        grid.add(msglabel, x, y + 7);
        grid.add(test, x + 2, y + 7);

        // Enable/Disable login button depending on whether a username was entered.
        Node loginButton = dialog.getDialogPane().lookupButton(loginButtonType);
        loginButton.setDisable(true);
        test.setDisable(true);
        dbname.setDisable(true);
        hosturl.setDisable(true);
        username.setDisable(true);
        password.setDisable(true);
        jarpath.setDisable(true);
        driverclass.setDisable(true);
        
        if (!connName.isEmpty()) {
            logger.info("Loading DB Connection " + connName + " for modification");
            File file = new File("conn/" + connName + ".con");
            FileReader fw = new FileReader(file.getAbsoluteFile());
            BufferedReader bw = new BufferedReader(fw);
            
            String line = bw.readLine();
//            databasetype.selectionModelProperty().setValue(line.substring(line.indexOf("#"), line.lastIndexOf("#")));
            databasetype.setValue(line.substring(line.indexOf("#") + 1, line.lastIndexOf("#")));
            
            dbname.setText(line.substring(0, line.lastIndexOf("(")));
            
            hosturl.setText(bw.readLine());
            
            username.setText(bw.readLine());
            
            password.setText(bw.readLine());
            
            driverclass.setText(bw.readLine());
            
            jarpath.setText(bw.readLine());
            dbname.setDisable(false);
            hosturl.setDisable(false);
            username.setDisable(false);
            password.setDisable(false);
            jarpath.setDisable(false);
            driverclass.setDisable(false);
            loginButton.setDisable(false);
            
        } else {
            logger.info("Creating new DB Connection");
            databasetype.valueProperty().addListener((observable, oldValue, newValue) -> {
                
                dbname.setDisable(false);
                hosturl.setDisable(false);
                username.setDisable(false);
                password.setDisable(false);
                jarpath.setDisable(false);
                driverclass.setDisable(false);
                
            });
        }
        // Do some validation (using the Java 8 lambda syntax).  

        dbname.textProperty().addListener((observable, oldValue, newValue) -> {
            
            if (!driverclass.getText().trim().isEmpty() && !hosturl.getText().trim().isEmpty() && !jarpath.getText().trim().isEmpty()) {
                //loginButton.setDisable(newValue.trim().isEmpty());
                test.setDisable(newValue.trim().isEmpty());
                loginButton.setDisable(true);
                msglabel.setText("Message: Please Test to enable Add button");
            }
            
        });
        
        hosturl.textProperty().addListener((observable, oldValue, newValue) -> {
            
            if (!dbname.getText().trim().isEmpty() && !driverclass.getText().trim().isEmpty() && !jarpath.getText().trim().isEmpty()) {
                //loginButton.setDisable(newValue.trim().isEmpty());
                test.setDisable(newValue.trim().isEmpty());
                loginButton.setDisable(true);
                msglabel.setText("Message: Please Test to enable Add button");
            }
            
        });
        
        jarpath.textProperty().addListener((observable, oldValue, newValue) -> {
            
            if (!dbname.getText().trim().isEmpty() && !hosturl.getText().trim().isEmpty() && !driverclass.getText().trim().isEmpty()) {
                //loginButton.setDisable(newValue.trim().isEmpty());
                test.setDisable(newValue.trim().isEmpty());
                loginButton.setDisable(true);
                msglabel.setText("Message: Please Test to enable Add button");
            }
            
        });
        
        driverclass.textProperty().addListener((observable, oldValue, newValue) -> {
            
            if (!dbname.getText().trim().isEmpty() && !hosturl.getText().trim().isEmpty() && !jarpath.getText().trim().isEmpty()) {
                //loginButton.setDisable(newValue.trim().isEmpty());
                test.setDisable(newValue.trim().isEmpty());
                loginButton.setDisable(true);
                msglabel.setText("Message: Please Test to enable Add button");
            }
            
        });
        
        dialog.getDialogPane().setContent(grid);

        //test db connection    
        test.setOnAction((ActionEvent event) -> {
            logger.info("Verifying the DB Connection.");
            try {
                conn = new DBConnectionManager(driverclass.getText(), hosturl.getText(), username.getText(), password.getText(), jarpath.getText()).getDBCon();
            } catch (Exception ex) {
                logger.error(ex.toString());
                new ExceptionUI(ex);
            }
            if (conn != null) {
                logger.info("DB Connection verification is successful.");
                loginButton.setDisable(false);
                msglabel.setStyle("-fx-text-fill: green");
                msglabel.setTooltip(new Tooltip(conn.getClass().toString()));
                msglabel.setText("Message: Connection Successfull - " + conn);
                try {
                    conn.close();
                } catch (SQLException ex) {
                    logger.error(ex.toString());
                    new ExceptionUI(ex);
                }
            } else {
                logger.error("DB Connection verification is failed.");
                msglabel.setStyle("-fx-text-fill: red");
                msglabel.setText("Message: Connection Unsuccessfull - " + conn);
            }
            
        });

        // Request focus on the username field by default.
        Platform.runLater(() -> dbname.requestFocus());

        // Convert the result to a username-password-pair when the login button is clicked.
        dialog.setResultConverter(dialogButton -> {
            if (dialogButton == loginButtonType) {
                return new Pair<>(dbname.getText(), hosturl.getText() + ";;" + username.getText() + ";;" + password.getText());
            }
            return null;
        });
        
        Optional<Pair<String, String>> result = dialog.showAndWait();
        
        result.ifPresent(dbconstring -> {
            logger.info("Storing DB Connection: " + dbname.getText());
//            System.out.println("Username=" + dbconstring.getKey() + ", Password=" + dbconstring.getValue());
            new SaveConnections(databasetype.getSelectionModel().getSelectedItem().toString(), dbname.getText(), hosturl.getText(), username.getText(), password.getText(), driverclass.getText(), jarpath.getText());
            lctv.appendConnectionTreeView(dbname.getText() + "(#" + databasetype.getSelectionModel().getSelectedItem().toString().toUpperCase() + "#)");
            logger.info("DB Connection has been Stored: " + dbname.getText());
        });
        
    }
    
    public ObservableList getDBTypeList() throws IOException {
        logger.info("Fetching the DB Types that currently application support");
        ObservableList list = FXCollections.observableArrayList();
        
        BufferedReader reader = new BufferedReader(new FileReader("dbtypelist.dat"));
        String line;
        while ((line = reader.readLine()) != null) {
            list.add(line);
        }
        
        reader.close();
        logger.info("DB Types has been fetched");
        return list;
    }
    
}
