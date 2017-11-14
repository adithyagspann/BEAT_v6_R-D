/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved.
 *
 * This software and related documentation are provided under a license agreement containing restrictions on use and
 * disclosure and are protected by intellectual property laws. Except as expressly permitted in your license agreement
 * or allowed by law, you may not use, copy, reproduce, translate, broadcast, modify, license, transmit, distribute,
 * exhibit, perform, publish, or display any part, in any form, or by any means. Reverse engineering, disassembly
 * or decompilation of this software, unless required by law for interoperability, is prohibited.
 *
 * The information contained herein is subject to change without notice and is not warranted to be error-free.
 * If you find any errors, please report them to us in writing.
 *
 * If this software or related documentation is delivered to the U.S. Government or anyone licensing it on behalf
 * of the U.S. Government, the following notice is applicable:
 *
 * U.S. GOVERNMENT RIGHTS Programs, software, databases, and related documentation and technical data delivered to U.S.
 * Government customers are "commercial computer software" or "commercial technical data" pursuant to the applicable Federal
 * Acquisition Regulation and agency-specific supplemental regulations. As such, the use, duplication, disclosure,
 * modification, and adaptation shall be subject to the restrictions and license terms set forth in the applicable Government
 * contract, and, to the extent applicable by the terms of the Government contract, the additional rights set forth in FAR 52.227-19,
 * Commercial Computer Software License (December 2007). GSPANN Technologies Inc., 362 Fairview Way, Milpitas, CA 95035, USA .
 *
 * This software is developed for general use in a variety of information management applications. It is not developed or
 * intended for use in any inherently dangerous applications, including applications which may create a risk of personal
 * injury. If you use this software in dangerous applications, then you shall be responsible to take all appropriate fail-safe,
 * backup, redundancy, and other measures to ensure the safe use of this software. Gspann and its affiliates disclaim any liability
 * for any damages caused by use of this software in dangerous applications.
 *
 * GSPANN is a registered trademark of GSPANN and/or its affiliates. Other names may be trademarks of their respective owners.
 *
 * This software and documentation may provide access to or information on content, products, and services from third parties.
 * Gspann and its affiliates are not responsible for and expressly disclaim all warranties of any kind with respect to third-party
 * content, products, and services. Gspann and its affiliates will not be responsible for any loss, costs, or damages incurred due
 * to your access to or use of third-party content, products, or services.
 * */
package beat;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.util.Pair;

/**
 *
 * @author Ravindra
 */
public class LoadManualTestCaseUI {

// Create the custom dialog.
    private Dialog<Pair<String, String>> dialog;
    private ButtonType loginButtonType;
    private Button test;
    private Button localfilebt;
    private TextField hosturl;

    private ComboBox filetypecmb;
    private TextField jarpath;

    private Label msglabel;
    private LoadFlatFilesTreeView lctv;
    
    private CheckBox whereClausesrcStatus;
    private TextArea whereClausesrc;

//file chooser
    final FileChooser fileChooser = new FileChooser();
    File file;
    private TextArea whereClausetrg;
    private CheckBox whereClausetrgStatus;

    LoadManualTestCaseUI(LoadFlatFilesTreeView lctv, VBox mainvbox) {

        this.lctv = lctv;

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

        localfilebt = new Button();
        localfilebt.setText("Upload File");
        localfilebt.setMaxWidth(150);

        whereClausesrcStatus = new CheckBox("Source");
        whereClausesrcStatus.setDisable(true);
        whereClausetrgStatus = new CheckBox("Target");
        whereClausetrgStatus.setDisable(true);
        whereClausesrc = new TextArea();
        whereClausesrc.setPromptText("Ex: col1 = 'value1' and col2 = 'value2 ");
        whereClausesrc.setMaxWidth(250);
        whereClausesrc.setMaxHeight(120);
        whereClausesrc.setDisable(true);
        whereClausetrg = new TextArea();
        whereClausetrg.setPromptText("Ex: having col1 = 'value1' order by colname");
        whereClausetrg.setMaxWidth(250);
        whereClausetrg.setMaxHeight(120);
        whereClausetrg.setDisable(true);

        grid.add(new Label("Choose:"), 0, 0);

        grid.add(localfilebt, 1, 0);

        grid.add(new Label("Where Condition:"), 0,1 );
        grid.add(whereClausesrcStatus, 1, 1);
        
        grid.add(whereClausetrgStatus, 1, 2);
        
   grid.add(new Label("Source:"), 0, 3);
        grid.add(whereClausesrc, 1, 3, 2, 1);
        grid.add(new Label("Target:"), 0, 4);
        grid.add(whereClausetrg, 1, 4, 2, 1);


        msglabel = new Label("Choose Test Case File");
        grid.add(msglabel, 0, 5, 2, 1);

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

        whereClausesrcStatus.selectedProperty().addListener((observable, oldValue, newValue) -> {

            if (whereClausesrcStatus.isSelected()) {

                whereClausesrc.setDisable(false);
                whereClausetrg.setDisable(false);
                dialog.getDialogPane().lookupButton(loginButtonType).setDisable(true);
            } else {

                whereClausesrc.setDisable(true);
                whereClausetrg.setDisable(true);
                dialog.getDialogPane().lookupButton(loginButtonType).setDisable(false);
            }
        });
        
        
        whereClausetrgStatus.selectedProperty().addListener((observable, oldValue, newValue) -> {

            if (whereClausesrcStatus.isSelected()) {

           
                whereClausetrg.setDisable(false);
                dialog.getDialogPane().lookupButton(loginButtonType).setDisable(true);
            } else {

           
                whereClausetrg.setDisable(true);
                dialog.getDialogPane().lookupButton(loginButtonType).setDisable(false);
            }
        });

        whereClausesrc.selectionProperty().addListener((observable, oldValue, newValue) -> {
            if (whereClausesrc.getSelection().toString().isEmpty() || whereClausesrc.getSelection().toString() == "" || whereClausesrc.getSelection().toString() == " ") {
                dialog.getDialogPane().lookupButton(loginButtonType).setDisable(true);
            } else {

                dialog.getDialogPane().lookupButton(loginButtonType).setDisable(false);

            }
        });
        
        whereClausetrg.selectionProperty().addListener((observable, oldValue, newValue) -> {
            if (whereClausetrg.getSelection().toString().isEmpty() || whereClausetrg.getSelection().toString() == "" || whereClausetrg.getSelection().toString() == " ") {
                dialog.getDialogPane().lookupButton(loginButtonType).setDisable(true);
            } else {

                dialog.getDialogPane().lookupButton(loginButtonType).setDisable(false);

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
                if(file.toString().contains(".xls")){
                msglabel.setStyle("-fx-text-fill: black");
                msglabel.setTooltip(new Tooltip(fpath));
                msglabel.setText("Test Case File: " + fname);
                test.setDisable(false);
                whereClausesrc.setDisable(false);
                whereClausetrg.setDisable(false);
                whereClausesrcStatus.setDisable(false);
                whereClausesrcStatus.setSelected(true);
                whereClausetrgStatus.setDisable(false);
                whereClausetrgStatus.setSelected(true);

                dialog.getDialogPane().lookupButton(loginButtonType).setDisable(true);
                }
                else{
                    msglabel.setStyle("-fx-text-fill: red");
                msglabel.setText("Message: Please Load Valid the File");
                }
            } else {
                msglabel.setStyle("-fx-text-fill: red");
                msglabel.setText("Message: Please Load the File");
            }

        });

        // Convert the result to a username-password-pair when the login button is clicked.
        dialog.setResultConverter(dialogButton -> {
            if (dialogButton == loginButtonType) {
                return new Pair<>("","");
            }
            return null;
        });

        Optional<Pair<String, String>> result = dialog.showAndWait();


    }

    public List getFileWhereClauseData() {
        List fileWhereClause = new ArrayList();
        fileWhereClause.add(file);
        fileWhereClause.add(whereClausesrc.getText());
        fileWhereClause.add(whereClausetrg.getText());
        return fileWhereClause;
    }
}
