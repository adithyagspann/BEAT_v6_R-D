/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved. * */
package beat;

import javafx.scene.control.Alert;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ravindra
 */
public class AlertUI {

    private final static Logger logger = LoggerFactory.getLogger(AlertUI.class);

    public AlertUI(String msg) {
        logger.info("Processing Alert UI ");
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("Alert Dialog");
        alert.setHeaderText("Please See below message");
        alert.setContentText(msg);
        
        Label label = new Label("Alert:");
        
        TextArea textArea = new TextArea(msg);
        textArea.setEditable(false);
        textArea.setWrapText(true);
        
        textArea.setMaxWidth(Double.MAX_VALUE);
        textArea.setMaxHeight(Double.MAX_VALUE);
        GridPane.setVgrow(textArea, Priority.ALWAYS);
        GridPane.setHgrow(textArea, Priority.ALWAYS);
        
        GridPane expContent = new GridPane();
        expContent.setMaxWidth(Double.MAX_VALUE);
        expContent.add(label, 0, 0);
        expContent.add(textArea, 0, 1);

        // Set expandable Exception into the dialog pane.
        alert.getDialogPane().setExpandableContent(expContent);
        
        alert.showAndWait();
        
        logger.info("Alert UI has been closed");
    }
    
}
