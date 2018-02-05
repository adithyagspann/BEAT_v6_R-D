/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package remoteutility;

import java.util.Optional;
import javafx.scene.control.Alert;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import javafx.stage.Stage;

/**
 *
 * @author Ravindra
 */
public class ConfirmUI {
    
    boolean status;
    
    public boolean getStatus(){
        
        return status;
    }
    
    public ConfirmUI(String msg){
    
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Alert Dialog");
        alert.setHeaderText("Please See below message");
        alert.setContentText(msg);
        
        Stage stage = (Stage) alert.getDialogPane().getScene().getWindow();
        
        // Add a custom icon.
        stage.getIcons().add(new Image(this.getClass().getResource("/icon/save.png").toString()));

        // Set the icon (must be included in the project).
        alert.setGraphic(new ImageView(new Image(getClass().getResourceAsStream("/icon/save.png"))));
        
        

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
        
        Optional<ButtonType> result = alert.showAndWait();
        if (result.get() == ButtonType.OK) {
           
            status = true;
   
        } else {
            
            status = false;
           
        }
    }
    
}
