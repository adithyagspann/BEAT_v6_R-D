package remoteutility;

import java.util.Optional;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.GridPane;
import javafx.stage.Stage;
import javafx.util.Pair;

/**
 *
 * @author Ravindra
 */
public class PromptPasswordUI {

    // Create the custom dialog.
    private Dialog<Pair<String, String>> dialog;
    private ButtonType loginButtonType;
    private PasswordField tf;
    private String promptmsg="";
    
    
    public PromptPasswordUI(String titile, String header,String msg) {

        dialog = new Dialog<>();

        dialog.setTitle(titile);
        dialog.setHeaderText(header);

        // Get the Stage.
        Stage stage = (Stage) dialog.getDialogPane().getScene().getWindow();

        // Add a custom icon.
        stage.getIcons().add(new Image(this.getClass().getResource("/icon/save.png").toString()));

        // Set the icon (must be included in the project).
        dialog.setGraphic(new ImageView(new Image(getClass().getResourceAsStream("/icon/save.png"))));

        // Set the button types.
        loginButtonType = new ButtonType("SAVE", ButtonBar.ButtonData.OK_DONE);
        dialog.getDialogPane().getButtonTypes().addAll(loginButtonType, ButtonType.CANCEL);

        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20, 30, 10, 10));

        tf = new PasswordField();
        tf.setMinWidth(300);
        tf.setPromptText(msg);

        grid.add(new Label(msg+" :"), 0, 0);
        grid.add(tf, 1, 0);

        // Enable/Disable login button depending on whether a username was entered.
        Node loginButton = dialog.getDialogPane().lookupButton(loginButtonType);
        loginButton.setDisable(true);

        tf.textProperty().addListener((observable, oldValue, newValue) -> {

            if (!tf.getText().trim().isEmpty()) {
                loginButton.setDisable(false);
            } else {
                loginButton.setDisable(true);
            }
        });

        dialog.getDialogPane().setContent(grid);

        // Request focus on the username field by default.
        Platform.runLater(() -> tf.requestFocus());

        // Convert the result to a username-password-pair when the login button is clicked.
        dialog.setResultConverter(dialogButton -> {
            if (dialogButton == loginButtonType) {
                return new Pair<>("USERNAME", tf.getText());
            }
            return null;
        });

        Optional<Pair<String, String>> result = dialog.showAndWait();

        result.ifPresent(testerstring -> {
            promptmsg = tf.getText();
            System.out.println("Prompt Password  : " + tf.getText());

        });
        
        promptmsg = result.isPresent()?tf.getText():null;
        

    }
    
    public String getPassword(){    
        return promptmsg;
    }
}