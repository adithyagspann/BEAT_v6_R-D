/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package beat;

import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;
import java.util.logging.Level;
import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.StackPane;
import javafx.stage.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Admin
 */
public class PreLoaderScreenController implements Initializable {

    @FXML
    private AnchorPane rootPane;

    @Override
    public void initialize(URL url, ResourceBundle rb) {
        System.err.println("Error: " + rootPane);
        new SplashScreenLoader().start();
    }

    class SplashScreenLoader extends Thread {

        private Stage mainStage;
        private Parent root;
        private MainStageController controller;
        private final  Logger logger = LoggerFactory.getLogger(SplashScreenLoader.class);

        @Override
        public void run() {
            try {
                logger.info("Anchor Pane: " + rootPane);
                Thread.sleep(5000);
                Platform.runLater(new Runnable() {
                    @Override
                    public void run() {
                        logger.info("UI loading");
                        mainStage = new Stage();
                        mainStage.setTitle("BEAT - BIG DATA & ETL Analytics Tester");
                        mainStage.getIcons().add(new Image(getClass().getResourceAsStream("/icon/cont.png")));
                        mainStage.resizableProperty().setValue(Boolean.TRUE);
                        mainStage.setMaximized(true);
//        Thread.sleep(5000);
                        FXMLLoader fxmlLoader = new FXMLLoader();

                        fxmlLoader.setLocation(getClass().getResource("/fxml/MainStage.fxml"));

                        try {
                            root = fxmlLoader.load();
                        } catch (IOException ex) {
                            java.util.logging.Logger.getLogger(PreLoaderScreenController.class.getName()).log(Level.SEVERE, null, ex);
                        }

                        System.out.println("Root :" + root);
                        controller = fxmlLoader.<MainStageController>getController();
                        Scene scene = new Scene(root);

                        mainStage.setScene(scene);
                        mainStage.show();
                    rootPane.getScene().getWindow().hide();
                    }
                }
                
                );
                
                
            } catch (InterruptedException ex) {
                java.util.logging.Logger.getLogger(PreLoaderScreenController.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
    }
}
