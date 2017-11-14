/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package beat;

import java.io.IOException;
import java.util.logging.Level;
import javafx.application.Preloader;
import javafx.application.Preloader.ProgressNotification;
import javafx.application.Preloader.StateChangeNotification;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.ProgressBar;
import javafx.scene.image.Image;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Preloader Using the ProgressBar Control
 *
 * @author Admin
 */
public class BeatPreLoader extends Preloader {

    private final static Logger logger = LoggerFactory.getLogger(BeatPreLoader.class);
    ProgressBar bar;
    Stage stage;
   

    private Scene createPreloaderScene() {
        bar = new ProgressBar();
        BorderPane p = new BorderPane();

        p.setCenter(bar);

        return new Scene(p, 300, 150);
    }

//    @Override
//    public void start(Stage stage) throws Exception {
//        logger.info("Preparing to Load UI");
//        this.mainStage = stage;
//        mainStage.setTitle("BEAT - BIG DATA & ETL Analytics Tester");
//        mainStage.getIcons().add(new Image(getClass().getResourceAsStream("/icon/cont.png")));
//        mainStage.resizableProperty().setValue(Boolean.TRUE);
//        mainStage.setMaximized(true);
//
//        showMainStage();
//    }

    
    @Override
    public void start(Stage stage) throws Exception {
        this.stage = stage;
        stage.setScene(createPreloaderScene());        
        stage.show();
        Thread.sleep(5000);
    }
    @Override
    public void handleStateChangeNotification(StateChangeNotification scn) {
        if (scn.getType() == StateChangeNotification.Type.BEFORE_START) {
            stage.hide();
        }
    }

//    @Override
//    public void handleProgressNotification(ProgressNotification pn) {
//        bar.setProgress(pn.getProgress());
//    }

}
