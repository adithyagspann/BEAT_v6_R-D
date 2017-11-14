/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved.* */
package beat;

import com.sun.javafx.application.LauncherImpl;
import java.io.IOException;
import java.util.logging.Level;
import javafx.application.Application;
import static javafx.application.Application.launch;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.scene.layout.AnchorPane;
import javafx.stage.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ravindra
 */
public class BEAT_V5 extends Application {

    private static final Logger logger = LoggerFactory.getLogger(BEAT_V5.class);
    Stage mainStage;
    AnchorPane root;
    PreLoaderScreenController controller;

    @Override
    public void start(Stage stage) throws Exception {
        logger.info("Preparing to Load UI");
        this.mainStage = stage;
        mainStage.setTitle("BEAT - BIG DATA & ETL Analytics Tester");
        mainStage.getIcons().add(new Image(getClass().getResourceAsStream("/icon/cont.png")));
//        mainStage.resizableProperty().setValue(Boolean.TRUE);
//        mainStage.setMaximized(true);

        showMainStage();
    }

    private void showMainStage() throws IOException {
        logger.info("UI loading");
//        FXMLLoader fxmlLoader = FXMLLoader.load(getClass().getResource("/fxml/PreLoaderScreen.fxml"));
//        fxmlLoader.setLocation(getClass().getResource("/fxml/PreLoaderScreen.fxml"));
//        root = fxmlLoader.load();
        root = FXMLLoader.load(getClass().getResource("/fxml/PreLoaderScreen.fxml"));
        System.out.println("Root :" + root);
//        controller = fxmlLoader.<PreLoaderScreenController>getController();
        Scene scene = new Scene(root);
        mainStage.setScene(scene);
        mainStage.show();
        System.err.println("Done: "+root.getScene());

    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        logger.info("Beat Application Starting");
        launch(args);
//        LauncherImpl.launchApplication(BEAT_V5.class, BeatPreLoader.class, args);
    }

}
