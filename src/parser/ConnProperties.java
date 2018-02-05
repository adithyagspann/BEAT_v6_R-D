package parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author Adithya
 */
public class ConnProperties {

    

    private Properties loadConn;
    private FileInputStream propFile;
    private File connectionFile;

    public ConnProperties() throws FileNotFoundException, IOException, IOException {
        loadConn = new Properties();
        connectionFile = new File("BEAT.properties");
        propFile = new FileInputStream(connectionFile);
        loadConn.load(propFile);
        System.out.println("File: " + connectionFile.getAbsolutePath());

    }

    public String getPropertyValue(String key) {
        return loadConn.getProperty(key);
    }

}
