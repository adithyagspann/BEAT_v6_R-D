/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved. * */
package beat;

/**
 *
 * @author Ravindra
 */
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaveFF {
    
    private static Logger logger = LoggerFactory.getLogger(SaveFF.class);
    private String filename = "";
    private String filepath = "";
    private String type = "";
    
    public SaveFF(String filename, String filepath, String type, LoadFlatFilesTreeView lctv) {
        logger.info("Saving Flat file : " + filename);
        this.filename = filename;
        this.filepath = filepath;
        this.type = type;
        
        try {
            
            File file = new File("files/" + type.toLowerCase() + "/" + filename);

            // if file doesnt exists, then create it
            if (!file.exists()) {
                System.out.println(filename);
                file.createNewFile();
                lctv.appendConnectionTreeView(file.getName());
            }
            
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("local");
            bw.write("\n");
            bw.write(filepath);
            bw.write("\n");
            bw.close();
            logger.info("Flat file has been saved ");
        } catch (IOException ex) {
            logger.info("Exception in saving file : ", ex);
            new ExceptionUI(ex);
        }
        
    }
    
    public SaveFF(String filename, String hosttype, String hostUrl, String username, String password, String filepath, String type, LoadFlatFilesTreeView lctv) {
        try {
            logger.info("Saving Flat file for Remote connection : " + filename);
            File file = new File("files/" + type.toLowerCase() + "/" + filename);

            // if file doesnt exists, then create it
            if (!file.exists()) {
                System.out.println(filename);
                file.createNewFile();
                lctv.appendConnectionTreeView(file.getName());
            }
            
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("remote");
            bw.write("\n");
            bw.write(hosttype);
            bw.write("\n");
            bw.write(hostUrl);
            
            bw.write("\n");
            bw.write(filepath);
            bw.write("\n");
            bw.write(username);
            bw.write("\n");
            bw.write(password);
            bw.close();
            logger.info("Flat file has been saved for Remote connection");
        } catch (IOException ex) {
            logger.error(ex.toString());
            new ExceptionUI(ex);
        }
    }
    
}
