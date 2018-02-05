/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved. **/

package beat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ravindra
 */
public class SaveConnections {
    
    private String connname="";
    private String hosturl="";
    private String usrname="";
    private String pswd="";
    private String drvclassnme="";
    private String drvpath="";
     
    private static Logger logger = LoggerFactory.getLogger(SaveConnections.class);
    public SaveConnections(String dbtype,String connname, String hosturl, String usrname, String pswd, String  drvclassnme, String drvpath){
   logger.info("Saving Connection file for Connection Name:  "+connname);
        this.connname = connname;
        this.hosturl = hosturl;
        this.usrname = usrname;
        this.pswd = pswd;
        this.drvclassnme = drvclassnme;
        this.drvpath = drvpath;
        
        try {
            File file = new File("conn/" + connname + "(#"+dbtype.toUpperCase()+"#).con");

            // if file doesnt exists, then create it
            if (!file.exists()) {
                    file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(connname + "(#"+dbtype.toUpperCase()+"#)");
            bw.write("\n");
            bw.write(hosturl);
            bw.write("\n");
            bw.write(usrname);
            bw.write("\n");
            bw.write(pswd);
            bw.write("\n");
            bw.write(drvclassnme);
            bw.write("\n");
            bw.write(drvpath);
            bw.write("\n");
            bw.close();
            logger.info("Connection file has been saved");
	} catch (IOException ex) {
            logger.error(ex.toString());
            new ExceptionUI(ex);
	}
        
    }
    
    
}
