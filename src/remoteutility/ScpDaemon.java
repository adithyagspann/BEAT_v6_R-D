/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package remoteutility;

import com.jcraft.jsch.JSchException;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import remoteutility.RemoteFileDownload;

/**
 *
 * @author Ravindra
 */
public class ScpDaemon {
   
    static Properties props;
    
    String serverAddress;
    String userId;
    String filepattern;
    String remotefilepath;
    String localfilepath; 
    String rfileextension;
    String lfileextension;
    String firstfilename;
    String lastfilename;
    String refreshrate;
    String curdate;

//    
//    public ScpDaemon(String propertiesFilename) throws IOException, JSchException{
//        
//        //loading prop file
//        props = new Properties();
//        props.load(new FileInputStream("resources/" + propertiesFilename));
//        
//        //loading props
//        remotefilepath = props.getProperty("remotefilepath").trim();
//        localfilepath = props.getProperty("localfilepath").trim();
//        filepattern = props.getProperty("filepattern").trim();
//        rfileextension = props.getProperty("rfileextension").trim();
//        lfileextension = props.getProperty("lfileextension").trim();
//        firstfilename = props.getProperty("firstfilename").trim();
//        lastfilename = props.getProperty("lastfilename").trim();
//        refreshrate = props.getProperty("refreshrate").trim();
//        serverAddress = props.getProperty("serverAddress").trim();
//        //file name
//        curdate = dateFormat(filepattern);
//
//    }
    
    public String getLFileName(){
        return "File Name : " + curdate + "." + lfileextension;
    }
    
    public String getLFilePath(){
        return props.getProperty("localfilepath").trim();
    }
    
    public String getRefreshRate(){
        return "Refresh Rate : " + refreshrate + " secs";
    }
    
    public String getServerAddress(){
        return serverAddress;
    }
    
    public void callScpgetFile(RemoteFileDownload sd) throws IOException, JSchException{
       
        
        String rfile = remotefilepath + "/" + firstfilename + curdate + lastfilename + "." + rfileextension;       
        String lfile = localfilepath + "/" + firstfilename + curdate + lastfilename + "." + lfileextension;
        
        sd.ScpGetFile(rfile,lfile);
        
    }
    
    public String dateFormat(String pattern){
                
        Calendar cal = Calendar.getInstance();
        Date date = cal.getTime();
        SimpleDateFormat ft = new SimpleDateFormat(pattern);
        
        String fdate = ft.format(date);
        System.out.println("[INFO] Formated Date  is : "+fdate);
                
        return fdate;        
        
    }
    
    public void scpConnectionClose(){
        
        
        
    }
    
    
    /*public static void main(String args[]) throws IOException, JSchException{
        
            ScpDaemon sdm = new ScpDaemon("scp.properties");
            sdm.callScpgetFile();
    }*/
    
    
}
