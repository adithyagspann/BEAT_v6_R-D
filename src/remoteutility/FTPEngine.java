package remoteutility;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPEngine {
    
    private static FTPClient ftpClient;
    private static Logger logger = LoggerFactory.getLogger(FTPEngine.class);
    
    public FTPEngine(String hostname, int portno, String username, String password) throws IOException {
        logger.info("Creating Connection to FTP server: " + hostname);
        ftpClient = new FTPClient();
        ftpClient.connect(hostname, portno);
        ftpClient.login(username, password);
        logger.info("Connection Created to FTP server: " + hostname);
    }
    
    public boolean getFile(String remotePathWithFilename, String localPathWithFilename) throws FileNotFoundException, IOException {
//        System.out.println("Processing the Data");
        logger.info("Processing the file download");
        File localfile = new File(localPathWithFilename);
        System.out.println("Local Path: " + localfile.getParentFile());
        if (!localfile.getParentFile().exists()) {
//            System.out.println("Create Directory Called");
            Files.createDirectory(Paths.get(localfile.getParent()));
        }
        
        boolean status = false;
        
        status = ftpClient.retrieveFile(remotePathWithFilename, new FileOutputStream(localPathWithFilename));

//        System.out.println("FTP File Download Process Completed");
        logger.info("FTP File Download Process Completed");
        return status;
        
    }
    
    public boolean checkFileExists(String remoteName) throws IOException {
        logger.info("Checking whether file "+remoteName+" exists on server");
        if (ftpClient.retrieveFileStream(remoteName) != null) {
            logger.info(remoteName+" file available.");
            return true;
        }
        logger.info(remoteName+" file not available.");
        return false;
        
    }
    
    public String getRemoteModTimeStamp(String fileName) throws IOException {
        logger.info("Retreving time stamp of file: "+fileName);
        String timeStamp = ftpClient.getModificationTime(fileName);
        String time = timeStamp.substring(timeStamp.indexOf(" ") + 1, timeStamp.length()).trim();
        logger.info("Time stamp of file: "+time);
        return time;
    }

//    public static void main(String[] args) {
//
//        FTPEngine ftpobj = null;
//
//        try {
//            //        try {
//
//            ftpobj = new FTPEngine("mdc1vr1002", 21, "scripts", "st@rs1");
//            System.out.println(ftpClient.retrieveFileStream("rfilePath"));
////            boolean success = ftpobj.getFile("/data/aggregator/test_preview/aggregator_output/site/category.csv", "E:/category12.csv");
////            System.out.println("Time : " + ftpobj.getRemoteModTimeStamp("/data/aggregator/test_preview/aggregator_output/site/category1212.csv"));
//        } catch (IOException ex) {
//            Logger.getLogger(FTPEngine.class.getName()).log(Level.SEVERE, null, ex);
//        }
//
////            if (!success) {
////                System.out.println("Ftp to download the file unsuccessful.");
////            } else {
////                System.out.println("Ftp to download the file successful.");
////            }
////        } catch (IOException ex) {
////            Logger.getLogger(FTPEngine.class.getName()).log(Level.SEVERE, null, ex);
////        } finally {
////            try {
////
////                if (ftpobj.ftpClient.isConnected()) {
////                    ftpobj.ftpClient.logout();
////                    ftpobj.ftpClient.disconnect();
////                }
////            } catch (IOException ex) {
////                ex.printStackTrace();
////            }
////        }
//    }
}
