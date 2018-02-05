
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package remoteutility;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import java.io.*;

/**
 *
 * @author Admin
 */
public class RemoteFileOps {
 Session session;
    public String passphrase, password;
     public RemoteFileOps(String host, String user, String password) throws JSchException {

        JSch jsch = new JSch();
        session = jsch.getSession(user, host, 22);
session.setPassword(password);
java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
         session.setConfig(config);
        System.out.println("Demo start");
//        UserInfo ui = new ScpUserInfo(password);
//
//        session.setUserInfo(ui);
        System.out.println("Host: "+host);
        System.out.println("User: "+user);
        session.connect();
        

    }
    public void send(String trgPath, String fileName) {

        int SFTPPORT = 22;

        
        Channel channel = null;
        ChannelSftp channelSftp = null;
        System.out.println("preparing the host information for sftp.");
        try {
            JSch jsch = new JSch();
            
//            session = jsch.getSession(username, hostname, SFTPPORT);
//            session.setPassword(password);
//            java.util.Properties config = new java.util.Properties();
//            config.put("StrictHostKeyChecking", "no");
//            session.setConfig(config);
//            session.connect();
            System.out.println("Host connected.");
            channel = session.openChannel("sftp");
            channel.connect();
            System.out.println("sftp channel opened and connected.");
            channelSftp = (ChannelSftp) channel;
            channelSftp.cd(trgPath);
            
            File f = new File(fileName);
            channelSftp.put(new FileInputStream(f), f.getName());

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Exception found while tranfer the response.");
        } finally {

            channelSftp.exit();
            System.out.println("sftp Channel exited.");
            channel.disconnect();
            System.out.println("Channel disconnected.");
            session.disconnect();
            System.out.println("Host Session disconnected.");
        }
    }
    public static void main(String args[]) throws JSchException
    {
        RemoteFileOps fileOps = new RemoteFileOps("mdc2vr3072", "yh04218", "b1gd2t2sync");
//        SSHReadFile file = new SSHReadFile();
        fileOps.send( "/home/yh04218", "C:\\Users\\Admin\\Desktop\\sqoop");
    }

}
