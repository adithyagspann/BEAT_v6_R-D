/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package remoteutility;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Admin
 */
public class RemoteFileDownload {

    Session session;
    public String passphrase, password;

//    UserName, Password, HostName, Path
    public RemoteFileDownload(String host, String user, String password) throws JSchException {

        JSch jsch = new JSch();
        session = jsch.getSession(user, host, 22);

        System.out.println("Demo start");
        UserInfo ui = new ScpUserInfo(password);

        session.setUserInfo(ui);
        System.out.println("Demo2");
        session.connect();
        System.out.println("Demo");

    }

    public void ScpGetFile(String remotefile, String localfile) throws IOException, JSchException {

        System.out.println("Time : " + new Date());

        String prefix = null;
        if (new File(localfile).isDirectory()) {
            prefix = localfile + File.separator;
        }

        FileOutputStream fos = null;

        // exec 'scp -f rfile' remotely
        String command = "scp -f " + remotefile;
        System.out.println("Session object " + session);
        Channel channel = session.openChannel("exec");
        ((ChannelExec) channel).setCommand(command);
        System.out.println("Processing");
        // get I/O streams for remote scp
        OutputStream out = channel.getOutputStream();
        InputStream in = channel.getInputStream();

        channel.connect();

        byte[] buf = new byte[1024];

        // send '\0'
        buf[0] = 0;
        out.write(buf, 0, 1);
        out.flush();

        while (true) {
            int c = checkAck(in);
            if (c != 'C') {
                break;
            }

            // read '0644 '
            in.read(buf, 0, 5);

            long filesize = 0L;
            while (true) {
                if (in.read(buf, 0, 1) < 0) {
                    // error
                    break;
                }
                if (buf[0] == ' ') {
                    break;
                }
                filesize = filesize * 10L + (long) (buf[0] - '0');
            }

            String file = null;
            for (int i = 0;; i++) {
                in.read(buf, i, 1);
                if (buf[i] == (byte) 0x0a) {
                    file = new String(buf, 0, i);
                    break;
                }
            }

            //System.out.println("filesize="+filesize+", file="+file);
            // send '\0'
            buf[0] = 0;
            out.write(buf, 0, 1);
            out.flush();

            // read a content of lfile
            try {
                fos = new FileOutputStream(prefix == null ? localfile : prefix + file);
                int foo;
                while (true) {
                    if (buf.length < filesize) {
                        foo = buf.length;
                    } else {
                        foo = (int) filesize;
                    }
                    foo = in.read(buf, 0, foo);
                    if (foo < 0) {
                        // error 
                        break;
                    }
                    fos.write(buf, 0, foo);
                    filesize -= foo;
                    if (filesize == 0L) {
                        break;
                    }
                }

                fos.close();
                fos = null;

            } catch (Exception e) {
                try {
                    channel.disconnect();
                    in.close();
                } catch (Exception ee) {
                }
                throw e;
            }
            if (checkAck(in) != 0) {
                System.out.println("[ERROR] Something Wrong Happened! Reconnecting ...");
            }

            // send '\0'
            buf[0] = 0;
            out.write(buf, 0, 1);
            out.flush();
        }

        System.out.println("Done");
    }

    static int checkAck(InputStream in) throws IOException {
        int b = in.read();
        // b may be 0 for success,
        //          1 for error,
        //          2 for fatal error,
        //          -1
        if (b == 0) {
            return b;
        }
        if (b == -1) {
            return b;
        }

        if (b == 1 || b == 2) {
            StringBuffer sb = new StringBuffer();
            int c;
            do {
                c = in.read();
                sb.append((char) c);
            } while (c != '\n');
            if (b == 1) { // error
                System.err.print("[ERROR] " + sb.toString());
                throw (new IOException(sb.toString()));
            }
            if (b == 2) { // fatal error
                System.err.print("[ERROR] " + sb.toString());
                throw (new IOException(sb.toString()));
            }
        }
        return b;
    }

    public void sessionClose() {
        session.disconnect();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws JSchException {
        // TODO code application logic here
        new RemoteFileDownload("192.168.189.102", "adithyap", "hduser");
    }

}
