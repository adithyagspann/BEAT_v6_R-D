/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package remoteutility;

import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;
import javax.swing.JOptionPane;

/**
 *
 * @author Admin
 */
public class ScpUserInfo implements UserInfo, UIKeyboardInteractive {

    String passwd;

    @Override
    public String getPassword() {
        return passwd;
    }

    public ScpUserInfo(String passwd) {
        this.passwd = passwd;
    }

    @Override
    public boolean promptYesNo(String str) {
        boolean status = new ConfirmUI(str).getStatus();
        return status;
    }

    @Override
    public String getPassphrase() {
        return null;
    }

    @Override
    public boolean promptPassphrase(String message) {
        return true;
    }

    @Override
    public boolean promptPassword(String message) {

        String result = new PromptPasswordUI("Password Dialog", "Password", "Enter Password ").getPassword();

        if (result != null) {
            passwd = result;
            System.out.println("Passwd: " + passwd);
            return true;
        } else {
            return false;
        }
    }

    public void showMessage(String message) {
        JOptionPane.showMessageDialog(null, message);
    }

    public String[] promptKeyboardInteractive(String destination,
            String name,
            String instruction,
            String[] prompt,
            boolean[] echo) {

        String[] texts = new String[prompt.length];
        System.out.println("Instruction: " + instruction);
        for (int i = 0; i < prompt.length; i++) {
            System.out.println("Echo: " + prompt[i]);
            if (echo[i]) {
                texts[i] = new PromptInputUI("Prompt Dialog", prompt[i], prompt[i]).getMessage();
            } else {
                texts[i] = new PromptPasswordUI("Prompt Dialog", prompt[i], prompt[i]).getPassword();
            }

            System.out.println(texts[i]);

            //if cancel is pressed
            if (texts[i] == null) {

                break;

            }
        }

        if (texts.length > 0) {
            String[] response = new String[prompt.length];
            for (int i = 0; i < prompt.length; i++) {
                response[i] = texts[i];
            }
            return response;
        } else {
            return null;  // cancel
        }
    }
}
