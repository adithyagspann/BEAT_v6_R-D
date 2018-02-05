/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package beat;

import javafx.beans.property.SimpleStringProperty;

/**
 *
 * @author Admin
 */
public class SqoopProcessBean {

    private SimpleStringProperty sqoopHostName = new SimpleStringProperty("");
    private SimpleStringProperty sqoopHostPath = new SimpleStringProperty("");
    private SimpleStringProperty sqoopCmd = new SimpleStringProperty("");

    public SqoopProcessBean(String sqoopHostName, String sqoopHostPath, String sqoopCmd) {
        this.sqoopHostName.set(sqoopHostName);
        this.sqoopHostPath.set(sqoopHostPath);
        this.sqoopCmd.set(sqoopCmd);
    }

    public String getSqoopHostName() {
        return sqoopHostName.getValue();
    }

    public void setSqoopHostName(String sqoopHostName) {
        this.sqoopHostName.set(sqoopHostName);
    }

    public String getSqoopHostPath() {
        return sqoopHostPath.getValue();
    }

    public void setSqoopHostPath(String sqoopHostPath) {
        this.sqoopHostPath.set(sqoopHostPath);
    }

    public String getSqoopCmd() {
        return sqoopCmd.getValue();
    }

    public void setSqoopCmd(String sqoopCmd) {
        this.sqoopCmd.set(sqoopCmd);
    }

    @Override
    public String toString() {
        return sqoopHostName.getValue() + "#" + sqoopHostPath.getValue() + "#" + sqoopCmd .getValue();
    }

}
