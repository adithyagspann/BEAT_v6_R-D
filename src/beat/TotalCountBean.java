/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved.*/

package beat;

import javafx.beans.property.SimpleStringProperty;

/**
 *
 * @author Adithya
 */

/*Bean for the Total Count Data*/
public class TotalCountBean {

    public SimpleStringProperty srcCnt = new SimpleStringProperty();
    public SimpleStringProperty trgCnt = new SimpleStringProperty();
    public SimpleStringProperty totCnt = new SimpleStringProperty();

    public String getSrcCnt() {
        return srcCnt.get();
    }

    public String getTrgCnt() {
        return trgCnt.get();
    }

    public String getTotCnt() {
        return totCnt.get();
    }

    @Override
    public String toString() {
        return "srcCnt," + getSrcCnt() + ",trgCnt," + getTrgCnt() + "," + getTotCnt();
    }

}
