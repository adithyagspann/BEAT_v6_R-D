/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package beat;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Sachin
 */
public class ComparisonResultModel {
    List srclist = new ArrayList();
    List trglist = new ArrayList();

    public List getSrclist() {
        return srclist;
    }

    public void setSrclist(List srclist) {
        this.srclist = srclist;
    }

    public List getTrglist() {
        return trglist;
    }

    public void setTrglist(List trglist) {
        this.trglist = trglist;
    }
}
