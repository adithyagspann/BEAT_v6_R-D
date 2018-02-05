/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package beat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import javafx.collections.ObservableList;

/**
 *
 * @author Sachin
 */
public class ComparisonProcessor {
    
    private ObservableList srcInput;
    private ObservableList trgInput;

    public ComparisonResultModel process() {
        try {
            ComparisonResultModel resultModel = new ComparisonResultModel();
            
            ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(50);
            Future<List> srcfuture = executor.submit(new TargetTask(srcInput, trgInput));
            Future<List> trgfuture = executor.submit(new TargetTask(trgInput, srcInput));
            
            resultModel.setSrclist(srcfuture.get());
            resultModel.setTrglist(trgfuture.get());
            
            return resultModel;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public ObservableList getSrcInput() {
        return srcInput;
    }

    public void setSrcInput(ObservableList srcInput) {
        this.srcInput = srcInput;
    }

    public ObservableList getTrgInput() {
        return trgInput;
    }

    public void setTrgInput(ObservableList trgInput) {
        this.trgInput = trgInput;
    }
}
