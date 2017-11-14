/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package beat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import javafx.collections.ObservableList;

/**
 *
 * @author Snehasish
 */
public class TargetTask implements Callable<List> {

    private static final int OFFSET = 10000;
    private static final int THRED_SIZE = 30;
    
    private ObservableList srcInput;
    private ObservableList trgInput;

    TargetTask(ObservableList srcInput, ObservableList trgInput) {
        this.srcInput = srcInput;
        this.trgInput = trgInput;        
    }

    @Override
    public List call() throws Exception {

        try {
            List resultlist = new ArrayList();

            int i = 0;
            
            //If source and target input file size is 0 then dont progress.
            if (srcInput.size() > 0 && trgInput.size() > 0) {
                ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(THRED_SIZE);

                //slice chunks until possible
                for (i = 0; (i + OFFSET) <= srcInput.size() && (i + OFFSET) <= trgInput.size(); i++) {
                    List srcTemp = null;
                    List trgTemp = null;

                    /*If chunk can be sliced from target then do it else
                    * take a slice upto target maximum size.
                     */
                    srcTemp = srcInput.subList(i, i + OFFSET);
                    trgTemp = trgInput.subList(i, i + OFFSET);

                    if (srcTemp != null && trgTemp != null) {
                        Future<List> future = executor.submit(new ChunkTask(srcTemp, trgTemp));
                        resultlist.addAll(future.get());
                    }
                    i = i + OFFSET;
                }
                /*If i is still less after taking maxymum possible chunks
                * then take slice upto last index.
                */
                
                List srcTemp = new ArrayList();
                List trgTemp = new ArrayList();
                if (i < srcInput.size()) {
                    srcTemp = srcInput.subList(i, srcInput.size());
                }
                if (i < trgInput.size()) {
                    trgTemp = trgInput.subList(i, trgInput.size());    
                }
                Future<List> future = executor.submit(new ChunkTask(srcTemp, trgTemp));
                resultlist.addAll(future.get());
            }
            return resultlist;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
