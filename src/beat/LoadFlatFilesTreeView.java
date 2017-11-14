/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved.
 *
 * This software and related documentation are provided under a license agreement containing restrictions on use and 
 * disclosure and are protected by intellectual property laws. Except as expressly permitted in your license agreement 
 * or allowed by law, you may not use, copy, reproduce, translate, broadcast, modify, license, transmit, distribute,
 * exhibit, perform, publish, or display any part, in any form, or by any means. Reverse engineering, disassembly
 * or decompilation of this software, unless required by law for interoperability, is prohibited.
 *
 * The information contained herein is subject to change without notice and is not warranted to be error-free.
 * If you find any errors, please report them to us in writing.
 *
 * If this software or related documentation is delivered to the U.S. Government or anyone licensing it on behalf 
 * of the U.S. Government, the following notice is applicable:
 *
 * U.S. GOVERNMENT RIGHTS Programs, software, databases, and related documentation and technical data delivered to U.S. 
 * Government customers are "commercial computer software" or "commercial technical data" pursuant to the applicable Federal
 * Acquisition Regulation and agency-specific supplemental regulations. As such, the use, duplication, disclosure, 
 * modification, and adaptation shall be subject to the restrictions and license terms set forth in the applicable Government 
 * contract, and, to the extent applicable by the terms of the Government contract, the additional rights set forth in FAR 52.227-19, 
 * Commercial Computer Software License (December 2007). GSPANN Technologies Inc., 362 Fairview Way, Milpitas, CA 95035, USA .
 *
 * This software is developed for general use in a variety of information management applications. It is not developed or 
 * intended for use in any inherently dangerous applications, including applications which may create a risk of personal 
 * injury. If you use this software in dangerous applications, then you shall be responsible to take all appropriate fail-safe,
 * backup, redundancy, and other measures to ensure the safe use of this software. Gspann and its affiliates disclaim any liability
 * for any damages caused by use of this software in dangerous applications.
 *
 * GSPANN is a registered trademark of GSPANN and/or its affiliates. Other names may be trademarks of their respective owners.
 *
 * This software and documentation may provide access to or information on content, products, and services from third parties.
 * Gspann and its affiliates are not responsible for and expressly disclaim all warranties of any kind with respect to third-party 
 * content, products, and services. Gspann and its affiliates will not be responsible for any loss, costs, or damages incurred due 
 * to your access to or use of third-party content, products, or services.
 **/

package beat;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;


/**
 *
 * @author Ravindra
 */
class LoadFlatFilesTreeView {
    
    private TreeItem <String> flatfiles, textcsvfiles, excel, xml, json, csv;
    private TreeView flatfiletree;
    private String connpath = "files/";
    private Image csvicon;
    private Image textcsvicon;
    private Image textfileicon;
    private Image csvfileicon;
    private Image excelfileicon;
    private Image jsonfileicon;
    private Image xmlfileicon;    
    private Image jsonicon;
    private Image excelicon;
    private Image xmlicon;
    
    
    List txtfilesList = new ArrayList();
    List excelfilesList = new ArrayList();
    List jsonfilesList = new ArrayList();
    List csvfilesList = new ArrayList();
    List xmlfilesList = new ArrayList();
    

    public LoadFlatFilesTreeView(TreeView flatfiletree) {
        
//        flatfiletree.getChildrenUnmodifiable.clear();
        this.flatfiletree = flatfiletree;
        ImageView rootIcon = new ImageView(new Image(getClass().getResourceAsStream("/icon/fficon.png")));
        flatfiles = new TreeItem<>("Flat Files",rootIcon);
        flatfiles.setExpanded(true);
        
        
        textcsvicon = new Image(getClass().getResourceAsStream("/icon/txticon.png")); 
        csvicon = new Image(getClass().getResourceAsStream("/icon/csvicon.png"));
        jsonicon = new Image(getClass().getResourceAsStream("/icon/jsonicon.png"));
        excelicon = new Image(getClass().getResourceAsStream("/icon/excelicon.png"));
        xmlicon = new Image(getClass().getResourceAsStream("/icon/xmlicon.png"));
        
        
        textfileicon = new Image(getClass().getResourceAsStream("/icon/txtfileicon.png")); 
        csvfileicon = new Image(getClass().getResourceAsStream("/icon/csvfileicon.png")); 
        excelfileicon = new Image(getClass().getResourceAsStream("/icon/excelfileicon.png")); 
        jsonfileicon = new Image(getClass().getResourceAsStream("/icon/jsonfileicon.png"));
        xmlfileicon = new Image(getClass().getResourceAsStream("/icon/xmlfileicon.png"));
        
        
        textcsvfiles = new TreeItem<>("TEXT",new ImageView(textcsvicon));
        csv = new TreeItem<>("CSV",new ImageView(csvicon));
        json = new TreeItem<>("JSON",new ImageView(jsonicon));
        excel = new TreeItem<>("EXCEL",new ImageView(excelicon));
        xml = new TreeItem<>("XML",new ImageView(xmlicon));
     
        flatfiles.getChildren().add(textcsvfiles);
        flatfiles.getChildren().add(csv);
        flatfiles.getChildren().add(json);
        flatfiles.getChildren().add(excel);
        flatfiles.getChildren().add(xml);
          System.out.println("Flat Files: "+flatfiles.getValue());
        flatfiletree.setRoot(flatfiles);
      
       
        getFileNames();
        
        addtxtfilestree(txtfilesList);
        addxmlfilestree(xmlfilesList);
        addexcelfilestree(excelfilesList);
        addjsonfilestree(jsonfilesList);
        addcsvfilestree(csvfilesList);
        
    }
    
    public void getFileNames(){
        
        
        System.out.println("Entered - getFileNames");
        
        List<File> files= listf(connpath);
        
        System.out.println("FileList :"+files);
        
        for(File f: files){
            if(f.getName().toLowerCase().contains(".txt")){
                txtfilesList.add(f.getName());
            }
            else if(f.getName().contains(".xls") || f.getName().contains(".xlsx")){
                excelfilesList.add(f.getName());
            }
            else if(f.getName().contains(".xml")){
                xmlfilesList.add(f.getName());
            }
            else if(f.getName().contains(".csv")){
                csvfilesList.add(f.getName());
            }
            else if(f.getName().contains(".json")){
                jsonfilesList.add(f.getName());
            }
            //else{
              //  new ExceptionUI(new Exception("Unrecognized File Format"));
            //}
                
        }
        
        System.out.println("jsonfilesList : "+jsonfilesList);
        System.out.println("csvfilesList : "+csvfilesList);
        System.out.println("txtfilesList : "+txtfilesList);
        System.out.println("xmlfilesList : "+xmlfilesList);
        System.out.println("excelfilesList : "+excelfilesList);
        
        
    }
    
    public static List<File> listf(String directoryName) {
        
         System.out.println("Entered - listf");
        
        File directory = new File(directoryName);

        List<File> resultList = new ArrayList<File>();

        // get all the files from a directory
        File[] fList = directory.listFiles();
        resultList.addAll(Arrays.asList(fList));
        for (File file : fList) {
            if (file.isFile()) {
                System.out.println(file.getAbsolutePath());
            } else if (file.isDirectory()) {
                resultList.addAll(listf(file.getAbsolutePath()));
            }
        }
        
        return resultList;
    } 
    
    public void addtxtfilestree(List l) {

        TreeItem item = null;

        for (Object a : l) {

            item = new TreeItem<>(a.toString(), new ImageView(textfileicon));
            textcsvfiles.getChildren().add(item);
        }
    }
    
    public void addcsvfilestree(List l){
        
        TreeItem item = null;

        for (Object a : l) {

            item = new TreeItem<>(a.toString(), new ImageView(csvfileicon));
            csv.getChildren().add(item);
        }

    }
    public void addjsonfilestree(List l){
        
        TreeItem item = null;

        for (Object a : l) {

            item = new TreeItem<>(a.toString(), new ImageView(jsonfileicon));
            json.getChildren().add(item);
        }

        
    }
    public void addxmlfilestree(List l){
        
        TreeItem item = null;

        for (Object a : l) {

            item = new TreeItem<>(a.toString(), new ImageView(xmlfileicon));
            xml.getChildren().add(item);
        }
 
    }
    public void addexcelfilestree(List l){
        
        TreeItem item = null;

        for (Object a : l) {

            item = new TreeItem<>(a.toString(), new ImageView(excelfileicon));
            excel.getChildren().add(item);
        }
        
        
    }
    
    public void appendConnectionTreeView(String filename) {
       
        
        if(filename.contains("txt")){
            List l = new ArrayList();
            l.add(filename);
            addtxtfilestree(l);
            textcsvfiles.setExpanded(true);
        }
        else if(filename.contains("xml")){
            List l = new ArrayList();
            l.add(filename);
            addxmlfilestree(l);
            xml.setExpanded(true);
        }
        else if(filename.contains("json")){
            List l = new ArrayList();
            l.add(filename);
            addjsonfilestree(l);
            json.setExpanded(true);
        }
        else if(filename.contains("csv")){
            List l = new ArrayList();
            l.add(filename);
            addcsvfilestree(l);
            csv.setExpanded(true);
        }
        else if(filename.contains("xls")||filename.contains("xlsx")){
            List l = new ArrayList();
            l.add(filename);
            addexcelfilestree(l);
            excel.setExpanded(true);
        }
        
    }
    
}
    
