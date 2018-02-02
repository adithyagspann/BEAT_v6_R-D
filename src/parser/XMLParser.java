package parser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author Adithya
 */

/*
 * To Read/Write the data in XML using DOM Builders
 */
public class XMLParser {

    private DocumentBuilderFactory dbFactory;
    private DocumentBuilder dBuilder;
    private Document doc;
    private NodeList nodeList;

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(XMLParser.class);
    private String key;
    private String value;
    private boolean dataInsertCheck;

    public XMLParser() throws ParserConfigurationException {

        dbFactory = DocumentBuilderFactory.newInstance();
        dBuilder = dbFactory.newDocumentBuilder();
        //now XML is loaded as Document in memory, lets convert it to Object List
    }

    /*
     * Used to Read the XML File based on the Tags & Attributes passed
     */
    List<String> tableColumns;

    public List xmlAttributesReader(String xmlFile, String xmlTag, Map<String, String> attributes) throws SAXException, ParserConfigurationException, IOException {
        LOGGER.info("Reading attributes from XML File: " + xmlFile);
        List<String> tableProperties = new ArrayList<>();

        List<List> tableColumnsall = new ArrayList<>();

        doc = dBuilder.parse(xmlFile);
        doc.getDocumentElement().normalize();
        nodeList = doc.getElementsByTagName(xmlTag);
        List schema = new ArrayList<>();

        System.out.println("Replaceing the Data: " + nodeList.getLength());
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element rootElementUpdate = (Element) nodeList.item(i);
            Node node = nodeList.item(i);
            NodeList tableList = node.getChildNodes(); //Gets Table Nodes
            System.out.println("Node List: " + node.hasChildNodes());
            for (int j = 0; j < tableList.getLength(); j++) {

                if (!tableList.item(j).getNodeName().contains("#")) {
                    System.out.println("Data: " + tableList.item(j).getNodeName());
                    //Gets Attributes of Table Node
                    NamedNodeMap tableAttr = tableList.item(j).getAttributes();
                    System.out.println("Table Attr: " + tableList.item(j).hasAttributes());
                    for (Map.Entry<String, String> entry : attributes.entrySet()) {
                        key = entry.getKey();
                        value = entry.getValue();

                    }
                    for (int k = 0; k < tableAttr.getLength(); k++) {

                        if (tableAttr.item(k).getNodeName().equalsIgnoreCase(key) && tableAttr.item(k).getNodeValue().equalsIgnoreCase(value)) {

                            Element childElement = (Element) tableList.item(j);
                            StringBuffer xmlAttributes = new StringBuffer();
                            for (int l = 0; l < tableAttr.getLength(); l++) {
                                if (l == tableAttr.getLength() - 1) {
                                    xmlAttributes.append("'").append(tableAttr.item(l).getNodeName()).append("'").append("#").append("'").append(tableAttr.item(l).getNodeValue()).append("'");
//                                    xmlAttributes.append(childElement.getAttribute(attributes.get(j).get(k)));
                                } else {
                                    xmlAttributes.append("'").append(tableAttr.item(l).getNodeName()).append("'").append("#").append("'").append(tableAttr.item(l).getNodeValue()).append("'").append("\n");
//                                    xmlAttributes.append(childElement.getAttribute(attributes.get(j).get(k))).append("#");
                                }
//                               
                            }
                            tableProperties.add(xmlAttributes.toString());

                            if (childElement.hasChildNodes()) {
                                NodeList columnList = tableList.item(j).getChildNodes();
                                System.out.println("childElement.hasChildNodes()" + columnList.getLength());
                                for (int l = 0; l < columnList.getLength(); l++) {
                                    StringBuffer xmlColumnAttributes = new StringBuffer();
                                    System.out.println("columnNode: " + columnList.item(l).getNodeName());
                                    if (!columnList.item(l).getNodeName().contains("#")) {

                                        System.out.println("columnNode1: " + columnList.item(l).getNodeName());
                                        tableColumns = new ArrayList<>();
                                        Element columnNode = (Element) columnList.item(l);
                                        NamedNodeMap colAttrList = columnList.item(l).getAttributes();
                                        System.out.println("colAttrList.getLength(): " + colAttrList.getLength());
                                        for (int m = 0; m < colAttrList.getLength(); m++) {

                                            if (m == colAttrList.getLength() - 1) {
                                                xmlColumnAttributes.append("'").append(colAttrList.item(m).getNodeName()).append("'").append("#").append("'").append(colAttrList.item(m).getNodeValue()).append("'");
                                            } else {
                                                xmlColumnAttributes.append("'").append(colAttrList.item(m).getNodeName()).append("'").append("#").append("'").append(colAttrList.item(m).getNodeValue()).append("'").append("\n");
                                            }
                                        }
                                        tableColumns.add(xmlColumnAttributes.toString());
//                                        if (tableColumns != null && !tableColumns.isEmpty()) {
                                        tableColumnsall.add(tableColumns);
//                                        }
                                    }

//                                    break;
                                }
                            }

                        }

                    }

                }
            }
        }
        schema.add(tableProperties);

        schema.add(tableColumnsall);
        return schema;
    }

    public String getXmlRootElement(String xmlFile) throws SAXException, IOException {
        File checkFile = new File(xmlFile);
        if (checkFile.exists()) {
            doc = dBuilder.parse(xmlFile);
            doc.getDocumentElement().normalize();
//            checkFile.delete();
            return doc.getDocumentElement().getNodeName();
        } else {
            File schemFile = new File(xmlFile);
            if (!schemFile.exists()) {
                schemFile.createNewFile();
            }
        }

        return "";
    }

    private static Node setXMLAttributes(Document doc, String element, String[] data) {
        LOGGER.info("setXMLAttributes called");
        Element node = doc.createElement(element);
        for (int l = 0; l < data.length; l++) {

//        node.appendChild(doc.createTextNode(value));
            node.setAttribute(data[l].split("#")[0].replace("'", ""), data[l].split("#")[1].replace("'", ""));
        }
        return node;
    }
    int checkCount = 0;

    public void xmlAttributesWriter(String xmlFile, String rootTag, String masterTag, String childTag, List<List<String>> metaDataAttributes) throws SAXException, IOException, TransformerConfigurationException, TransformerException {

        doc = dBuilder.newDocument();
        String rootElement = getXmlRootElement(xmlFile);

        if (rootElement.isEmpty()) {
            Element newRootElement = doc.createElement(rootTag);
            doc.appendChild(newRootElement);

            Element masterElement = doc.createElement(masterTag);
            newRootElement.appendChild(masterElement);

            System.out.println("Meta Length: " + metaDataAttributes.get(0).size());

            for (int i = 0; i < metaDataAttributes.get(0).size(); i++) {
                String[] data = metaDataAttributes.get(0).get(i).split("\n");

                masterElement.setAttribute(data[0].split("#")[0].replace("'", ""), data[0].split("#")[1].replace("'", ""));
            }

            for (int i = 0; i < metaDataAttributes.get(1).size(); i++) {
                String[] data = metaDataAttributes.get(1).get(i).split("\n");
//                masterElement.setAttribute(data[0].split("#")[0].replace("'", ""), data[0].split("#")[1].replace("'", ""));

                masterElement.appendChild(setXMLAttributes(doc, childTag, data));
            }
//            for (Map.Entry<String, String> attrData : metaDataAttributes.get(0).entrySet()) {
//
//                masterElement.setAttribute(attrData.getKey(), attrData.getValue());
//
//            }
//
//            for (Map.Entry<String, String> attrData : metaDataAttributes.get(1).entrySet()) {
//
//                masterElement.appendChild(setXMLAttributes(doc, childTag, attrData.getKey(), attrData.getValue()));
//
//            }

        } else {

            //Logic to replace the existing metadata
            NodeList rootList = doc.getElementsByTagName(rootTag);
            System.out.println("Replaceing the Data: " + rootList.getLength());
            for (int i = 0; i < rootList.getLength(); i++) {
                Element rootElementUpdate = (Element) rootList.item(i);
                Node node = rootList.item(i);
                NodeList nodeList = node.getChildNodes();
                System.out.println("Node List: " + node.hasChildNodes());
                for (int j = 0; j < nodeList.getLength(); j++) {

                    if (!nodeList.item(j).getNodeName().contains("#")) {
                        System.out.println("Data: " + nodeList.item(j).getNodeName());
                        //Gets Attributes of Table Node
                        NamedNodeMap nodeAttr = nodeList.item(j).getAttributes();
                        System.out.println("NodeAttr: " + nodeList.item(j).hasAttributes());

                        for (int k = 0; k < nodeAttr.getLength(); k++) {
                            for (int l = 0; l < metaDataAttributes.get(0).size(); l++) {

                                String[] data = metaDataAttributes.get(0).get(l).split("\n");
//                masterElement.setAttribute(data[0].split("#")[0], data[0].split("#")[1]);
                                key = data[0].split("#")[0].replace("'", "");
                                value = data[0].split("#")[1].replace("'", "");
                                break;
                            }
//                            for (Map.Entry<String, String> entry : metaDataAttributes.get(0).entrySet()) {
//                                key = entry.getKey();
//                                value = entry.getValue();
//                                break;
//                            }
//                            System.out.println("Key: " + key + "\n Value: " + value);
//                            System.out.println("Table Data: " + nodeAttr.item(k).getNodeName() + "##" + nodeAttr.item(k).getNodeValue());
                            if (nodeAttr.item(k).getNodeName().equalsIgnoreCase(key) && nodeAttr.item(k).getNodeValue().equalsIgnoreCase(value)) {
//                                System.out.println("Table Data: " + nodeAttr.item(k).getNodeName() + "##" + nodeAttr.item(k).getNodeValue());
                                System.out.println("nodeList.item(j): " + nodeList.item(j));
                                rootList.item(i).removeChild(nodeList.item(j));

                                Element masterElement = doc.createElement(masterTag);
                                rootElementUpdate.appendChild(masterElement);

                                System.out.println("Meta Length: " + metaDataAttributes.get(0).size());

//                                for (Map.Entry<String, String> attrData : metaDataAttributes.get(0).entrySet()) {
//
//                                    masterElement.setAttribute(attrData.getKey(), attrData.getValue());
//
//                                }
//
//                                for (Map.Entry<String, String> attrData : metaDataAttributes.get(1).entrySet()) {
//
//                                    masterElement.appendChild(setXMLAttributes(doc, childTag, attrData.getKey(), attrData.getValue()));
//
//                                }
                                for (int m = 0; m < metaDataAttributes.get(0).size(); m++) {
                                    String[] data = metaDataAttributes.get(0).get(m).split("\n");
                                    System.out.println(data[0].split("#")[0].replace("'", "") + "::" + data[0].split("#")[1].replace("'", ""));
                                    masterElement.setAttribute(data[0].split("#")[0].replace("'", ""), data[0].split("#")[1].replace("'", ""));
                                }

                                for (int m = 0; m < metaDataAttributes.get(1).size(); m++) {
                                    System.out.println("Size: " + metaDataAttributes.get(1).size());
                                    String[] data = metaDataAttributes.get(1).get(m).split("\n");
//                masterElement.setAttribute(data[0].split("#")[0].replace("'", ""), data[0].split("#")[1].replace("'", ""));

                                    masterElement.appendChild(setXMLAttributes(doc, childTag, data)); // data[l].split("#")[0].replace("'", ""), data[l].split("#")[1].replace("'", "")));

                                }
                                dataInsertCheck = true;

                            }
                            break;
                        }
                        if (!dataInsertCheck) {
                            Element masterElement = doc.createElement(masterTag);
                            rootElementUpdate.appendChild(masterElement);

                            System.out.println("Meta Length: " + metaDataAttributes.get(0).size());
                            for (int m = 0; m < metaDataAttributes.get(0).size(); m++) {
                                String[] data = metaDataAttributes.get(0).get(m).split("\n");
                                masterElement.setAttribute(data[0].split("#")[0].replace("'", ""), data[0].split("#")[1].replace("'", ""));
                            }

                            for (int m = 0; m < metaDataAttributes.get(1).size(); m++) {
                                System.out.println("Size: " + metaDataAttributes.get(1).size());
                                String[] data = metaDataAttributes.get(1).get(m).split("\n");
//                masterElement.setAttribute(data[0].split("#")[0].replace("'", ""), data[0].split("#")[1].replace("'", ""));
                                masterElement.appendChild(setXMLAttributes(doc, childTag, data));
//masterElement.appendChild(setXMLAttributes(doc, childTag, data[0].split("#")[0].replace("'", ""), data[0].split("#")[1].replace("'", "")));
                            }
//                            for (Map.Entry<String, String> attrData : metaDataAttributes.get(0).entrySet()) {
//
//                                masterElement.setAttribute(attrData.getKey(), attrData.getValue());
//
//                            }
//
//                            for (Map.Entry<String, String> attrData : metaDataAttributes.get(1).entrySet()) {
//
//                                masterElement.appendChild(setXMLAttributes(doc, childTag, attrData.getKey(), attrData.getValue()));
//
//                            }
                        }

                    }

                }
            }
        }
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        DOMSource source = new DOMSource(doc);
//        StreamResult console = new StreamResult(System.out);
        StreamResult file = new StreamResult(new File(xmlFile));
//        transformer.transform(source, console);
        transformer.transform(source, file);

    }

    public static void main(String args[]) throws SAXException, ParserConfigurationException, IOException, TransformerException {

        XMLParser lParser = new XMLParser();

        List data = new ArrayList<>();
//
//        data.add("name");
//        data.add("type");
//        data.add("size");
//        data.add("size1");
//        List main = new ArrayList<>();
        Map<String, String> main = new HashMap<>();
        main.put("name", "Orders_Source.csv");
//        main.add(data);
//        List data1 = lParser.xmlAttributesReader("C:\\Users\\Admin\\Desktop\\SCHEMA.xml", "schema", main);
//        for (int i = 0; i < data1.size(); i++) {
//            System.out.println(data1.get(i));
//        }

//        Map<String, String> data1 = new HashMap<String, String>();
//        data1.put("col", "Order_name");
//        data1.put("col1", "Order_name1");
//        data1.put("col2", "Order_name1");
//        data1.put("col3", "Order_name1");
        List<String> columnsAttr = new ArrayList<String>();

        columnsAttr.add("'name'#'Order_name'\n"
                + ""
                + "'type'#'varchar'");
        columnsAttr.add("'name'#'Order_ID'\n"
                + ""
                + "'type'#'INT'");

//        Map<String, String> dataTable = new HashMap<String, String>();
        List dataTable = new ArrayList<>();
        dataTable.add("'name'#'Order_'");
        dataTable.add("'seperator'#','");
//        dataTable.put("name1", "Order_name1");
        data.add(dataTable);
        data.add(columnsAttr);

//        main.add(data);
        lParser.xmlAttributesWriter("C:\\Users\\Admin\\Desktop\\SCHEMA2.xml", "schema", "table", "column", data);
//        System.out.println("Data Lenght: " + lParser.nodeList.getLength());
//        Element element = (Element) lParser.nodeList.item(0);
//        System.out.println(element.getAttribute("name"));
//        System.out.println(element.getNodeName() + " :Element: " + element.getChildNodes().getLength());
////            NodeList nl = element.getChildNodes();
//            System.out.println("NodeList 1:"+nl.getLength());
//            Element el1 = (Element)nl.item(0);
//            System.out.println("Element 1: "+el1.getNodeName());
//            NodeList nl = element.getElementsByTagName("column").item(0).getChildNodes();
//            System.out.println("Node List: " + nl.item(0).getNodeValue());
    }

}
