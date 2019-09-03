package autodao.struct.xml;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class Structure {
    
    synchronized public static String createXml(boolean bShowOrder, boolean bShowRows, String url, String uname, String pass)
            throws Exception {
        DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
        Document document = documentBuilder.newDocument();
        
        Element elRoot = document.createElement("database");
        Attr attr = document.createAttribute("name");
        attr.setValue("inspector");
        elRoot.setAttributeNode(attr);
        document.appendChild(elRoot);
        
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection(url, uname, pass);
        DatabaseMetaData md = conn.getMetaData();
        Statement st = conn.createStatement();
        
        Map<String, Set<String>> map = new HashMap<>();
        List<String> list = new ArrayList<>();
        List<Element> elements = new ArrayList<>();
        
        ResultSet rs = md.getTables("public", null, "%", new String[] { "TABLE" });
        List<String> columns = new ArrayList<>();
        List<String> pks = new ArrayList<>();
        List<String[]> fks = new ArrayList<>();
        while (rs.next()) {
            String tableName = rs.getString(3);
            
            Element elTable = document.createElement("table");
            //elRoot.appendChild(elTable);
            elements.add(elTable);
            attr = document.createAttribute("name");
            attr.setValue(tableName);
            elTable.setAttributeNode(attr);
            
            ResultSet rs2 = md.getColumns("public", null, tableName, null);
            columns.clear();
            pks.clear();
            fks.clear();
            ResultSet primaryKeys = md.getPrimaryKeys(null, "public", tableName);
            while (primaryKeys.next()) {
                String pk = primaryKeys.getString("COLUMN_NAME");
                pks.add(pk);
            }
            
            ResultSet foreignKeys = md.getImportedKeys(null, "public", tableName);
            while (foreignKeys.next()) {
                fks.add(new String[] { foreignKeys.getString(8), foreignKeys.getString(3), foreignKeys.getString(4) });
            }
            
            Set<String> set = new HashSet<>();
            while (rs2.next()) {
                String name = rs2.getString("COLUMN_NAME");
                String type = rs2.getString("TYPE_NAME");
                int size = rs2.getInt("COLUMN_SIZE");
                columns.add(name);
                Element elColumn = document.createElement("column");
                elTable.appendChild(elColumn);
                attr = document.createAttribute("name");
                attr.setValue(name);
                elColumn.setAttributeNode(attr);
                attr = document.createAttribute("type");
                attr.setValue(type);
                elColumn.setAttributeNode(attr);
                attr = document.createAttribute("size");
                attr.setValue(Integer.toString(size));
                elColumn.setAttributeNode(attr);
                if (pks.contains(name)) {
                    attr = document.createAttribute("pk");
                    attr.setValue("true");
                    elColumn.setAttributeNode(attr);
                }
                for (String[] sa : fks) {
                    if (sa[0].equals(name)) {
                        Element elFks = document.createElement("foreignKey");
                        elColumn.appendChild(elFks);
                        attr = document.createAttribute("name");
                        attr.setValue(sa[0]);
                        elFks.setAttributeNode(attr);
                        attr = document.createAttribute("table");
                        attr.setValue(sa[1]);
                        set.add(sa[1]);
                        elFks.setAttributeNode(attr);
                    }
                }
            }
            map.put(tableName, set);
            
            ResultSet rsi = st.executeQuery(String.format(
                    "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename = '%s'", tableName));
            while (rsi.next()) {
                Element elIndex = document.createElement("index");
                elTable.appendChild(elIndex);
                attr = document.createAttribute("name");
                attr.setValue(rsi.getString(1));
                elIndex.setAttributeNode(attr);
                
                String sd = rsi.getString(2);
                if (sd.contains("UNIQUE")) {
                    attr = document.createAttribute("unique");
                    attr.setValue("true");
                    elIndex.setAttributeNode(attr);
                }
                for (String s0 : sd.split("[(]")[1].replace(")", "").split(",")) {
                    Element elColumnIdx = document.createElement("column");
                    elIndex.appendChild(elColumnIdx);
                    attr = document.createAttribute("name");
                    String[] sa = s0.trim().split(" ");
                    attr.setValue(sa[0]);
                    elColumnIdx.setAttributeNode(attr);
                    if (s0.contains("DESC")) {
                        attr = document.createAttribute("desc");
                        attr.setValue("true");
                        elColumnIdx.setAttributeNode(attr);
                    }
                    if (s0.contains("NULLS")) {
                        attr = document.createAttribute("nullable");
                        attr.setValue("true");
                        elColumnIdx.setAttributeNode(attr);
                    }
                    if (s0.contains("LAST")) {
                        attr = document.createAttribute("last");
                        attr.setValue("true");
                        elColumnIdx.setAttributeNode(attr);
                    }
                    
                }
            }
            
            if (bShowRows) {
                ResultSet rsData = st.executeQuery(String.format("SELECT * FROM %s", tableName));
                while (rsData.next()) {
                    Element elRow = document.createElement("row");
                    elTable.appendChild(elRow);
                    for (String colName : columns) {
                        attr = document.createAttribute(colName);
                        attr.setValue(rsData.getString(colName));
                        elRow.setAttributeNode(attr);
                    }
                }
            }
        }
        
        rs = md.getTables("public", null, "%", new String[] { "SEQUENCE" });
        while (rs.next())
        
        {
            String sName = rs.getString(3);
            
            Element elTable = document.createElement("sequence");
            attr = document.createAttribute("name");
            attr.setValue(sName);
            elTable.setAttributeNode(attr);
            ResultSet rss = st.executeQuery("SELECT last_value, start_value, increment_by FROM " + sName);
            if (rss.next()) {
                attr = document.createAttribute("last_value");
                attr.setValue(rss.getString(1));
                elTable.setAttributeNode(attr);
                attr = document.createAttribute("start_value");
                attr.setValue(rss.getString(2));
                elTable.setAttributeNode(attr);
                attr = document.createAttribute("increment_by");
                attr.setValue(rss.getString(3));
                elTable.setAttributeNode(attr);
            }
            elRoot.appendChild(elTable);
        }
        
        for (String s : map.keySet()) {
            if (map.get(s).size() == 0) {
                list.add(0, s);
            } else {
                list.add(s);
            }
        }
        
        int sz = list.size();
        for (int j = 0; j < sz; j++) {
            for (int i = 0; i < sz; i++) {
                String s = list.get(i);
                for (String s2 : map.get(s)) {
                    int is = list.indexOf(s);
                    int is2 = list.indexOf(s2);
                    if (is2 > is) {
                        list.set(is2, s);
                        list.set(is, s2);
                    }
                }
            }
        }
        
        if (bShowOrder) {
            int cnt = 0;
            for (String s : list) {
                Element elOrder = document.createElement("order");
                attr = document.createAttribute("cnt");
                attr.setValue(Integer.toString(cnt++));
                elOrder.setAttributeNode(attr);
                attr = document.createAttribute("name");
                attr.setValue(s);
                elOrder.setAttributeNode(attr);
                elRoot.appendChild(elOrder);
                if (map.get(s).size() > 0) {
                    for (String s2 : map.get(s)) {
                        Element elParent = document.createElement("parent");
                        attr = document.createAttribute("name");
                        attr.setValue(s2);
                        elParent.setAttributeNode(attr);
                        elOrder.appendChild(elParent);
                    }
                }
            }
        }
        
        for (Element el : elements) {
            String name = el.getAttribute("name");
            int id = list.indexOf(name);
            attr = document.createAttribute("order");
            attr.setValue(Integer.toString(id));
            el.setAttributeNode(attr);
            elRoot.appendChild(el);
        }
        
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(document), new StreamResult(writer));
        String output = writer.getBuffer().toString().replaceAll("\n|\r", "");
        
        return output;
    }
}
