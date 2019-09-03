package autodao.struct.xml;

public class TestStructure {
    public static void main(String[] args) throws Exception {
       System.out.println(XmlFormatter.format(Structure.createXml(true, false, "jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres")));
    }
}