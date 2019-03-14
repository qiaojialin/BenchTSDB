package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Record;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.util.FileManager;
import org.apache.jena.vocabulary.VCARD;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * for sensor data in RDF format, .n3 = .ttl
 * <p>
 * reference: https://jena.apache.org/documentation/io/
 */
public class RDFParser implements IParser {


    @Override
    public List<Record> parse(String fileName) {

        List<Record> records = new ArrayList<>();

        // create an empty Model
        Model model = ModelFactory.createDefaultModel();

        final Property omowl = model.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#");
        final Property rdfs = model.createProperty("http://www.w3.org/2000/01/rdf-schema#");
        final Property sensobs = model.createProperty("http://knoesis.wright.edu/ssw/");
        final Property owltime = model.createProperty("http://www.w3.org/2006/time#");
        final Property owl = model.createProperty("http://www.w3.org/2002/07/owl#");
        final Property xsd = model.createProperty("http://www.w3.org/2001/XMLSchema#");
        final Property weather = model.createProperty("http://knoesis.wright.edu/ssw/ont/weather.owl#");
        final Property rdf = model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#");

        InputStream is = FileManager.get().open(fileName);
        if (is != null) {
            model.read(is, null, "Turtle");
            StmtIterator iter = model.listStatements();

            while (iter.hasNext()) {
                Statement stmt = iter.nextStatement();
                convertRecord(stmt);
            }
        }
        return records;
    }


    private void convertRecord(Statement stmt) {
        Resource subject = stmt.getSubject();     // get the subject
        Property predicate = stmt.getPredicate();   // get the predicate
        RDFNode object = stmt.getObject();      // get the object



        if (object instanceof Resource) {
            System.out.print("主语 " + subject + "\t");
            System.out.print(" 谓语 " + predicate + "\t");
            System.out.print(object.toString());
//            System.out.println();
        } else {
            System.out.print("主语 " + subject + "\t");
            System.out.print(" 谓语 " + predicate + "\t");
            System.out.print(" \"" + object.toString() + "\"");
            System.out.println();
        }

    }


    /**
     * ChronixTS Jena
     */
    public static void main(String... args) {
        String personURI = "http://somewhere/JohnSmith";
        String givenName = "John";
        String familyName = "Smith";
        String fullName = givenName + " " + familyName;

        // create an empty Model
        Model model = ModelFactory.createDefaultModel();

        // create the resource and add the properties cascading style
        model.createResource(personURI)
                .addProperty(VCARD.FN, fullName)
                .addProperty(VCARD.N,
                        model.createResource()
                                .addProperty(VCARD.Given, givenName)
                                .addProperty(VCARD.Family, familyName));

        StmtIterator iter = model.listStatements();

        // print out the predicate, subject and object of each statement
        while (iter.hasNext()) {
            Statement stmt = iter.nextStatement();  // get next statement
            Resource subject = stmt.getSubject();     // get the subject
            Property predicate = stmt.getPredicate();   // get the predicate
            RDFNode object = stmt.getObject();      // get the object

            System.out.print(subject.toString());
            System.out.print(" " + predicate.toString() + " ");
            if (object instanceof Resource) {
                System.out.print(object.toString());
            } else {
                // object is a literal
                System.out.print(" \"" + object.toString() + "\"");
            }

            System.out.println(" .");
        }

        model.write(System.out);
    }

}
