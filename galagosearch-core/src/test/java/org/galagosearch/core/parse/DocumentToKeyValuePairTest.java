
package org.galagosearch.core.parse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import junit.framework.TestCase;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class DocumentToKeyValuePairTest extends TestCase {
    
    public DocumentToKeyValuePairTest(String testName) {
        super(testName);
    }

    public class KeyValuePairProcessor implements KeyValuePair.Processor {
        KeyValuePair pair;

        public void process(KeyValuePair pair) {
            this.pair = pair;
        }

        public void close() throws IOException {
        }
    }

    public void testProcess() throws Exception {
        DocumentToKeyValuePair dkvp = new DocumentToKeyValuePair();
        KeyValuePairProcessor kvpProcessor = new KeyValuePairProcessor();
        dkvp.setProcessor(kvpProcessor);

        Document document = new Document();
        document.text = "This is text.";
        document.identifier = "DOC2";
        document.metadata.put("this", "that");
        dkvp.process(document);

        KeyValuePair pair = kvpProcessor.pair;
        assertEquals(Utility.makeString(pair.key), "DOC2");

        ByteArrayInputStream stream = new ByteArrayInputStream(pair.value);
        ObjectInputStream input = new ObjectInputStream(stream);
        Document result = (Document) input.readObject();

        assertEquals(result.text, document.text);
        assertEquals(result.identifier, document.identifier);
        assertEquals(result.metadata.size(), document.metadata.size());
        assertEquals(result.metadata.get("this"), "that");
    }
}
