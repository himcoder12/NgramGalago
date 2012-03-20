// This file was automatically generated with the command: 
//     java org.galagosearch.tupleflow.typebuilder.TypeBuilderMojo ...
package org.galagosearch.core.types;

import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.ArrayInput;
import org.galagosearch.tupleflow.ArrayOutput;
import org.galagosearch.tupleflow.Order;   
import org.galagosearch.tupleflow.OrderedWriter;
import org.galagosearch.tupleflow.Type; 
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.Step; 
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.ReaderSource;
import java.io.IOException;             
import java.io.EOFException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;   
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Collection;

public class NumberWordPosition implements Type<NumberWordPosition> {
    public int document;
    public byte[] word;
    public int position; 
    
    public NumberWordPosition() {}
    public NumberWordPosition(int document, byte[] word, int position) {
        this.document = document;
        this.word = word;
        this.position = position;
    }  
    
    public String toString() {
        try {
            return String.format("%d,%s,%d",
                                   document, new String(word, "UTF-8"), position);
        } catch(UnsupportedEncodingException e) {
            throw new RuntimeException("Couldn't convert string to UTF-8.");
        }
    } 

    public Order<NumberWordPosition> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+word", "+document", "+position" })) {
            return new WordDocumentPositionOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.galagosearch.tupleflow.Processor<NumberWordPosition> {
        public void process(NumberWordPosition object) throws IOException;
        public void close() throws IOException;
    }                        
    public interface Source extends Step {
    }
    public static class WordDocumentPositionOrder implements Order<NumberWordPosition> {
        public int hash(NumberWordPosition object) {
            int h = 0;
            h += Utility.hash(object.word);
            h += Utility.hash(object.document);
            h += Utility.hash(object.position);
            return h;
        } 
        public Comparator<NumberWordPosition> greaterThan() {
            return new Comparator<NumberWordPosition>() {
                public int compare(NumberWordPosition one, NumberWordPosition two) {
                    int result = 0;
                    do {
                        result = + Utility.compare(one.word, two.word);
                        if(result != 0) break;
                        result = + Utility.compare(one.document, two.document);
                        if(result != 0) break;
                        result = + Utility.compare(one.position, two.position);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<NumberWordPosition> lessThan() {
            return new Comparator<NumberWordPosition>() {
                public int compare(NumberWordPosition one, NumberWordPosition two) {
                    int result = 0;
                    do {
                        result = + Utility.compare(one.word, two.word);
                        if(result != 0) break;
                        result = + Utility.compare(one.document, two.document);
                        if(result != 0) break;
                        result = + Utility.compare(one.position, two.position);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<NumberWordPosition> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<NumberWordPosition> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<NumberWordPosition> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static class OrderedWriterClass extends OrderedWriter< NumberWordPosition > {
            NumberWordPosition last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(NumberWordPosition object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != Utility.compare(object.word, last.word)) { processAll = true; shreddedWriter.processWord(object.word); }
               if (processAll || last == null || 0 != Utility.compare(object.document, last.document)) { processAll = true; shreddedWriter.processDocument(object.document); }
               if (processAll || last == null || 0 != Utility.compare(object.position, last.position)) { processAll = true; shreddedWriter.processPosition(object.position); }
               shreddedWriter.processTuple();
               last = object;
            }           
                 
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<NumberWordPosition> getInputClass() {
                return NumberWordPosition.class;
            }
        } 
        public ReaderSource<NumberWordPosition> orderedCombiner(Collection<TypeReader<NumberWordPosition>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList();
            
            for (TypeReader<NumberWordPosition> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public NumberWordPosition clone(NumberWordPosition object) {
            NumberWordPosition result = new NumberWordPosition();
            if (object == null) return result;
            result.document = object.document; 
            result.word = object.word; 
            result.position = object.position; 
            return result;
        }                 
        public Class<NumberWordPosition> getOrderedClass() {
            return NumberWordPosition.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+word", "+document", "+position"};
        }

        public static String getSpecString() {
            return "+word +document +position";
        }
                           
        public interface ShreddedProcessor extends Step {
            public void processWord(byte[] word) throws IOException;
            public void processDocument(int document) throws IOException;
            public void processPosition(int position) throws IOException;
            public void processTuple() throws IOException;
            public void close() throws IOException;
        }    
        public interface ShreddedSource extends Step {
        }                                              
        
        public static class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            byte[] lastWord;
            int lastDocument;
            int lastPosition;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        
            
            public void close() throws IOException {
                flush();
            }
            
            public void processWord(byte[] word) {
                lastWord = word;
                buffer.processWord(word);
            }
            public void processDocument(int document) {
                lastDocument = document;
                buffer.processDocument(document);
            }
            public void processPosition(int position) {
                lastPosition = position;
                buffer.processPosition(position);
            }
            public final void processTuple() throws IOException {
                if (lastFlush) {
                    if(buffer.words.size() == 0) buffer.processWord(lastWord);
                    if(buffer.documents.size() == 0) buffer.processDocument(lastDocument);
                    if(buffer.positions.size() == 0) buffer.processPosition(lastPosition);
                    lastFlush = false;
                }
                buffer.processTuple();
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    buffer.incrementTuple();
                }
            }  
            public final void flushWord(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getWordEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeBytes(buffer.getWord());
                    output.writeInt(count);
                    buffer.incrementWord();
                      
                    flushDocument(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public final void flushDocument(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getDocumentEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeInt(buffer.getDocument());
                    output.writeInt(count);
                    buffer.incrementDocument();
                      
                    flushPosition(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public final void flushPosition(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getPositionEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeInt(buffer.getPosition());
                    output.writeInt(count);
                    buffer.incrementPosition();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushWord(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static class ShreddedBuffer {
            ArrayList<byte[]> words = new ArrayList();
            ArrayList<Integer> documents = new ArrayList();
            ArrayList<Integer> positions = new ArrayList();
            ArrayList<Integer> wordTupleIdx = new ArrayList();
            ArrayList<Integer> documentTupleIdx = new ArrayList();
            ArrayList<Integer> positionTupleIdx = new ArrayList();
            int wordReadIdx = 0;
            int documentReadIdx = 0;
            int positionReadIdx = 0;
                            
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processWord(byte[] word) {
                words.add(word);
                wordTupleIdx.add(writeTupleIndex);
            }                                      
            public void processDocument(int document) {
                documents.add(document);
                documentTupleIdx.add(writeTupleIndex);
            }                                      
            public void processPosition(int position) {
                positions.add(position);
                positionTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple() {
                assert words.size() > 0;
                assert documents.size() > 0;
                assert positions.size() > 0;
                writeTupleIndex++;
            }
            public void resetData() {
                words.clear();
                documents.clear();
                positions.clear();
                wordTupleIdx.clear();
                documentTupleIdx.clear();
                positionTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                wordReadIdx = 0;
                documentReadIdx = 0;
                positionReadIdx = 0;
            } 

            public void reset() {
                resetData();
                resetRead();
            } 
            public boolean isFull() {
                return writeTupleIndex >= batchSize;
            }

            public boolean isEmpty() {
                return writeTupleIndex == 0;
            }                          

            public boolean isAtEnd() {
                return readTupleIndex >= writeTupleIndex;
            }           
            public void incrementWord() {
                wordReadIdx++;  
            }                                                                                              

            public void autoIncrementWord() {
                while (readTupleIndex >= getWordEndIndex() && readTupleIndex < writeTupleIndex)
                    wordReadIdx++;
            }                 
            public void incrementDocument() {
                documentReadIdx++;  
            }                                                                                              

            public void autoIncrementDocument() {
                while (readTupleIndex >= getDocumentEndIndex() && readTupleIndex < writeTupleIndex)
                    documentReadIdx++;
            }                 
            public void incrementPosition() {
                positionReadIdx++;  
            }                                                                                              

            public void autoIncrementPosition() {
                while (readTupleIndex >= getPositionEndIndex() && readTupleIndex < writeTupleIndex)
                    positionReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getWordEndIndex() {
                if ((wordReadIdx+1) >= wordTupleIdx.size())
                    return writeTupleIndex;
                return wordTupleIdx.get(wordReadIdx+1);
            }

            public int getDocumentEndIndex() {
                if ((documentReadIdx+1) >= documentTupleIdx.size())
                    return writeTupleIndex;
                return documentTupleIdx.get(documentReadIdx+1);
            }

            public int getPositionEndIndex() {
                if ((positionReadIdx+1) >= positionTupleIdx.size())
                    return writeTupleIndex;
                return positionTupleIdx.get(positionReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public byte[] getWord() {
                assert readTupleIndex < writeTupleIndex;
                assert wordReadIdx < words.size();
                
                return words.get(wordReadIdx);
            }
            public int getDocument() {
                assert readTupleIndex < writeTupleIndex;
                assert documentReadIdx < documents.size();
                
                return documents.get(documentReadIdx);
            }
            public int getPosition() {
                assert readTupleIndex < writeTupleIndex;
                assert positionReadIdx < positions.size();
                
                return positions.get(positionReadIdx);
            }

            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple();
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexWord(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processWord(getWord());
                    assert getWordEndIndex() <= endIndex;
                    copyUntilIndexDocument(getWordEndIndex(), output);
                    incrementWord();
                }
            } 
            public void copyUntilIndexDocument(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processDocument(getDocument());
                    assert getDocumentEndIndex() <= endIndex;
                    copyUntilIndexPosition(getDocumentEndIndex(), output);
                    incrementDocument();
                }
            } 
            public void copyUntilIndexPosition(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processPosition(getPosition());
                    assert getPositionEndIndex() <= endIndex;
                    copyTuples(getPositionEndIndex(), output);
                    incrementPosition();
                }
            }  
            public void copyUntilWord(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + Utility.compare(getWord(), other.getWord());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processWord(getWord());
                                      
                        if (c < 0) {
                            copyUntilIndexDocument(getWordEndIndex(), output);
                        } else if (c == 0) {
                            copyUntilDocument(other, output);
                            autoIncrementWord();
                            break;
                        }
                    } else {
                        output.processWord(getWord());
                        copyUntilIndexDocument(getWordEndIndex(), output);
                    }
                    incrementWord();  
                    
               
                }
            }
            public void copyUntilDocument(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + Utility.compare(getDocument(), other.getDocument());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processDocument(getDocument());
                                      
                        if (c < 0) {
                            copyUntilIndexPosition(getDocumentEndIndex(), output);
                        } else if (c == 0) {
                            copyUntilPosition(other, output);
                            autoIncrementDocument();
                            break;
                        }
                    } else {
                        output.processDocument(getDocument());
                        copyUntilIndexPosition(getDocumentEndIndex(), output);
                    }
                    incrementDocument();  
                    
                    if (getWordEndIndex() <= readTupleIndex)
                        break;   
                }
            }
            public void copyUntilPosition(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + Utility.compare(getPosition(), other.getPosition());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processPosition(getPosition());
                                      
                        copyTuples(getPositionEndIndex(), output);
                    } else {
                        output.processPosition(getPosition());
                        copyTuples(getPositionEndIndex(), output);
                    }
                    incrementPosition();  
                    
                    if (getDocumentEndIndex() <= readTupleIndex)
                        break;   
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilWord(other, output);
            }
            
        }                         
        public static class ShreddedCombiner implements ReaderSource<NumberWordPosition>, ShreddedSource {   
            public ShreddedProcessor processor;
            Collection<ShreddedReader> readers;       
            boolean closeOnExit = false;
            boolean uninitialized = true;
            PriorityQueue<ShreddedReader> queue = new PriorityQueue<ShreddedReader>();
            
            public ShreddedCombiner(Collection<ShreddedReader> readers, boolean closeOnExit) {
                this.readers = readers;                                                       
                this.closeOnExit = closeOnExit;
            }
                                  
            public void setProcessor(Step processor) throws IncompatibleProcessorException {  
                if (processor instanceof ShreddedProcessor) {
                    this.processor = new DuplicateEliminator((ShreddedProcessor) processor);
                } else if (processor instanceof NumberWordPosition.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberWordPosition.Processor) processor));
                } else if (processor instanceof org.galagosearch.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.galagosearch.tupleflow.Processor<NumberWordPosition>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberWordPosition> getOutputClass() {
                return NumberWordPosition.class;
            }
            
            public void initialize() throws IOException {
                for (ShreddedReader reader : readers) {
                    reader.fill();                                        
                    
                    if (!reader.getBuffer().isAtEnd())
                        queue.add(reader);
                }   

                uninitialized = false;
            }

            public void run() throws IOException {
                initialize();
               
                while (queue.size() > 0) {
                    ShreddedReader top = queue.poll();
                    ShreddedReader next = null;
                    ShreddedBuffer nextBuffer = null; 
                    
                    assert !top.getBuffer().isAtEnd();
                                                  
                    if (queue.size() > 0) {
                        next = queue.peek();
                        nextBuffer = next.getBuffer();
                        assert !nextBuffer.isAtEnd();
                    }
                    
                    top.getBuffer().copyUntil(nextBuffer, processor);
                    if (top.getBuffer().isAtEnd())
                        top.fill();                 
                        
                    if (!top.getBuffer().isAtEnd())
                        queue.add(top);
                }              
                
                if (closeOnExit)
                    processor.close();
            }

            public NumberWordPosition read() throws IOException {
                if (uninitialized)
                    initialize();

                NumberWordPosition result = null;

                while (queue.size() > 0) {
                    ShreddedReader top = queue.poll();
                    result = top.read();

                    if (result != null) {
                        if (top.getBuffer().isAtEnd())
                            top.fill();

                        queue.offer(top);
                        break;
                    } 
                }

                return result;
            }
        } 
        public static class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<NumberWordPosition>, ShreddedSource {      
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            NumberWordPosition last = new NumberWordPosition();         
            long updateWordCount = -1;
            long updateDocumentCount = -1;
            long updatePositionCount = -1;
            long tupleCount = 0;
            long bufferStartCount = 0;  
            ArrayInput input;
            
            public ShreddedReader(ArrayInput input) {
                this.input = input; 
                this.buffer = new ShreddedBuffer();
            }                               
            
            public ShreddedReader(ArrayInput input, int bufferSize) { 
                this.input = input;
                this.buffer = new ShreddedBuffer(bufferSize);
            }
                 
            public final int compareTo(ShreddedReader other) {
                ShreddedBuffer otherBuffer = other.getBuffer();
                
                if (buffer.isAtEnd() && otherBuffer.isAtEnd()) {
                    return 0;                 
                } else if (buffer.isAtEnd()) {
                    return -1;
                } else if (otherBuffer.isAtEnd()) {
                    return 1;
                }
                                   
                int result = 0;
                do {
                    result = + Utility.compare(buffer.getWord(), otherBuffer.getWord());
                    if(result != 0) break;
                    result = + Utility.compare(buffer.getDocument(), otherBuffer.getDocument());
                    if(result != 0) break;
                    result = + Utility.compare(buffer.getPosition(), otherBuffer.getPosition());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final NumberWordPosition read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                NumberWordPosition result = new NumberWordPosition();
                
                result.word = buffer.getWord();
                result.document = buffer.getDocument();
                result.position = buffer.getPosition();
                
                buffer.incrementTuple();
                buffer.autoIncrementWord();
                buffer.autoIncrementDocument();
                buffer.autoIncrementPosition();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateWordCount - tupleCount > 0) {
                            buffer.words.add(last.word);
                            buffer.wordTupleIdx.add((int) (updateWordCount - tupleCount));
                        }                              
                        if(updateDocumentCount - tupleCount > 0) {
                            buffer.documents.add(last.document);
                            buffer.documentTupleIdx.add((int) (updateDocumentCount - tupleCount));
                        }                              
                        if(updatePositionCount - tupleCount > 0) {
                            buffer.positions.add(last.position);
                            buffer.positionTupleIdx.add((int) (updatePositionCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updatePosition();
                        buffer.processTuple();
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateWord() throws IOException {
                if (updateWordCount > tupleCount)
                    return;
                     
                last.word = input.readBytes();
                updateWordCount = tupleCount + input.readInt();
                                      
                buffer.processWord(last.word);
            }
            public final void updateDocument() throws IOException {
                if (updateDocumentCount > tupleCount)
                    return;
                     
                updateWord();
                last.document = input.readInt();
                updateDocumentCount = tupleCount + input.readInt();
                                      
                buffer.processDocument(last.document);
            }
            public final void updatePosition() throws IOException {
                if (updatePositionCount > tupleCount)
                    return;
                     
                updateDocument();
                last.position = input.readInt();
                updatePositionCount = tupleCount + input.readInt();
                                      
                buffer.processPosition(last.position);
            }

            public void run() throws IOException {
                while (true) {
                    fill();
                    
                    if (buffer.isAtEnd())
                        break;
                    
                    buffer.copyUntil(null, processor);
                }      
                processor.close();
            }
            
            public void setProcessor(Step processor) throws IncompatibleProcessorException {  
                if (processor instanceof ShreddedProcessor) {
                    this.processor = new DuplicateEliminator((ShreddedProcessor) processor);
                } else if (processor instanceof NumberWordPosition.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberWordPosition.Processor) processor));
                } else if (processor instanceof org.galagosearch.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.galagosearch.tupleflow.Processor<NumberWordPosition>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberWordPosition> getOutputClass() {
                return NumberWordPosition.class;
            }                
        }
        
        public static class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            NumberWordPosition last = new NumberWordPosition();
            boolean wordProcess = true;
            boolean documentProcess = true;
            boolean positionProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processWord(byte[] word) throws IOException {  
                if (wordProcess || Utility.compare(word, last.word) != 0) {
                    last.word = word;
                    processor.processWord(word);
            resetDocument();
                    wordProcess = false;
                }
            }
            public void processDocument(int document) throws IOException {  
                if (documentProcess || Utility.compare(document, last.document) != 0) {
                    last.document = document;
                    processor.processDocument(document);
            resetPosition();
                    documentProcess = false;
                }
            }
            public void processPosition(int position) throws IOException {  
                if (positionProcess || Utility.compare(position, last.position) != 0) {
                    last.position = position;
                    processor.processPosition(position);
                    positionProcess = false;
                }
            }  
            
            public void resetWord() {
                 wordProcess = true;
            resetDocument();
            }                                                
            public void resetDocument() {
                 documentProcess = true;
            resetPosition();
            }                                                
            public void resetPosition() {
                 positionProcess = true;
            }                                                
                               
            public void processTuple() throws IOException {
                processor.processTuple();
            } 
            
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static class TupleUnshredder implements ShreddedProcessor {
            NumberWordPosition last = new NumberWordPosition();
            public org.galagosearch.tupleflow.Processor<NumberWordPosition> processor;                               
            
            public TupleUnshredder(NumberWordPosition.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.galagosearch.tupleflow.Processor<NumberWordPosition> processor) {
                this.processor = processor;
            }
            
            public NumberWordPosition clone(NumberWordPosition object) {
                NumberWordPosition result = new NumberWordPosition();
                if (object == null) return result;
                result.document = object.document; 
                result.word = object.word; 
                result.position = object.position; 
                return result;
            }                 
            
            public void processWord(byte[] word) throws IOException {
                last.word = word;
            }   
                
            public void processDocument(int document) throws IOException {
                last.document = document;
            }   
                
            public void processPosition(int position) throws IOException {
                last.position = position;
            }   
                
            
            public void processTuple() throws IOException {
                processor.process(clone(last));
            }               
            
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static class TupleShredder implements Processor {
            NumberWordPosition last = new NumberWordPosition();
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public NumberWordPosition clone(NumberWordPosition object) {
                NumberWordPosition result = new NumberWordPosition();
                if (object == null) return result;
                result.document = object.document; 
                result.word = object.word; 
                result.position = object.position; 
                return result;
            }                 
            
            public void process(NumberWordPosition object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || Utility.compare(last.word, object.word) != 0 || processAll) { processor.processWord(object.word); processAll = true; }
                if(last == null || Utility.compare(last.document, object.document) != 0 || processAll) { processor.processDocument(object.document); processAll = true; }
                if(last == null || Utility.compare(last.position, object.position) != 0 || processAll) { processor.processPosition(object.position); processAll = true; }
                processor.processTuple();                                         
            }
                          
            public Class<NumberWordPosition> getInputClass() {
                return NumberWordPosition.class;
            }
            
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    