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

public class NumberedDocumentData implements Type<NumberedDocumentData> {
    public String identifier;
    public String url;
    public int number;
    public int textLength; 
    
    public NumberedDocumentData() {}
    public NumberedDocumentData(String identifier, String url, int number, int textLength) {
        this.identifier = identifier;
        this.url = url;
        this.number = number;
        this.textLength = textLength;
    }  
    
    public String toString() {
            return String.format("%s,%s,%d,%d",
                                   identifier, url, number, textLength);
    } 

    public Order<NumberedDocumentData> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] {  })) {
            return new Unordered();
        }
        if (Arrays.equals(spec, new String[] { "+number" })) {
            return new NumberOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.galagosearch.tupleflow.Processor<NumberedDocumentData> {
        public void process(NumberedDocumentData object) throws IOException;
        public void close() throws IOException;
    }                        
    public interface Source extends Step {
    }
    public static class Unordered implements Order<NumberedDocumentData> {
        public int hash(NumberedDocumentData object) {
            int h = 0;
            return h;
        } 
        public Comparator<NumberedDocumentData> greaterThan() {
            return new Comparator<NumberedDocumentData>() {
                public int compare(NumberedDocumentData one, NumberedDocumentData two) {
                    int result = 0;
                    do {
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<NumberedDocumentData> lessThan() {
            return new Comparator<NumberedDocumentData>() {
                public int compare(NumberedDocumentData one, NumberedDocumentData two) {
                    int result = 0;
                    do {
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<NumberedDocumentData> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<NumberedDocumentData> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<NumberedDocumentData> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static class OrderedWriterClass extends OrderedWriter< NumberedDocumentData > {
            NumberedDocumentData last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(NumberedDocumentData object) throws IOException {
               boolean processAll = false;
               shreddedWriter.processTuple(object.identifier, object.url, object.number, object.textLength);
               last = object;
            }           
                 
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<NumberedDocumentData> getInputClass() {
                return NumberedDocumentData.class;
            }
        } 
        public ReaderSource<NumberedDocumentData> orderedCombiner(Collection<TypeReader<NumberedDocumentData>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList();
            
            for (TypeReader<NumberedDocumentData> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public NumberedDocumentData clone(NumberedDocumentData object) {
            NumberedDocumentData result = new NumberedDocumentData();
            if (object == null) return result;
            result.identifier = object.identifier; 
            result.url = object.url; 
            result.number = object.number; 
            result.textLength = object.textLength; 
            return result;
        }                 
        public Class<NumberedDocumentData> getOrderedClass() {
            return NumberedDocumentData.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {};
        }

        public static String getSpecString() {
            return "";
        }
                           
        public interface ShreddedProcessor extends Step {
            public void processTuple(String identifier, String url, int number, int textLength) throws IOException;
            public void close() throws IOException;
        }    
        public interface ShreddedSource extends Step {
        }                                              
        
        public static class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        
            
            public void close() throws IOException {
                flush();
            }
            
            public final void processTuple(String identifier, String url, int number, int textLength) throws IOException {
                if (lastFlush) {
                    lastFlush = false;
                }
                buffer.processTuple(identifier, url, number, textLength);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeString(buffer.getIdentifier());
                    output.writeString(buffer.getUrl());
                    output.writeInt(buffer.getNumber());
                    output.writeInt(buffer.getTextLength());
                    buffer.incrementTuple();
                }
            }  
            public void flush() throws IOException { 
                flushTuples(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static class ShreddedBuffer {
                            
            String[] identifiers;
            String[] urls;
            int[] numbers;
            int[] textLengths;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                identifiers = new String[batchSize];
                urls = new String[batchSize];
                numbers = new int[batchSize];
                textLengths = new int[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processTuple(String identifier, String url, int number, int textLength) {
                identifiers[writeTupleIndex] = identifier;
                urls[writeTupleIndex] = url;
                numbers[writeTupleIndex] = number;
                textLengths[writeTupleIndex] = textLength;
                writeTupleIndex++;
            }
            public void resetData() {
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
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
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public String getIdentifier() {
                assert readTupleIndex < writeTupleIndex;
                return identifiers[readTupleIndex];
            }                                         
            public String getUrl() {
                assert readTupleIndex < writeTupleIndex;
                return urls[readTupleIndex];
            }                                         
            public int getNumber() {
                assert readTupleIndex < writeTupleIndex;
                return numbers[readTupleIndex];
            }                                         
            public int getTextLength() {
                assert readTupleIndex < writeTupleIndex;
                return textLengths[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getIdentifier(), getUrl(), getNumber(), getTextLength());
                   incrementTuple();
                }
            }                                                                           
             
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
            }
            
        }                         
        public static class ShreddedCombiner implements ReaderSource<NumberedDocumentData>, ShreddedSource {   
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
                } else if (processor instanceof NumberedDocumentData.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberedDocumentData.Processor) processor));
                } else if (processor instanceof org.galagosearch.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.galagosearch.tupleflow.Processor<NumberedDocumentData>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberedDocumentData> getOutputClass() {
                return NumberedDocumentData.class;
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

            public NumberedDocumentData read() throws IOException {
                if (uninitialized)
                    initialize();

                NumberedDocumentData result = null;

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
        public static class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<NumberedDocumentData>, ShreddedSource {      
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            NumberedDocumentData last = new NumberedDocumentData();         
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
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final NumberedDocumentData read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                NumberedDocumentData result = new NumberedDocumentData();
                
                result.identifier = buffer.getIdentifier();
                result.url = buffer.getUrl();
                result.number = buffer.getNumber();
                result.textLength = buffer.getTextLength();
                
                buffer.incrementTuple();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        buffer.processTuple(input.readString(), input.readString(), input.readInt(), input.readInt());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
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
                } else if (processor instanceof NumberedDocumentData.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberedDocumentData.Processor) processor));
                } else if (processor instanceof org.galagosearch.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.galagosearch.tupleflow.Processor<NumberedDocumentData>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberedDocumentData> getOutputClass() {
                return NumberedDocumentData.class;
            }                
        }
        
        public static class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            NumberedDocumentData last = new NumberedDocumentData();
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

          
            
                               
            public void processTuple(String identifier, String url, int number, int textLength) throws IOException {
                processor.processTuple(identifier, url, number, textLength);
            } 
            
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static class TupleUnshredder implements ShreddedProcessor {
            NumberedDocumentData last = new NumberedDocumentData();
            public org.galagosearch.tupleflow.Processor<NumberedDocumentData> processor;                               
            
            public TupleUnshredder(NumberedDocumentData.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.galagosearch.tupleflow.Processor<NumberedDocumentData> processor) {
                this.processor = processor;
            }
            
            public NumberedDocumentData clone(NumberedDocumentData object) {
                NumberedDocumentData result = new NumberedDocumentData();
                if (object == null) return result;
                result.identifier = object.identifier; 
                result.url = object.url; 
                result.number = object.number; 
                result.textLength = object.textLength; 
                return result;
            }                 
            
            
            public void processTuple(String identifier, String url, int number, int textLength) throws IOException {
                last.identifier = identifier;
                last.url = url;
                last.number = number;
                last.textLength = textLength;
                processor.process(clone(last));
            }               
            
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static class TupleShredder implements Processor {
            NumberedDocumentData last = new NumberedDocumentData();
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public NumberedDocumentData clone(NumberedDocumentData object) {
                NumberedDocumentData result = new NumberedDocumentData();
                if (object == null) return result;
                result.identifier = object.identifier; 
                result.url = object.url; 
                result.number = object.number; 
                result.textLength = object.textLength; 
                return result;
            }                 
            
            public void process(NumberedDocumentData object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                processor.processTuple(object.identifier, object.url, object.number, object.textLength);                                         
            }
                          
            public Class<NumberedDocumentData> getInputClass() {
                return NumberedDocumentData.class;
            }
            
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static class NumberOrder implements Order<NumberedDocumentData> {
        public int hash(NumberedDocumentData object) {
            int h = 0;
            h += Utility.hash(object.number);
            return h;
        } 
        public Comparator<NumberedDocumentData> greaterThan() {
            return new Comparator<NumberedDocumentData>() {
                public int compare(NumberedDocumentData one, NumberedDocumentData two) {
                    int result = 0;
                    do {
                        result = + Utility.compare(one.number, two.number);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<NumberedDocumentData> lessThan() {
            return new Comparator<NumberedDocumentData>() {
                public int compare(NumberedDocumentData one, NumberedDocumentData two) {
                    int result = 0;
                    do {
                        result = + Utility.compare(one.number, two.number);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<NumberedDocumentData> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<NumberedDocumentData> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<NumberedDocumentData> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static class OrderedWriterClass extends OrderedWriter< NumberedDocumentData > {
            NumberedDocumentData last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(NumberedDocumentData object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != Utility.compare(object.number, last.number)) { processAll = true; shreddedWriter.processNumber(object.number); }
               shreddedWriter.processTuple(object.identifier, object.url, object.textLength);
               last = object;
            }           
                 
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<NumberedDocumentData> getInputClass() {
                return NumberedDocumentData.class;
            }
        } 
        public ReaderSource<NumberedDocumentData> orderedCombiner(Collection<TypeReader<NumberedDocumentData>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList();
            
            for (TypeReader<NumberedDocumentData> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public NumberedDocumentData clone(NumberedDocumentData object) {
            NumberedDocumentData result = new NumberedDocumentData();
            if (object == null) return result;
            result.identifier = object.identifier; 
            result.url = object.url; 
            result.number = object.number; 
            result.textLength = object.textLength; 
            return result;
        }                 
        public Class<NumberedDocumentData> getOrderedClass() {
            return NumberedDocumentData.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+number"};
        }

        public static String getSpecString() {
            return "+number";
        }
                           
        public interface ShreddedProcessor extends Step {
            public void processNumber(int number) throws IOException;
            public void processTuple(String identifier, String url, int textLength) throws IOException;
            public void close() throws IOException;
        }    
        public interface ShreddedSource extends Step {
        }                                              
        
        public static class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            int lastNumber;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        
            
            public void close() throws IOException {
                flush();
            }
            
            public void processNumber(int number) {
                lastNumber = number;
                buffer.processNumber(number);
            }
            public final void processTuple(String identifier, String url, int textLength) throws IOException {
                if (lastFlush) {
                    if(buffer.numbers.size() == 0) buffer.processNumber(lastNumber);
                    lastFlush = false;
                }
                buffer.processTuple(identifier, url, textLength);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeString(buffer.getIdentifier());
                    output.writeString(buffer.getUrl());
                    output.writeInt(buffer.getTextLength());
                    buffer.incrementTuple();
                }
            }  
            public final void flushNumber(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getNumberEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeInt(buffer.getNumber());
                    output.writeInt(count);
                    buffer.incrementNumber();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushNumber(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static class ShreddedBuffer {
            ArrayList<Integer> numbers = new ArrayList();
            ArrayList<Integer> numberTupleIdx = new ArrayList();
            int numberReadIdx = 0;
                            
            String[] identifiers;
            String[] urls;
            int[] textLengths;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                identifiers = new String[batchSize];
                urls = new String[batchSize];
                textLengths = new int[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processNumber(int number) {
                numbers.add(number);
                numberTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String identifier, String url, int textLength) {
                assert numbers.size() > 0;
                identifiers[writeTupleIndex] = identifier;
                urls[writeTupleIndex] = url;
                textLengths[writeTupleIndex] = textLength;
                writeTupleIndex++;
            }
            public void resetData() {
                numbers.clear();
                numberTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                numberReadIdx = 0;
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
            public void incrementNumber() {
                numberReadIdx++;  
            }                                                                                              

            public void autoIncrementNumber() {
                while (readTupleIndex >= getNumberEndIndex() && readTupleIndex < writeTupleIndex)
                    numberReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getNumberEndIndex() {
                if ((numberReadIdx+1) >= numberTupleIdx.size())
                    return writeTupleIndex;
                return numberTupleIdx.get(numberReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public int getNumber() {
                assert readTupleIndex < writeTupleIndex;
                assert numberReadIdx < numbers.size();
                
                return numbers.get(numberReadIdx);
            }
            public String getIdentifier() {
                assert readTupleIndex < writeTupleIndex;
                return identifiers[readTupleIndex];
            }                                         
            public String getUrl() {
                assert readTupleIndex < writeTupleIndex;
                return urls[readTupleIndex];
            }                                         
            public int getTextLength() {
                assert readTupleIndex < writeTupleIndex;
                return textLengths[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getIdentifier(), getUrl(), getTextLength());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexNumber(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processNumber(getNumber());
                    assert getNumberEndIndex() <= endIndex;
                    copyTuples(getNumberEndIndex(), output);
                    incrementNumber();
                }
            }  
            public void copyUntilNumber(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + Utility.compare(getNumber(), other.getNumber());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processNumber(getNumber());
                                      
                        copyTuples(getNumberEndIndex(), output);
                    } else {
                        output.processNumber(getNumber());
                        copyTuples(getNumberEndIndex(), output);
                    }
                    incrementNumber();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilNumber(other, output);
            }
            
        }                         
        public static class ShreddedCombiner implements ReaderSource<NumberedDocumentData>, ShreddedSource {   
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
                } else if (processor instanceof NumberedDocumentData.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberedDocumentData.Processor) processor));
                } else if (processor instanceof org.galagosearch.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.galagosearch.tupleflow.Processor<NumberedDocumentData>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberedDocumentData> getOutputClass() {
                return NumberedDocumentData.class;
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

            public NumberedDocumentData read() throws IOException {
                if (uninitialized)
                    initialize();

                NumberedDocumentData result = null;

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
        public static class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<NumberedDocumentData>, ShreddedSource {      
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            NumberedDocumentData last = new NumberedDocumentData();         
            long updateNumberCount = -1;
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
                    result = + Utility.compare(buffer.getNumber(), otherBuffer.getNumber());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final NumberedDocumentData read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                NumberedDocumentData result = new NumberedDocumentData();
                
                result.number = buffer.getNumber();
                result.identifier = buffer.getIdentifier();
                result.url = buffer.getUrl();
                result.textLength = buffer.getTextLength();
                
                buffer.incrementTuple();
                buffer.autoIncrementNumber();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateNumberCount - tupleCount > 0) {
                            buffer.numbers.add(last.number);
                            buffer.numberTupleIdx.add((int) (updateNumberCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateNumber();
                        buffer.processTuple(input.readString(), input.readString(), input.readInt());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateNumber() throws IOException {
                if (updateNumberCount > tupleCount)
                    return;
                     
                last.number = input.readInt();
                updateNumberCount = tupleCount + input.readInt();
                                      
                buffer.processNumber(last.number);
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
                } else if (processor instanceof NumberedDocumentData.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberedDocumentData.Processor) processor));
                } else if (processor instanceof org.galagosearch.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.galagosearch.tupleflow.Processor<NumberedDocumentData>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberedDocumentData> getOutputClass() {
                return NumberedDocumentData.class;
            }                
        }
        
        public static class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            NumberedDocumentData last = new NumberedDocumentData();
            boolean numberProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processNumber(int number) throws IOException {  
                if (numberProcess || Utility.compare(number, last.number) != 0) {
                    last.number = number;
                    processor.processNumber(number);
                    numberProcess = false;
                }
            }  
            
            public void resetNumber() {
                 numberProcess = true;
            }                                                
                               
            public void processTuple(String identifier, String url, int textLength) throws IOException {
                processor.processTuple(identifier, url, textLength);
            } 
            
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static class TupleUnshredder implements ShreddedProcessor {
            NumberedDocumentData last = new NumberedDocumentData();
            public org.galagosearch.tupleflow.Processor<NumberedDocumentData> processor;                               
            
            public TupleUnshredder(NumberedDocumentData.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.galagosearch.tupleflow.Processor<NumberedDocumentData> processor) {
                this.processor = processor;
            }
            
            public NumberedDocumentData clone(NumberedDocumentData object) {
                NumberedDocumentData result = new NumberedDocumentData();
                if (object == null) return result;
                result.identifier = object.identifier; 
                result.url = object.url; 
                result.number = object.number; 
                result.textLength = object.textLength; 
                return result;
            }                 
            
            public void processNumber(int number) throws IOException {
                last.number = number;
            }   
                
            
            public void processTuple(String identifier, String url, int textLength) throws IOException {
                last.identifier = identifier;
                last.url = url;
                last.textLength = textLength;
                processor.process(clone(last));
            }               
            
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static class TupleShredder implements Processor {
            NumberedDocumentData last = new NumberedDocumentData();
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public NumberedDocumentData clone(NumberedDocumentData object) {
                NumberedDocumentData result = new NumberedDocumentData();
                if (object == null) return result;
                result.identifier = object.identifier; 
                result.url = object.url; 
                result.number = object.number; 
                result.textLength = object.textLength; 
                return result;
            }                 
            
            public void process(NumberedDocumentData object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || Utility.compare(last.number, object.number) != 0 || processAll) { processor.processNumber(object.number); processAll = true; }
                processor.processTuple(object.identifier, object.url, object.textLength);                                         
            }
                          
            public Class<NumberedDocumentData> getInputClass() {
                return NumberedDocumentData.class;
            }
            
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    