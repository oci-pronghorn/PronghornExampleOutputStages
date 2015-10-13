package com.ociweb.pronghorn.exampleStages;

import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.blobMask;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteLen;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteMetaData;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class OutputStageLowLevelExample extends PronghornStage {

	private final Pipe input;
	
	private final int msgIdx;
	private final FieldReferenceOffsetManager FROM; //Acronym so this is in all caps (this holds the schema)
	private final FauxDatabase databaseConnection;
	
	
	protected OutputStageLowLevelExample(GraphManager graphManager,	FauxDatabase databaseConnection, Pipe input) {
		super(graphManager, input, NONE);
		
		this.input = input;
		
		////
		//should pass in connection details and do the connect in the startup method
		//a real database connection is also unlikely to to write per field like this but
		//this makes an easy demo to understand and test.
		///
		this.databaseConnection = databaseConnection;
		this.FROM = Pipe.from(input);
		
		//all the script positions for every message is found in this array
		//the length of this array should match the count of templates
		this.msgIdx = FROM.messageStarts[0]; //for this demo we are just using the first message template
		
		validateSchemaSupported(FROM);
				
	}

	private void validateSchemaSupported(FieldReferenceOffsetManager from) {
		
		///////////
		//confirm that the schema in the output is the same one that we want to support in this stage.
		//if not we should throw now to stop the construction early
		///////////
		
		if (!"MQTTMsg".equals(from.fieldNameScript[msgIdx])) {
			throw new UnsupportedOperationException("Expected to find message template MQTTMsg");
		}
		if (100!=from.fieldIdScript[msgIdx]) {
			throw new UnsupportedOperationException("Expected to find message template MQTTMsg with id 100");
		}
	}
	
	@Override
	public void startup() {
		try{
		
		    ///////
			//PUT YOUR LOGIC HERE FOR CONNTECTING TO THE DATABASE OR OTHER TARGET OF INFORMATION
			//////
			
			
					
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}
	
	@Override
	public void run() {
		
		
		//must be at least 1, if so we have a fragment
		if (Pipe.hasContentToRead((Pipe<S>) input, 1)){
			int msgIdx = Pipe.takeMsgIdx(input);
			
			databaseConnection.writeMessageId(msgIdx);
			
			//Read the ASCII server URI
			{
	        	int meta = takeRingByteMetaData(input);
	        	int len = takeRingByteLen(input);
	        	int pos = bytePosition(meta, input, len);        		
				byte[] data = byteBackingArray(meta, input);
				int mask = blobMask(input);//NOTE: the consumer must do their own ASCII conversion
				databaseConnection.writeServerURI(data,pos,len,mask);
			}
			//Read the UTF8 client id
			{
	        	int meta = takeRingByteMetaData(input);
	        	int len = takeRingByteLen(input);
	        	int pos = bytePosition(meta, input, len);        		
				byte[] data = byteBackingArray(meta, input);
				int mask = blobMask(input);//NOTE: the consumer must do their own UTF8 conversion
				databaseConnection.writeClientId(data,pos,len,mask);
			}
			int clientIdIdx = Pipe.takeValue(input);
			databaseConnection.writeClientIdIdx(clientIdIdx);
			//read the ASCII topic
			{
	        	int meta = takeRingByteMetaData(input);
	        	int len = takeRingByteLen(input);
	        	int pos = bytePosition(meta, input, len);        		
				byte[] data = byteBackingArray(meta, input);
				int mask = blobMask(input);	
				databaseConnection.writeTopic(data,pos,len,mask);
			}
			//read the binary payload
			{
	        	int meta = takeRingByteMetaData(input);
	        	int len = takeRingByteLen(input);
	        	int pos = bytePosition(meta, input, len);        		
				byte[] data = byteBackingArray(meta, input);
				int mask = blobMask(input);	
				databaseConnection.writePayload(data,pos,len,mask);
			}
			int qos = Pipe.takeValue(input);
						
			databaseConnection.writeQOS(qos);
			
			Pipe.releaseReads(input);
			
			//low level API can write multiple message and messages with multiple fragments but it 
			//becomes more difficult. (That is what the high level API is more commonly used for)
			//In this example we are writing 1 message that is made up of 1 fragment
			Pipe.confirmLowLevelRead(input, FROM.fragDataSize[msgIdx]);
			
		}
		
		
	}

	@Override
	public void shutdown() {
			
		try{
			
		    ///////
			//PUT YOUR LOGIC HERE TO CLOSE CONNECTIONS FROM THE DATABASE OR OTHER SOURCE OF INFORMATION
			//////
			
		} catch (Throwable t) {
			throw new RuntimeException(t);
		} 
	}
	
	
}
