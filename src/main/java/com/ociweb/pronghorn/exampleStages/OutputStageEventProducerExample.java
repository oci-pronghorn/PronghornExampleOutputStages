package com.ociweb.pronghorn.exampleStages;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.proxy.EventProducer;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class OutputStageEventProducerExample extends PronghornStage {

	private final EventProducer producer;
	private final int msgIdx;
	private final FauxDatabase databaseConnection;
	
	private StringBuilder serverURIBuilder;
	private StringBuilder clientIdBuilder;
	private StringBuilder topicIBuilder;
	private ByteBuffer tempByteBuffer;

	protected OutputStageEventProducerExample(GraphManager graphManager, FauxDatabase databaseConnection, Pipe input) {
		super(graphManager, input, NONE);
		////////
		//STORE OTHER FIELDS THAT WILL BE REQUIRED IN STARTUP
		////////
	
		this.databaseConnection = databaseConnection;
		this.producer = new EventProducer(input);
		this.msgIdx = FieldReferenceOffsetManager.lookupTemplateLocator(100, Pipe.from(input));
	}

	@Override
	public void startup() {
		super.startup();
				
		try{
			
			serverURIBuilder = new StringBuilder();
			clientIdBuilder = new StringBuilder();
			topicIBuilder = new StringBuilder();
			
			tempByteBuffer = ByteBuffer.allocate(512); //Does not grow so we need to know the maximum size.
			
		    ///////
			//PUT YOUR LOGIC HERE FOR CONNTECTING TO THE DATABASE OR OTHER TARGET FOR INFORMATION
			//////
						
					
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}
	
	
	@Override
	public void run() {
				
		MQTTProducer event = EventProducer.take(producer, MQTTProducer.class);
		if (null != event) {//null when there is no content on the input ring
			readFields(event);			
			EventProducer.dispose(producer, event);
		}
	}
	

	private void readFields(MQTTProducer event) {
		
		databaseConnection.writeMessageId(msgIdx);//Not dynamic because this is always the same
		
		clientIdBuilder.setLength(0);
		databaseConnection.writeClientId(event.readClientId(clientIdBuilder));
		databaseConnection.writeClientIdIdx(event.readIndex());
				
		serverURIBuilder.setLength(0);
		databaseConnection.writeServerURI(event.readSeverURI(serverURIBuilder));
		
		topicIBuilder.setLength(0);
		databaseConnection.writeTopic(event.readTopic(topicIBuilder));
		
		databaseConnection.writeQOS(event.readQoS());
				
		tempByteBuffer.clear();
		ByteBuffer readPayload = event.readPayload(tempByteBuffer);
		readPayload.flip();
		databaseConnection.writePayload(readPayload);
		
	}

	@Override
	public void shutdown() {
		
		try{
			
		    ///////
			//PUT YOUR LOGIC HERE TO CLOSE CONNECTIONS FROM THE DATABASE OR OTHER TARGET OF INFORMATION
			//////
			
		} catch (Throwable t) {
			throw new RuntimeException(t);
		} 
	}

}
