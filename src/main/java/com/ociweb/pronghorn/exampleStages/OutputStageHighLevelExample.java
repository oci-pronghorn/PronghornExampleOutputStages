package com.ociweb.pronghorn.exampleStages;

import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupTemplateLocator;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class OutputStageHighLevelExample extends PronghornStage {

	//TODO: AA, monitoring ring id
	//TODO: AA, shutdown startup order of super check
	//TODO: AA, no startup /shutdown called in stage
	//TODO: AA, shutdown is a signal not a concurrent method call.
	
	private final RingBuffer input;
	private final FieldReferenceOffsetManager FROM; //Acronym so this is in all caps (this holds the schema)
	
	private final int MSG_MQTT;
	private final int FIELD_SERVER_URI;
	private final int FIELD_CLIENT_ID;	
	private final int FIELD_CLIENT_INDEX;
	private final int FIELD_TOPIC;
	private final int FIELD_PAYLOAD;
	private final int FIELD_QOS;
		
	private final FauxDatabase databaseConnection;
		
	private StringBuilder serverURIBuilder;
	private StringBuilder clientIdBuilder;
	private StringBuilder topicIBuilder;
	
	protected OutputStageHighLevelExample(GraphManager graphManager, FauxDatabase databaseConnection, RingBuffer input) {
		super(graphManager, input, NONE);
		////////
		//STORE OTHER FIELDS THAT WILL BE REQUIRED IN STARTUP
		////////
	
		this.input = input;
		FROM = RingBuffer.from(input);
		
		this.databaseConnection = databaseConnection;
		
		//NOTE: instead of String names, the template Ids can also be used to look up the locators.
		
		MSG_MQTT = lookupTemplateLocator("MQTTMsg",FROM);  
		
		FIELD_SERVER_URI = lookupFieldLocator("serverURI", MSG_MQTT, FROM);
		FIELD_CLIENT_ID = lookupFieldLocator("clientid", MSG_MQTT, FROM);		
		FIELD_CLIENT_INDEX = lookupFieldLocator("index", MSG_MQTT, FROM);		
		FIELD_TOPIC = lookupFieldLocator("topic", MSG_MQTT, FROM);
		FIELD_PAYLOAD = lookupFieldLocator("payload", MSG_MQTT, FROM);
		FIELD_QOS = lookupFieldLocator("qos", MSG_MQTT, FROM);
		
	}

	@Override
	public void startup() {
		super.startup();
		
		
		try{
			serverURIBuilder = new StringBuilder();
			clientIdBuilder = new StringBuilder();
			topicIBuilder = new StringBuilder();
			
			
		    ///////
			//PUT YOUR LOGIC HERE FOR CONNTECTING TO THE DATABASE OR OTHER TARGET FOR INFORMATION
			//////
						
					
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}
	
	
	@Override
	public void run() {
				
		while (RingReader.tryReadFragment(input)) {		
			
			
			assert(RingReader.isNewMessage(input)) : "This test should only have one simple message made up of one fragment";
			
			int msgIdx = RingReader.getMsgIdx(input);
			
			serverURIBuilder.setLength(0);
			CharSequence serverURI = (CharSequence)RingReader.readASCII(input, FIELD_SERVER_URI, serverURIBuilder);
			
			clientIdBuilder.setLength(0);
			CharSequence clientId = (CharSequence)RingReader.readASCII(input, FIELD_CLIENT_ID, clientIdBuilder);
			
			topicIBuilder.setLength(0);
			CharSequence topic = (CharSequence)RingReader.readASCII(input, FIELD_TOPIC, topicIBuilder);
			
			//This is more verbose because we have raw bytes in this field.
			byte[] backingArray = RingReader.readBytesBackingArray(input, FIELD_PAYLOAD);
			int len = RingReader.readBytesLength(input,FIELD_PAYLOAD);
			int pos = RingReader.readBytesPosition(input, FIELD_PAYLOAD);
			int mask = RingReader.readBytesMask(input, FIELD_PAYLOAD);
						
			int clientIndex = RingReader.readInt(input, FIELD_CLIENT_INDEX);
			int qos = RingReader.readInt(input, FIELD_QOS);
			
			databaseConnection.writeMessageId(msgIdx);
			databaseConnection.writeServerURI(serverURI);
			databaseConnection.writeClientId(clientId);
			databaseConnection.writeClientIdIdx(clientIndex);
			databaseConnection.writeTopic(topic);
			databaseConnection.writePayload(backingArray, pos, len, mask);
			databaseConnection.writeQOS(qos);
						
			RingReader.releaseReadLock(input);
		} 
		
	}
	

	@Override
	public void shutdown() {
		
		try{
			
		    ///////
			//PUT YOUR LOGIC HERE TO CLOSE CONNECTIONS FROM THE DATABASE OR OTHER TARGET OF INFORMATION
			//////
			
		} catch (Throwable t) {
			throw new RuntimeException(t);
		} finally {		
			//call the super.shutdown() last so that any watchers know when this has really stopped
			super.shutdown();
		}
	}

}
