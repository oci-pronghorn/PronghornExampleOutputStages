package com.ociweb.pronghorn.exampleStages;

import static com.ociweb.pronghorn.pipe.Pipe.addASCII;
import static com.ociweb.pronghorn.pipe.Pipe.addByteArray;
import static com.ociweb.pronghorn.pipe.Pipe.addIntValue;
import static com.ociweb.pronghorn.pipe.Pipe.addMsgIdx;
import static com.ociweb.pronghorn.pipe.Pipe.addUTF8;
import static com.ociweb.pronghorn.pipe.Pipe.confirmLowLevelWrite;
import static com.ociweb.pronghorn.pipe.Pipe.publishWrites;
import static com.ociweb.pronghorn.pipe.Pipe.roomToLowLevelWrite;
import static com.ociweb.pronghorn.stage.scheduling.GraphManager.getOutputPipe;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorCollectorStage;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema;
import com.ociweb.pronghorn.stage.route.RoundRobinRouteStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class PipelineTest {

	static final long TIMEOUT_SECONDS = 12;
	static final long TEST_LENGTH_IN_SECONDS = 7;
	
	private static FieldReferenceOffsetManager from;
	public static final int messagesOnRing = 2000;
	public static final int monitorMessagesOnRing = 7;
	
	private static final int maxLengthVarField = 256;
	
	private final Integer monitorRate = Integer.valueOf(50000000);
	
	private static PipeConfig ringBufferConfig;
	private static PipeConfig ringBufferMonitorConfig;
	
	public static String serverURI = "tcp://localhost:1883";
	public static String clientId = "thingFortytwo";
	public static String topic = "root/colors/blue";
	public static byte[] payload;
	static {
		int payloadSize = 128;
		payload = new byte[payloadSize];
		int i = payloadSize;
		while (--i>=0) {
			payload[i]=(byte)i;
		}
	}

	@BeforeClass
	public static void loadSchema() {
		System.gc();
		///////
		//When doing development and testing be sure that assertions are on by adding -ea to the JVM arguments
		//This will enable a lot of helpful to catch configuration and setup errors earlier
		/////
		
		try {
			from = TemplateHandler.loadFrom("/exampleTemplate.xml");
			ringBufferConfig = new PipeConfig(new MessageSchemaDynamic(from), messagesOnRing, maxLengthVarField);
			ringBufferMonitorConfig = new PipeConfig(PipeMonitorSchema.instance, monitorMessagesOnRing, maxLengthVarField);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
	
	@Test
	public void lowLevelOutputStageTest() {
				
        GraphManager gm = new GraphManager();
        
        PronghornStage router = buildPipeline(gm);
        
        FauxDatabase checker1 = new FauxDatabaseChecker();
        FauxDatabase checker2 = new FauxDatabaseChecker();
        FauxDatabase checker3 = new FauxDatabaseChecker();
        FauxDatabase checker4 = new FauxDatabaseChecker();
                
        PronghornStage out1 = new OutputStageLowLevelExample(gm, checker1, GraphManager.getOutputPipe(gm, router, 1));
        PronghornStage out2 = new OutputStageLowLevelExample(gm, checker2, GraphManager.getOutputPipe(gm, router, 2));
        PronghornStage out3 = new OutputStageLowLevelExample(gm, checker3, GraphManager.getOutputPipe(gm, router, 3));
        PronghornStage out4 = new OutputStageLowLevelExample(gm, checker4, GraphManager.getOutputPipe(gm, router, 4));


		//Turn on monitoring
        PipeMonitorCollectorStage.attach(gm);	
		
		//Enable batching
	//	GraphManager.enableBatching(gm);

		timeAndRunTest(getOutputPipe(gm, GraphManager.findStageByPath(gm, 1)), gm, " LowLevel", checker1, checker2, checker3, checker4);   
		
	}
	
	@Test
	public void highLevelOutputStageTest() {
				
	        GraphManager gm = new GraphManager();
	        
	        PronghornStage router = buildPipeline(gm);
	        
	        FauxDatabase checker1 = new FauxDatabaseChecker();
	        FauxDatabase checker2 = new FauxDatabaseChecker();
	        FauxDatabase checker3 = new FauxDatabaseChecker();
	        FauxDatabase checker4 = new FauxDatabaseChecker();
	                
	        PronghornStage out1 = new OutputStageHighLevelExample(gm, checker1, GraphManager.getOutputPipe(gm, router, 1));
	        PronghornStage out2 = new OutputStageHighLevelExample(gm, checker2, GraphManager.getOutputPipe(gm, router, 2));
	        PronghornStage out3 = new OutputStageHighLevelExample(gm, checker3, GraphManager.getOutputPipe(gm, router, 3));
	        PronghornStage out4 = new OutputStageHighLevelExample(gm, checker4, GraphManager.getOutputPipe(gm, router, 4));
	        
	
		//Turn on monitoring
	        PipeMonitorCollectorStage.attach(gm);
		
		//Enable batching
		GraphManager.enableBatching(gm);

		timeAndRunTest(getOutputPipe(gm, GraphManager.findStageByPath(gm, 1)), gm, " HighLevel", checker1, checker2, checker3, checker4);   
		
	}
	

	
	@Test
	public void streamingVisitorOutputStageTest() {
	    GraphManager gm = new GraphManager();
		
	    PronghornStage router = buildPipeline(gm);
		
		FauxDatabase checker1 = new FauxDatabaseChecker();
		FauxDatabase checker2 = new FauxDatabaseChecker();
		FauxDatabase checker3 = new FauxDatabaseChecker();
		FauxDatabase checker4 = new FauxDatabaseChecker();
				
		PronghornStage out1 = new OutputStageStreamingVisitorExample(gm, checker1, GraphManager.getOutputPipe(gm, router, 1));
		PronghornStage out2 = new OutputStageStreamingVisitorExample(gm, checker2, GraphManager.getOutputPipe(gm, router, 2));
		PronghornStage out3 = new OutputStageStreamingVisitorExample(gm, checker3, GraphManager.getOutputPipe(gm, router, 3));
		PronghornStage out4 = new OutputStageStreamingVisitorExample(gm, checker4, GraphManager.getOutputPipe(gm, router, 4));

		//Turn on monitoring
//		MonitorConsoleStage.attach(gm, monitorRate, ringBufferMonitorConfig);
		
		//Enable batching
		GraphManager.enableBatching(gm);

		timeAndRunTest(getOutputPipe(gm, GraphManager.findStageByPath(gm, 1)), gm, " StreamingVisitor", checker1, checker2, checker3, checker4);   
		
	}


    private PronghornStage buildPipeline(GraphManager gm) {
        PronghornStage router;
	    {
    		GenerateTestDataStage generator = new GenerateTestDataStage(gm, new Pipe(ringBufferConfig));		
    		router = new RoundRobinRouteStage(gm, GraphManager.getOutputPipe(gm, generator, 1), new Pipe(ringBufferConfig), new Pipe(ringBufferConfig), new Pipe(ringBufferConfig), new Pipe(ringBufferConfig));
	    }
        return router;
    }
	
	@Test
	public void eventProducerOutputStageTest() {
									
	    
	       GraphManager gm = new GraphManager();
	        
	        PronghornStage router = buildPipeline(gm);
	        
	        FauxDatabase checker1 = new FauxDatabaseChecker();
	        FauxDatabase checker2 = new FauxDatabaseChecker();
	        FauxDatabase checker3 = new FauxDatabaseChecker();
	        FauxDatabase checker4 = new FauxDatabaseChecker();
	                
	        PronghornStage out1 = new OutputStageEventProducerExample(gm, checker1, GraphManager.getOutputPipe(gm, router, 1));
	        PronghornStage out2 = new OutputStageEventProducerExample(gm, checker2, GraphManager.getOutputPipe(gm, router, 2));
	        PronghornStage out3 = new OutputStageEventProducerExample(gm, checker3, GraphManager.getOutputPipe(gm, router, 3));
	        PronghornStage out4 = new OutputStageEventProducerExample(gm, checker4, GraphManager.getOutputPipe(gm, router, 4));
	    
	    

		//Turn on monitoring
	        PipeMonitorCollectorStage.attach(gm);
		
		//Enable batching
		GraphManager.enableBatching(gm); //TODO: NOTE this is batching our monitors and we dont want that!!

		timeAndRunTest(getOutputPipe(gm, GraphManager.findStageByPath(gm, 1)), gm, " EventProducer", checker1, checker2, checker3, checker4);   
		
	}
	
	private long timeAndRunTest(Pipe ringBuffer, GraphManager gm, String label, FauxDatabase  ... checker) {
		StageScheduler scheduler = new ThreadPerStageScheduler(GraphManager.cloneAll(gm));
		 
	    long startTime = System.currentTimeMillis();
		scheduler.startup();

		try {
			Thread.sleep(TEST_LENGTH_IN_SECONDS*1000);
		} catch (InterruptedException e) {
		}
		
		//NOTE: if the tested input stage is the sort of stage that calls shutdown on its own then
		//      you do not need the above sleep
		//      you do not need the below shutdown
		//
		scheduler.shutdown();
		
        boolean cleanExit = scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);     
        assertTrue(cleanExit);
        long duration = System.currentTimeMillis()-startTime;
        
        long messages = reportResults(ringBuffer, label, duration, checker);
		
	
		return messages;
	}


	private long reportResults(Pipe ringBuffer, String label,
			long duration, FauxDatabase... checker) {
		long messages=0;
        long bytes=0;
        int i = checker.length;
        while (--i>=0) {
        	messages += checker[i].totalMessagesCount();
        	bytes += checker[i].totalBytesCount();
        }        
				
		if (0!=duration) {
			
			long bytesMoved = (4*Pipe.headPosition(ringBuffer))+bytes;
			float mbMoved = (8f*bytesMoved)/(float)(1<<20);
			
			System.out.println("TotalMessages:"+messages + 
					           " Msg/Ms:"+(messages/(float)duration) +
					           " Mb/Ms:"+((mbMoved/(float)duration)) +
					           label
							  );
		}
		return messages;
	}
	

	
	private final class FauxDatabaseChecker implements FauxDatabase {
		long totalMessages;
		long totalBytes;
		byte[] serverURIBytes = serverURI.getBytes();
		byte[] clientIdBytes = clientId.getBytes();
		byte[] topicBytes = topic.getBytes();
		byte[] payloadBytes = payload;		

		@Override
		public void writeMessageId(int msgIdx) {
			assertEquals(0, msgIdx);
			totalMessages++;
			//System.err.println("total messages "+totalMessages);
		}

		@Override
		public void writeServerURI(byte[] data, int pos, int len, int mask) {
			
			assertEquals(serverURIBytes.length,len);
			int j = len;
			while (--j>=0) {
				assertEquals(data[mask&pos+j], serverURIBytes[j]);				
			}			
			
			totalBytes+=len;
		}

		@Override
		public void writeClientId(byte[] data, int pos, int len, int mask) {
			
			assertEquals(clientIdBytes.length,len);
			int j = len;
			while (--j>=0) {
				assertEquals(data[mask&pos+j], clientIdBytes[j]);	
			}
			
			totalBytes+=len;
		}

		@Override
		public void writeTopic(byte[] data, int pos, int len, int mask) {
			
			assertEquals(topicBytes.length,len);
			int j = len;
			while (--j>=0) {
				assertEquals(data[mask&pos+j], topicBytes[j]);
			}
			
			totalBytes+=len;
		}

		@Override
		public void writePayload(byte[] data, int pos, int len, int mask) {
			
			assertEquals(payloadBytes.length,len);
			int j = len;
			while (--j>=0) {
				assertEquals(data[mask&pos+j], payloadBytes[j]);
			}
			
			totalBytes+=len;
		}
		
		@Override
		public void writePayload(ByteBuffer value) {
			int len = value.remaining();
			assertEquals(payloadBytes.length,len);
			int j = len;
			int c = 0;
			while (--j>=0) {
				assertEquals(value.get(), payloadBytes[c++]);
			}
			
			totalBytes+=len;
		}


        @Override
        public void writePayload(DataInputBlobReader reader) {

                int len = reader.available();
                assertEquals(payloadBytes.length,len);
                int j = len;
                int c = 0;
                while (--j>=0) {
                    assertEquals(reader.readByte(), payloadBytes[c++]);
                }
                
                totalBytes+=len;

        }
        
		@Override
		public void writeQOS(int qos) {
			assertEquals(0, qos);
		}
		
		@Override
		public void writeClientIdIdx(int clientIdIdx) {
			assertEquals(42,clientIdIdx);
		}

		@Override
		public long totalMessagesCount() {
			return totalMessages;
		}

		@Override
		public long totalBytesCount() {
			return totalBytes;
		}

		@Override
		public void writeTopic(CharSequence value) {
			int j = value.length();
			totalBytes+=j;
			assertEquals(topic.length(),value.length());			
			while (--j>=0) {
				if (topic.charAt(j)!=value.charAt(j)) {
					assertEquals(topic, value.toString());
				}
			}
		}

		@Override
		public void writeClientId(CharSequence value) {
			int j = value.length();
			totalBytes+=j;
			assertEquals(clientId.length(),value.length());			
			while (--j>=0) {
				if (clientId.charAt(j)!=value.charAt(j)) {
					assertEquals(clientId, value.toString());
				}
			}
		}

		@Override
		public void writeServerURI(CharSequence value) {
			int j = value.length();
			totalBytes+=j;
			assertEquals(serverURI.length(),value.length());
			while (--j>=0) {
				if (serverURI.charAt(j)!=value.charAt(j)) {
					assertEquals(serverURI, value.toString());
				}
			}
		}



	}

	private final class GenerateTestDataStage extends PronghornStage {
				
		private int fragToWrite;
		

		private int    clientIdIndex = 42;	
		private FieldReferenceOffsetManager FROM;		
		private Pipe output;
		
		protected GenerateTestDataStage(GraphManager graphManager, Pipe output) {
			super(graphManager, NONE, output);
			
			FROM = Pipe.from(output);
			
			this.output = output;
			//all the script positions for every message is found in this array
			//the length of this array should match the count of templates
			this.fragToWrite = FROM.messageStarts[0]; //for this demo we are just using the first message template
			
		}
		

		@Override
		public void startup() {
			super.startup();
			try{
			
			    ///////
				//PUT YOUR LOGIC HERE FOR CONNTECTING TO THE DATABASE OR OTHER SOURCE OF INFORMATION
				//////
				
				
						
			} catch (Throwable t) {
				throw new RuntimeException(t);
			}
		}
		
		@Override
		public void run() {
			int requiredSize = FROM.fragDataSize[fragToWrite]; //this can be set to the largest of the union of possible messages.
				
			int count = 100;
			while (--count>=0 && Pipe.hasRoomForWrite((Pipe<?>) output, requiredSize) ) {
			    
				int consumedSize = 0;
											
				//when starting a new message this method must be used to record the message template id, and do internal housekeeping
				addMsgIdx(output, fragToWrite);					
							
				addASCII(serverURI, 0, serverURI.length(), output); //serverURI
				addUTF8(clientId, clientId.length(), output); //clientId			
				addIntValue(clientIdIndex, output);  //clientId index								
				addASCII(topic, 0, topic.length(), output); //topic		
				addByteArray(payload, 0, payload.length, output);// payload 
				
				addIntValue(0, output); //QoS field
		
				
				//publish this fragment
				publishWrites(output);
				
				//total up all the consumed bytes by all the fragments
				consumedSize += FROM.fragDataSize[fragToWrite];
				
				//only increment upon success
				confirmLowLevelWrite(output, consumedSize);			
			}
		}
		
		
		
	}
	
}
