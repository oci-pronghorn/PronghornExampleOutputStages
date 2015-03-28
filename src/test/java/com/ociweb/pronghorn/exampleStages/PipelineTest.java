package com.ociweb.pronghorn.exampleStages;

import static com.ociweb.pronghorn.ring.RingBuffer.addASCII;
import static com.ociweb.pronghorn.ring.RingBuffer.addByteArray;
import static com.ociweb.pronghorn.ring.RingBuffer.addIntValue;
import static com.ociweb.pronghorn.ring.RingBuffer.addMsgIdx;
import static com.ociweb.pronghorn.ring.RingBuffer.addUTF8;
import static com.ociweb.pronghorn.ring.RingBuffer.confirmLowLevelWrite;
import static com.ociweb.pronghorn.ring.RingBuffer.initLowLevelWriter;
import static com.ociweb.pronghorn.ring.RingBuffer.publishWrites;
import static com.ociweb.pronghorn.ring.RingBuffer.roomToLowLevelWrite;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.monitor.MonitorFROM;
import com.ociweb.pronghorn.stage.route.RoundRobinRouteStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class PipelineTest {

	static final long TIMEOUT_SECONDS = 3;
	static final long TEST_LENGTH_IN_SECONDS = 7;
	
	private static FieldReferenceOffsetManager from;
	public static final int messagesOnRing = 2000;
	public static final int monitorMessagesOnRing = 7;
	
	private static final int maxLengthVarField = 256;
	
	private final Integer monitorRate = Integer.valueOf(50000000);
	
	private static RingBufferConfig ringBufferConfig;
	private static RingBufferConfig ringBufferMonitorConfig;
	
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
			ringBufferConfig = new RingBufferConfig(from, messagesOnRing, maxLengthVarField);
			ringBufferMonitorConfig = new RingBufferConfig(MonitorFROM.buildFROM(), monitorMessagesOnRing, maxLengthVarField);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
	
	@Test
	public void lowLevelOutputStageTest() {
				
		RingBuffer ringBuffer = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer1 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer2 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer3 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer4 = new RingBuffer(ringBufferConfig);
						
   	    GraphManager gm = new GraphManager();
							
		FauxDatabase checker1 = new FauxDatabaseChecker();
		FauxDatabase checker2 = new FauxDatabaseChecker();
		FauxDatabase checker3 = new FauxDatabaseChecker();
		FauxDatabase checker4 = new FauxDatabaseChecker();
				
		GenerateTestDataStage generator = new GenerateTestDataStage(gm, ringBuffer);
		
		RoundRobinRouteStage router = new RoundRobinRouteStage(gm, ringBuffer, ringBuffer1, ringBuffer2, ringBuffer3, ringBuffer4);
		
		OutputStageLowLevelExample out1 = new OutputStageLowLevelExample(gm, checker1, ringBuffer1);
		OutputStageLowLevelExample out2 = new OutputStageLowLevelExample(gm, checker2, ringBuffer2);
		OutputStageLowLevelExample out3 = new OutputStageLowLevelExample(gm, checker3, ringBuffer3);
		OutputStageLowLevelExample out4 = new OutputStageLowLevelExample(gm, checker4, ringBuffer4);

		//Turn on monitoring
		GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, monitorRate, new MonitorConsoleStage(gm, GraphManager.attachMonitorsToGraph(gm, monitorRate, ringBufferMonitorConfig)));	
		
		//Enable batching
		GraphManager.enableBatching(gm);

		timeAndRunTest(ringBuffer, gm, " LowLevel", checker1, checker2, checker3, checker4);   
		
	}
	
	@Test
	public void highLevelOutputStageTest() {
				
		RingBuffer ringBuffer = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer1 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer2 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer3 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer4 = new RingBuffer(ringBufferConfig);
						
   	    GraphManager gm = new GraphManager();
							
		FauxDatabase checker1 = new FauxDatabaseChecker();
		FauxDatabase checker2 = new FauxDatabaseChecker();
		FauxDatabase checker3 = new FauxDatabaseChecker();
		FauxDatabase checker4 = new FauxDatabaseChecker();
				
		GenerateTestDataStage generator = new GenerateTestDataStage(gm, ringBuffer);
		
		RoundRobinRouteStage router = new RoundRobinRouteStage(gm, ringBuffer, ringBuffer1, ringBuffer2, ringBuffer3, ringBuffer4);
		
		OutputStageHighLevelExample out1 = new OutputStageHighLevelExample(gm, checker1, ringBuffer1);
		OutputStageHighLevelExample out2 = new OutputStageHighLevelExample(gm, checker2, ringBuffer2);
		OutputStageHighLevelExample out3 = new OutputStageHighLevelExample(gm, checker3, ringBuffer3);
		OutputStageHighLevelExample out4 = new OutputStageHighLevelExample(gm, checker4, ringBuffer4);

		//Turn on monitoring
		GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, monitorRate, new MonitorConsoleStage(gm, GraphManager.attachMonitorsToGraph(gm, monitorRate, ringBufferMonitorConfig)));	
		
		//Enable batching
		GraphManager.enableBatching(gm);

		timeAndRunTest(ringBuffer, gm, " HighLevel", checker1, checker2, checker3, checker4);   
		
	}
	

	
	@Test
	public void streamingVisitorOutputStageTest() {
				
		RingBuffer ringBuffer = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer1 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer2 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer3 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer4 = new RingBuffer(ringBufferConfig);
						
   	    GraphManager gm = new GraphManager();
							
		FauxDatabase checker1 = new FauxDatabaseChecker();
		FauxDatabase checker2 = new FauxDatabaseChecker();
		FauxDatabase checker3 = new FauxDatabaseChecker();
		FauxDatabase checker4 = new FauxDatabaseChecker();
				
		GenerateTestDataStage generator = new GenerateTestDataStage(gm, ringBuffer);
		
		//OutputStageStreamingConsumerExample out1 = new OutputStageStreamingConsumerExample(gm, checker1, ringBuffer);
		
		RoundRobinRouteStage router = new RoundRobinRouteStage(gm, ringBuffer, ringBuffer1, ringBuffer2, ringBuffer3, ringBuffer4);
				
		OutputStageStreamingVisitorExample out1 = new OutputStageStreamingVisitorExample(gm, checker1, ringBuffer1);
		OutputStageStreamingVisitorExample out2 = new OutputStageStreamingVisitorExample(gm, checker2, ringBuffer2);
		OutputStageStreamingVisitorExample out3 = new OutputStageStreamingVisitorExample(gm, checker3, ringBuffer3);
		OutputStageStreamingVisitorExample out4 = new OutputStageStreamingVisitorExample(gm, checker4, ringBuffer4);

		//Turn on monitoring
		GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, monitorRate, new MonitorConsoleStage(gm, GraphManager.attachMonitorsToGraph(gm, monitorRate, ringBufferMonitorConfig)));	
		
		//Enable batching
		GraphManager.enableBatching(gm);

		timeAndRunTest(ringBuffer, gm, " StreamingConsumer", checker1, checker2, checker3, checker4);   
		
	}
	
	@Test
	public void eventProducerOutputStageTest() {
				
		RingBuffer ringBuffer = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer1 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer2 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer3 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer4 = new RingBuffer(ringBufferConfig);
						
   	    GraphManager gm = new GraphManager();
							
		FauxDatabase checker1 = new FauxDatabaseChecker();
		FauxDatabase checker2 = new FauxDatabaseChecker();
		FauxDatabase checker3 = new FauxDatabaseChecker();
		FauxDatabase checker4 = new FauxDatabaseChecker();
				
		GenerateTestDataStage generator = new GenerateTestDataStage(gm, ringBuffer);
				
		RoundRobinRouteStage router = new RoundRobinRouteStage(gm, ringBuffer, ringBuffer1, ringBuffer2, ringBuffer3, ringBuffer4);
				
		OutputStageEventProducerExample out1 = new OutputStageEventProducerExample(gm, checker1, ringBuffer1);
		OutputStageEventProducerExample out2 = new OutputStageEventProducerExample(gm, checker2, ringBuffer2);
		OutputStageEventProducerExample out3 = new OutputStageEventProducerExample(gm, checker3, ringBuffer3);
		OutputStageEventProducerExample out4 = new OutputStageEventProducerExample(gm, checker4, ringBuffer4);

		//Turn on monitoring
		GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, monitorRate, new MonitorConsoleStage(gm, GraphManager.attachMonitorsToGraph(gm, monitorRate, ringBufferMonitorConfig)));	
		
		//Enable batching
		GraphManager.enableBatching(gm);

		timeAndRunTest(ringBuffer, gm, " EventProducer", checker1, checker2, checker3, checker4);   
		
	}
	
	private long timeAndRunTest(RingBuffer ringBuffer, GraphManager gm, String label, FauxDatabase  ... checker) {
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
        long duration = System.currentTimeMillis()-startTime;
        
        long messages = reportResults(ringBuffer, label, duration, checker);
		
		//assertTrue("RingBuffer: "+ringBuffer, cleanExit);
	
		return messages;
	}


	private long reportResults(RingBuffer ringBuffer, String label,
			long duration, FauxDatabase... checker) {
		long messages=0;
        long bytes=0;
        int i = checker.length;
        while (--i>=0) {
        	messages += checker[i].totalMessagesCount();
        	bytes += checker[i].totalBytesCount();
        }        
				
		if (0!=duration) {
			
			long bytesMoved = (4*RingBuffer.headPosition(ringBuffer))+bytes;
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
				};
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
				};
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
				};
			}
		}


	}

	private final class GenerateTestDataStage extends PronghornStage {
				
		private int fragToWrite;
		

		private int    clientIdIndex = 42;	
		private FieldReferenceOffsetManager FROM;		
		private RingBuffer output;
		
		protected GenerateTestDataStage(GraphManager graphManager, RingBuffer output) {
			super(graphManager, NONE, output);
			
			FROM = RingBuffer.from(output);
			
			this.output = output;
			//all the script positions for every message is found in this array
			//the length of this array should match the count of templates
			this.fragToWrite = FROM.messageStarts[0]; //for this demo we are just using the first message template
			
		}
		

		@Override
		public void startup() {
			super.startup();
			try{
				//setup the output ring for low level writing			
				initLowLevelWriter(output); //TODO: AA, working to remove this.
			
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
						
			if (roomToLowLevelWrite(output, requiredSize) ) {
				int consumedSize = 0;
											
				//when starting a new message this method must be used to record the message template id, and do internal housekeeping
				addMsgIdx(output, fragToWrite);					
							
				addASCII(serverURI, 0, serverURI.length(), output); //serverURI
				addUTF8(clientId, 0, clientId.length(), output); //clientId			
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
