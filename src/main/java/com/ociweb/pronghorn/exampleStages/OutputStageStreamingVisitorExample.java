package com.ociweb.pronghorn.exampleStages;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitorAdapter;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class OutputStageStreamingVisitorExample extends PronghornStage {

	private final class ExampleVisitor extends StreamingReadVisitorAdapter {
		
		private final FauxDatabase databaseConnection;
		private final StringBuilder serverURIBuilder = new StringBuilder();
		private final StringBuilder clientIdBuilder = new StringBuilder();
		private final StringBuilder topicIBuilder = new StringBuilder();
				
		public ExampleVisitor(FauxDatabase databaseConnection ) {
			this.databaseConnection = databaseConnection;
	
		}


		@Override
		public void visitTemplateOpen(String name, long id) {
			databaseConnection.writeMessageId(0);//FieldReferenceOffsetManager.lookupTemplateLocator(id, from));
		}
		
		@Override
		public void visitBytes(String name, long id, ByteBuffer value) {			
			value.flip();
			databaseConnection.writePayload(value);
		}

		@Override
		public void visitSignedInteger(String string, long id, int value) {
			databaseConnection.writeClientIdIdx(value);
		}

		@Override
		public void visitUnsignedInteger(String string, long id, long value) {
			databaseConnection.writeQOS((int)value);
		}

		@Override
		public Appendable targetASCII(String name, long id) {
				switch((int)id) {
				case 110:
					serverURIBuilder.setLength(0);
					return serverURIBuilder;
				case 111:
					clientIdBuilder.setLength(0);
					return clientIdBuilder;
				case 120:
					topicIBuilder.setLength(0);
					return topicIBuilder;
				default:
					return super.targetASCII(name, id);
			}
		}

		
		@Override
		public void visitASCII(String name, long id, Appendable value) {
		   
			switch((int)id) {
				case 110:
					databaseConnection.writeServerURI((CharSequence)value);
					break;
				case 111:
					databaseConnection.writeClientId((CharSequence)value);
					break;					
				case 120:
					databaseConnection.writeTopic((CharSequence)value);
					break;
			}
		}
	}

	
	private StreamingReadVisitor visitor;
	private StreamingVisitorReader reader;
	private FieldReferenceOffsetManager from;
	
	
	protected OutputStageStreamingVisitorExample(GraphManager graphManager, FauxDatabase databaseConnection, Pipe input) {
		super(graphManager, input, NONE);

		from = Pipe.from(input);
		visitor = new ExampleVisitor(databaseConnection);
		
		reader = new StreamingVisitorReader(input, visitor);//new StreamingReadVisitorDebugDelegate(visitor) );
		
	}

	@Override
	public void startup() {	
		
		try{
		    reader.startup();
			
			
		    ///////
			//PUT YOUR LOGIC HERE FOR CONNTECTING TO THE DATABASE OR OTHER TARGET FOR INFORMATION
			//////
			
								
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}
	
	
	@Override
	public void run() {
		reader.run();
	}
	

	@Override
	public void shutdown() {
		
		try{
			reader.shutdown();
			
		    ///////
			//PUT YOUR LOGIC HERE TO CLOSE CONNECTIONS FROM THE DATABASE OR OTHER TARGET OF INFORMATION
			//////
			
		} catch (Throwable t) {
			throw new RuntimeException(t);
		} 
	}


}
