package com.ociweb.pronghorn.exampleStages;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.ring.proxy.ProngTemplateField;
import com.ociweb.pronghorn.ring.proxy.ProngTemplateMessage;

@ProngTemplateMessage(templateId=100)
public interface MQTTProducer {

	@ProngTemplateField(fieldId=110)	
	CharSequence readSeverURI(Appendable target);

	@ProngTemplateField(fieldId=111)	
	CharSequence readClientId(Appendable target);
	
	@ProngTemplateField(fieldId=112)	
	int readIndex();	
	
	@ProngTemplateField(fieldId=120)
	CharSequence readTopic(Appendable target);
	
	@ProngTemplateField(fieldId=121)
	ByteBuffer readPayload(ByteBuffer target);
	
	@ProngTemplateField(fieldId=122)
	int readQoS();
	
}
