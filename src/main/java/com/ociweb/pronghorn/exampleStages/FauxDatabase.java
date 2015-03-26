package com.ociweb.pronghorn.exampleStages;

import java.nio.ByteBuffer;

public interface FauxDatabase {

	//DO NOT implement one of these in your code this is only for testing.
	
	void writeMessageId(int msgIdx);
	void writeServerURI(byte[] data, int pos, int len, int mask);
	void writeClientId(byte[] data, int pos, int len, int mask);
	void writeTopic(byte[] data, int pos, int len, int mask);
	void writePayload(byte[] data, int pos, int len, int mask);
	void writeQOS(int qos);
	long totalMessagesCount();
	long totalBytesCount();
	
	void writeClientIdIdx(int clientIdIdx);
	void writeTopic(CharSequence topic);
	void writeClientId(CharSequence serverURI);
	void writeServerURI(CharSequence serverURI);
	void writePayload(ByteBuffer value);

}
