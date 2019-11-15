package com.combination.checkMessage;

import java.io.IOException;


import com.consumerclass.distinct.message.OwnConsumerFromKafka_DistictMessage;
import com.google.gson.JsonObject;

public class AppendMessageHere {
	

	public void appendMessage(long i, String c, String V, int w,int l, JsonObject json) throws IOException {
		OwnConsumerFromKafka_DistictMessage ock=new OwnConsumerFromKafka_DistictMessage();
		json.getAsJsonObject("v").addProperty("imo", i);
		json.getAsJsonObject("v").addProperty("width", w);
		json.getAsJsonObject("v").addProperty("vesselName", V);
		json.getAsJsonObject("v").addProperty("callSign", c);
		json.getAsJsonObject("v").addProperty("Length", l);
	//	System.out.println("this is a final json: - "+json);
		OwnConsumerFromKafka_DistictMessage.doAllPutOprationToHabse(json);
		
	}

}
