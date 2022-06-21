package com.function;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.OffsetDateTime;

public class KafkaEntity {
    public int Offset;
    public int Partition;
    public String Timestamp;
    public String Topic;
    public String Value;
    public KafkaHeaders Headers[];

    public KafkaEntity(int Offset, int Partition, String Topic, String Timestamp, String Value,KafkaHeaders[] headers) {
        this.Offset = Offset;
        this.Partition = Partition;
        this.Topic = Topic;
        this.Timestamp = Timestamp;
        this.Value = Value;
        this.Headers = headers;
    }

    public KafkaEntity(String Value, KafkaHeaders[] headers){
        this.Headers = headers;
        this.Value = Value;
        this.Offset = 0;
        this.Partition = 0;
        this.Topic = "";
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        this.Timestamp = now.toString();        
    }
}
