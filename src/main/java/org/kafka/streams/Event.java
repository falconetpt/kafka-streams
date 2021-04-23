package org.kafka.streams;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Event {
  private String provider;
  private String paymentSubmissionId;
  private String eventType;
  private String status;
  private String payload;
}
