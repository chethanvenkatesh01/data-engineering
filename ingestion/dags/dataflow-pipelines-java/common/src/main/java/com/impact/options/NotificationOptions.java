package com.impact.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface NotificationOptions extends PipelineOptions {
    @Description("Mail Recipients")
    String getMailRecipients();
    void setMailRecipients(String value);
}
