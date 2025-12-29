package com.impact.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface SftpOptions extends PipelineOptions {

    @Description("SFTP Host")
    //@Validation.Required
    String getSftpHost();
    void setSftpHost(String value);

    @Description("SFTP Username")
    //@Validation.Required
    @Hidden
    String getSftpUsername();
    void setSftpUsername(String value);

    @Description("SFTP Password")
    //@Validation.Required
    @Hidden
    String getSftpPassword();
    void setSftpPassword(String value);

    @Description("SFTP Secret Name")
    //@Validation.Required
    String getSftpSecretName();
    void setSftpSecretName(String value);


}
