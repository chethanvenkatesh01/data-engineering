package com.impact;

import com.impact.options.*;

public interface BqToSftpPipelineOptions extends BigQueryOptions, SftpOptions, ReadOptions,
        WriteOptions, NotificationOptions {

}
