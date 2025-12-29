package com.impact;

import com.impact.options.*;

public interface DbToSftpPipelineOptions extends JdbcOptions, SftpOptions, ReadOptions,
        WriteOptions, NotificationOptions {

}
