package com.haizhi.volans.sink.writer.orc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import java.io.IOException;

public interface HiveOrcBuilder {
    Writer createWriter(FileSystem stream) throws IOException;
}
