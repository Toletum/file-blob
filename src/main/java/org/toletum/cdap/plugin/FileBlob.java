package org.toletum.cdap.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;

import java.io.File;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("FileBlob")
@Description("Consume all input records and push to log a Sample")
public class FileBlob extends SparkSink<StructuredRecord> {
  private final FileBlobConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(FileBlob.class);


  public FileBlob(FileBlobConfig config) {
    this.config = config;
  }

  /**
   * @param context of runtime for this plugin.
   */
  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
    classBuildTimeMillis(LOG, FileBlob.class);

    LOG.info("LABEL {}", context.getStageName());


    LOG.info("{} -> {}", "TEST", context.getInputSchema().getField("body"));
  }

  /**
   */
  @Override
  public void run(SparkExecutionPluginContext sparkExecutionPluginContext,
                  JavaRDD<StructuredRecord> javaRDD) throws Exception {
    StructuredRecord record;
    Iterator<Schema.Field> fieldIter;

    List data = javaRDD.collect();

    Path destination = new Path(config.destinationPath);
    Configuration conf = new Configuration();
    FileSystem fileSystem = destination.getFileSystem(conf);

    try (OutputStream output = fileSystem.create(destination)) {
      for(int i = 0; i < data.size(); i++) {
        record = (StructuredRecord)data.get(i);

        output.write(record.get("body"));
      }

    }

  }

  private static void classBuildTimeMillis(Logger LOG, java.lang.Class o) {
    Date compilationDate;

    URL resource = o.getResource(o.getSimpleName() + ".class");
    if (resource == null) {
      compilationDate = new Date(0);
    } else {
      if (resource.getProtocol().equals("file")) {

        try {
          compilationDate = new Date(new File(resource.toURI()).lastModified());
        } catch (URISyntaxException e) {
          compilationDate = new Date(0);
        }

      } else if (resource.getProtocol().equals("jar")) {

        String path = resource.getPath();
        compilationDate = new Date(new File(path.substring(5, path.indexOf("!"))).lastModified());

      } else {
        compilationDate = new Date(0);
      }
    }

    LOG.info("Compilation date {}: {}", o.getSimpleName(), compilationDate);

  }


  /**
   * Config properties for the plugin.
   */
  public static final class FileBlobConfig extends PluginConfig {

    @Name("destinationPath")
    @Description("destinationPath")
    @Macro
    public String destinationPath;

    public FileBlobConfig(String destinationPath) {
      this.destinationPath = destinationPath;
    }

    public String getDestinationPath() {
      return destinationPath;
    }

  }
}
