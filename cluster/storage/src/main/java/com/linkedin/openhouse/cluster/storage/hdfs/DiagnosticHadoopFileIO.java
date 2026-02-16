package com.linkedin.openhouse.cluster.storage.hdfs;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * Diagnostic FileIO that logs metrics for HDFS client tuning decisions.
 *
 * <p>Focuses on 4 actionable signals:
 *
 * <ol>
 *   <li><b>NN vs DN latency</b> - If NN slow: tune failover. If DN slow: tune hedged reads.
 *   <li><b>Hedged/read failover internals</b> - Enable Hadoop loggers for DFSClient and retry
 *       handlers.
 *   <li><b>Write close time</b> - If slow: enable pipeline recovery.
 *   <li><b>Periodic operation summaries</b> - Track read/write and slow-path counts over time.
 * </ol>
 *
 * <p>Enable: {@code openhouse.hdfs.diagnostic.logging.enabled=true}
 *
 * <p>Log format:
 *
 * <pre>
 * HDFS_READ path=... total_ms=150 nn_ms=20 dn_ms=130 bytes=4096
 * HDFS_WRITE path=... total_ms=250 create_ms=30 write_ms=150 close_ms=70 bytes=4096
 * HDFS_STATS reads=10 writes=8 slow_nn=2 slow_dn=1 slow_close=0
 * </pre>
 */
@Slf4j
public class DiagnosticHadoopFileIO extends HadoopFileIO {

  private boolean enabled = false;

  // Periodic stats tracking
  private long lastStatsTime = 0;
  private static final long STATS_INTERVAL_MS = 60_000;

  // Aggregate counters for summary
  private final AtomicLong totalReads = new AtomicLong();
  private final AtomicLong totalWrites = new AtomicLong();
  private final AtomicLong slowNnReads = new AtomicLong(); // nn_ms > dn_ms
  private final AtomicLong slowDnReads = new AtomicLong(); // dn_ms > nn_ms
  private final AtomicLong slowCloses = new AtomicLong(); // close_ms > 500

  public DiagnosticHadoopFileIO() {
    super();
  }

  public DiagnosticHadoopFileIO(Configuration conf) {
    super(conf);
    init(conf);
  }

  @Override
  public void initialize(Map<String, String> properties) {
    super.initialize(properties);
    this.enabled =
        Boolean.parseBoolean(
            properties.getOrDefault("openhouse.hdfs.diagnostic.logging.enabled", "true"));
    try {
      init(conf());
    } catch (Exception e) {
      log.warn("Failed to initialize HDFS diagnostics", e);
      this.enabled = false;
    }
  }

  private void init(Configuration conf) {
    if (conf == null) return;
    try {
      FileSystem fs = FileSystem.get(conf);
      logConfig(conf);
      if (fs instanceof DistributedFileSystem) {
        log.info("HDFS_CONFIG fs_impl=DistributedFileSystem");
      } else {
        log.info("HDFS_CONFIG fs_impl={}", fs.getClass().getName());
      }
    } catch (IOException e) {
      log.warn("Failed to get FileSystem for diagnostics", e);
    }
  }

  private void logConfig(Configuration conf) {
    log.info(
        "HDFS_CONFIG hedged_pool={} hedged_threshold_ms={} failover_base_ms={} failover_max_ms={} socket_timeout_ms={} replace_dn_on_failure={}",
        conf.get("dfs.client.hedged.read.threadpool.size", "0"),
        conf.get("dfs.client.hedged.read.threshold.millis", "500"),
        conf.get("dfs.client.failover.sleep.base.millis", "500"),
        conf.get("dfs.client.failover.sleep.max.millis", "15000"),
        conf.get("dfs.client.socket-timeout", "60000"),
        conf.get("dfs.client.block.write.replace-datanode-on-failure.enable", "true"));
  }

  @Override
  public InputFile newInputFile(String path) {
    maybeLogStats();
    if (!enabled) return super.newInputFile(path);
    return new DiagnosticInputFile((HadoopInputFile) super.newInputFile(path), path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    maybeLogStats();
    if (!enabled) return super.newOutputFile(path);
    return new DiagnosticOutputFile((HadoopOutputFile) super.newOutputFile(path), path);
  }

  private void maybeLogStats() {
    long now = System.currentTimeMillis();
    if (now - lastStatsTime < STATS_INTERVAL_MS) return;
    lastStatsTime = now;

    log.info(
        "HDFS_STATS reads={} writes={} slow_nn={} slow_dn={} slow_close={}",
        totalReads.get(),
        totalWrites.get(),
        slowNnReads.get(),
        slowDnReads.get(),
        slowCloses.get());
  }

  private static String truncate(String path) {
    if (path == null || path.length() <= 60) return path;
    return "..." + path.substring(path.length() - 57);
  }

  // ========== Input File Wrapper ==========

  private class DiagnosticInputFile implements InputFile {
    private final HadoopInputFile delegate;
    private final String path;

    DiagnosticInputFile(HadoopInputFile delegate, String path) {
      this.delegate = delegate;
      this.path = path;
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      long openStart = System.currentTimeMillis();
      SeekableInputStream stream = delegate.newStream();
      long nnMs = System.currentTimeMillis() - openStart;
      return new DiagnosticInputStream(stream, path, openStart, nnMs);
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      return delegate.exists();
    }
  }

  // ========== Input Stream Wrapper ==========

  private class DiagnosticInputStream extends SeekableInputStream {
    private final SeekableInputStream delegate;
    private final String path;
    private final long startTime;
    private final long nnMs;
    private long bytes = 0;

    DiagnosticInputStream(SeekableInputStream delegate, String path, long startTime, long nnMs) {
      this.delegate = delegate;
      this.path = path;
      this.startTime = startTime;
      this.nnMs = nnMs;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long pos) throws IOException {
      delegate.seek(pos);
    }

    @Override
    public int read() throws IOException {
      int b = delegate.read();
      if (b >= 0) bytes++;
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int n = delegate.read(b, off, len);
      if (n > 0) bytes += n;
      return n;
    }

    @Override
    public void close() throws IOException {
      try {
        delegate.close();
      } finally {
        long totalMs = System.currentTimeMillis() - startTime;
        long dnMs = totalMs - nnMs;

        totalReads.incrementAndGet();
        if (nnMs > dnMs && nnMs > 100) slowNnReads.incrementAndGet();
        if (dnMs > nnMs && dnMs > 100) slowDnReads.incrementAndGet();

        log.info(
            "HDFS_READ path={} total_ms={} nn_ms={} dn_ms={} bytes={}",
            truncate(path),
            totalMs,
            nnMs,
            dnMs,
            bytes);

        if (totalMs > 500) {
          if (nnMs > dnMs) {
            log.warn(
                "HDFS_SLOW_NN path={} nn_ms={} - consider tuning failover timeouts",
                truncate(path),
                nnMs);
          } else {
            log.warn(
                "HDFS_SLOW_DN path={} dn_ms={} - consider tuning hedged reads",
                truncate(path),
                dnMs);
          }
        }
      }
    }
  }

  // ========== Output File Wrapper ==========

  private class DiagnosticOutputFile implements OutputFile {
    private final HadoopOutputFile delegate;
    private final String path;

    DiagnosticOutputFile(HadoopOutputFile delegate, String path) {
      this.delegate = delegate;
      this.path = path;
    }

    @Override
    public PositionOutputStream create() {
      long createStart = System.currentTimeMillis();
      PositionOutputStream stream = delegate.create();
      long createMs = System.currentTimeMillis() - createStart;
      return new DiagnosticOutputStream(stream, path, createStart, createMs);
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      long createStart = System.currentTimeMillis();
      PositionOutputStream stream = delegate.createOrOverwrite();
      long createMs = System.currentTimeMillis() - createStart;
      return new DiagnosticOutputStream(stream, path, createStart, createMs);
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public InputFile toInputFile() {
      return new DiagnosticInputFile((HadoopInputFile) delegate.toInputFile(), path);
    }
  }

  // ========== Output Stream Wrapper ==========

  private class DiagnosticOutputStream extends PositionOutputStream {
    private final PositionOutputStream delegate;
    private final String path;
    private final long startTime;
    private final long createMs;
    private long bytes = 0;

    DiagnosticOutputStream(
        PositionOutputStream delegate, String path, long startTime, long createMs) {
      this.delegate = delegate;
      this.path = path;
      this.startTime = startTime;
      this.createMs = createMs;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void write(int b) throws IOException {
      delegate.write(b);
      bytes++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      delegate.write(b, off, len);
      bytes += len;
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      long closeStart = System.currentTimeMillis();
      long writeMs = closeStart - startTime - createMs;

      try {
        delegate.close();
      } finally {
        long closeMs = System.currentTimeMillis() - closeStart;
        long totalMs = System.currentTimeMillis() - startTime;

        totalWrites.incrementAndGet();
        if (closeMs > 500) slowCloses.incrementAndGet();

        log.info(
            "HDFS_WRITE path={} total_ms={} create_ms={} write_ms={} close_ms={} bytes={}",
            truncate(path),
            totalMs,
            createMs,
            writeMs,
            closeMs,
            bytes);

        if (closeMs > 500) {
          log.warn(
              "HDFS_SLOW_CLOSE path={} close_ms={} - consider enabling pipeline recovery",
              truncate(path),
              closeMs);
        }
      }
    }
  }
}
