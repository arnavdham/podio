#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <chrono>
#include <thread>
#include <mutex>
#include <numeric>
#include <cmath>
#include <memory>
#include <algorithm>
#include <cstring>
#include <sstream>
#include <atomic>
#include <map>

#include <dlfcn.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "podio/Frame.h"
#include "podio/ROOTReader.h"
#include "podio/utilities/ArrowFrameConverter.h"
#include "podio/utilities/ArrowFrameData.h"
#include "podio/utilities/ArrowConverterRegistry.h"
#include "podio/utilities/ArrowTypeRegistry.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#ifdef PODIO_ENABLE_TEST_DATAMODEL
#include "datamodel/ExampleHitCollection.h"
#include "datamodel/ExampleClusterCollection.h"
#include "datamodel/ExampleWithOneRelationCollection.h"
#include "datamodel/ExampleWithVectorMemberCollection.h"
#endif

struct Options {
  std::string input{};
  std::string category{"events"};
  std::string events{"all"};
  int iterations{1};
  std::string mode{"pipeline"};
  std::string preload_frames{"off"};
  std::string collections{"auto-supported"};
  bool strict_collections{false};
  int pipe_size{1048576};
  std::vector<std::string> load_libs{};
  int64_t root_bytes_per_frame{-1};
  std::string output_json{};
};

struct StageTimings {
  double root_read = 0.0;
  double frame_to_arrow = 0.0;
  double stream_write = 0.0;
  double stream_read = 0.0;
  double arrow_to_frame = 0.0;
  double materialization = 0.0;
};

struct Stats {
  double min = 0.0;
  double max = 0.0;
  double mean = 0.0;
  double median = 0.0;
  double stddev = 0.0;
};

Stats compute_stats(std::vector<double> vals) {
  if (vals.empty()) return {};
  std::sort(vals.begin(), vals.end());
  Stats s;
  s.min = vals.front();
  s.max = vals.back();
  double sum = std::accumulate(vals.begin(), vals.end(), 0.0);
  s.mean = sum / vals.size();
  
  if (vals.size() % 2 == 0) {
    s.median = (vals[vals.size() / 2 - 1] + vals[vals.size() / 2]) / 2.0;
  } else {
    s.median = vals[vals.size() / 2];
  }
  
  double sq_sum = 0.0;
  for (double v : vals) {
    sq_sum += (v - s.mean) * (v - s.mean);
  }
  s.stddev = std::sqrt(sq_sum / vals.size());
  return s;
}

int64_t get_file_size(const std::string& path) {
  struct stat st;
  if (stat(path.c_str(), &st) == 0) {
    return st.st_size;
  }
  return 0;
}

void print_usage(const char* prog) {
  std::cout << "Usage: " << prog << " [options]\n"
            << "Options:\n"
            << "  --input <path>               ROOT input file (required)\n"
            << "  --category <name>            Category to read (default: events)\n"
            << "  --events <N|all>             Number of events to process (default: all)\n"
            << "  --iterations <N>             Number of measured iterations (default: 1)\n"
            << "  --mode <sequential|pipeline> Execution mode (default: pipeline)\n"
            << "  --preload-frames <on|off>    Preload frames in memory (default: off)\n"
            << "  --collections <list>         Comma-separated list of collections, or 'auto-supported' (default)\n"
            << "  --strict-collections         Fail on unsupported collections (default: false)\n"
            << "  --pipe-size <bytes>          UNIX pipe buffer size request (default: 1048576)\n"
            << "  --load-lib <path>            Repeatable, load datamodel library (e.g. TestDataModel)\n"
            << "  --root-bytes-per-frame <B>   Override ROOT bytes per frame\n"
            << "  --output-json <path>         Write results to a JSON file\n"
            << "  -h, --help                   Show this help message\n";
}

Options parse_args(int argc, char* argv[]) {
  Options opts;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--input" && i + 1 < argc) {
      opts.input = argv[++i];
    } else if (arg == "--category" && i + 1 < argc) {
      opts.category = argv[++i];
    } else if (arg == "--events" && i + 1 < argc) {
      opts.events = argv[++i];
    } else if (arg == "--iterations" && i + 1 < argc) {
      opts.iterations = std::stoi(argv[++i]);
    } else if (arg == "--mode" && i + 1 < argc) {
      opts.mode = argv[++i];
    } else if (arg == "--preload-frames" && i + 1 < argc) {
      opts.preload_frames = argv[++i];
    } else if (arg == "--collections" && i + 1 < argc) {
      opts.collections = argv[++i];
    } else if (arg == "--strict-collections") {
      opts.strict_collections = true;
    } else if (arg == "--pipe-size" && i + 1 < argc) {
      opts.pipe_size = std::stoi(argv[++i]);
    } else if (arg == "--load-lib" && i + 1 < argc) {
      opts.load_libs.push_back(argv[++i]);
    } else if (arg == "--root-bytes-per-frame" && i + 1 < argc) {
      opts.root_bytes_per_frame = std::stoll(argv[++i]);
    } else if (arg == "--output-json" && i + 1 < argc) {
      opts.output_json = argv[++i];
    } else if (arg == "-h" || arg == "--help") {
      print_usage(argv[0]);
      std::exit(0);
    } else {
      std::cerr << "Unknown argument: " << arg << "\n";
      print_usage(argv[0]);
      std::exit(1);
    }
  }
  if (opts.input.empty()) {
    std::cerr << "Error: --input is required.\n";
    print_usage(argv[0]);
    std::exit(1);
  }
  return opts;
}

double touch_and_checksum(const podio::Frame& frame, const std::vector<std::string>& selected_cols) {
  double checksum = 0.0;
  for (const auto& name : selected_cols) {
    const auto* coll = frame.get(name);
    if (!coll) continue;
    checksum += coll->getID();
    checksum += coll->size();
    
#ifdef PODIO_ENABLE_TEST_DATAMODEL
    if (coll->getValueTypeName() == "ExampleHit") {
      const auto& hits = *static_cast<const ExampleHitCollection*>(coll);
      for (const auto& hit : hits) {
        checksum += hit.x() + hit.y() + hit.z() + hit.energy();
      }
      continue;
    }
    if (coll->getValueTypeName() == "ExampleCluster") {
      const auto& clusters = *static_cast<const ExampleClusterCollection*>(coll);
      for (const auto& cluster : clusters) {
        checksum += cluster.energy();
        for (const auto& hit : cluster.Hits()) {
          checksum += hit.energy();
        }
      }
      continue;
    }
    if (coll->getValueTypeName() == "ExampleWithOneRelation") {
      const auto& rels = *static_cast<const ExampleWithOneRelationCollection*>(coll);
      for (const auto& rel : rels) {
        if (rel.cluster().isAvailable()) {
          checksum += rel.cluster().energy();
        }
      }
      continue;
    }
    if (coll->getValueTypeName() == "ExampleWithVectorMember") {
      const auto& vecs = *static_cast<const ExampleWithVectorMemberCollection*>(coll);
      for (const auto& vec : vecs) {
        for (size_t i = 0; i < vec.count_size(); ++i) {
          checksum += vec.count(i);
        }
      }
      continue;
    }
#endif

    checksum += coll->size();
    auto buffers = const_cast<podio::CollectionBase*>(coll)->getBuffers();
    if (buffers.data) {
      checksum += reinterpret_cast<uintptr_t>(buffers.data) % 1000;
    }
    if (buffers.references) {
      for (const auto& ref : *buffers.references) {
        if (ref && !ref->empty()) {
          checksum += ref->size() + ref->front().index + ref->front().collectionID;
        }
      }
    }
    if (buffers.vectorMembers) {
      for (const auto& vm : *buffers.vectorMembers) {
        checksum += reinterpret_cast<uintptr_t>(vm.second) % 1000;
      }
    }
  }
  return checksum;
}

std::vector<std::string> get_selected_collections(const podio::Frame& first_frame, const std::string& collections_arg, bool strict, std::vector<std::string>& skipped_names, std::vector<std::string>& skipped_types) {
  std::vector<std::string> all_cols = first_frame.getAvailableCollections();
  std::vector<std::string> selected;
  
  std::vector<std::string> filter;
  if (collections_arg != "auto-supported") {
    std::stringstream ss(collections_arg);
    std::string item;
    while (std::getline(ss, item, ',')) {
      filter.push_back(item);
    }
  }

  for (const auto& name : all_cols) {
    if (!filter.empty() && std::find(filter.begin(), filter.end(), name) == filter.end()) {
      continue;
    }
    const auto* coll = first_frame.get(name);
    if (!coll) continue;

    bool is_supported = false;
    if (coll->isSubsetCollection()) {
      is_supported = true;
    } else {
      std::string typeName = std::string(coll->getValueTypeName());
      auto converter = podio::ArrowConverterRegistry::instance().getConverter(typeName);
      auto arrowType = podio::ArrowTypeRegistry::instance().getType(typeName);
      if (converter && arrowType) {
        is_supported = true;
      } else {
        skipped_names.push_back(name);
        skipped_types.push_back(typeName);
      }
    }

    if (is_supported) {
      selected.push_back(name);
    } else if (strict) {
      throw std::runtime_error("Collection '" + name + "' of type '" + std::string(coll->getValueTypeName()) + "' is not supported by Arrow converter");
    }
  }
  return selected;
}

class PipeOutputStream : public arrow::io::OutputStream {
public:
  explicit PipeOutputStream(int fd) : m_fd(fd), m_bytes_written(0) {}
  arrow::Status Write(const void* data, int64_t nbytes) override {
    int64_t bytes_written = 0;
    while (bytes_written < nbytes) {
      auto res = write(m_fd, static_cast<const char*>(data) + bytes_written, nbytes - bytes_written);
      if (res < 0) {
        if (errno == EINTR) continue;
        return arrow::Status::IOError("pipe write failed: ", strerror(errno));
      }
      bytes_written += res;
    }
    m_bytes_written += bytes_written;
    return arrow::Status::OK();
  }
  arrow::Status Close() override {
    close(m_fd);
    return arrow::Status::OK();
  }
  arrow::Result<int64_t> Tell() const override { return m_bytes_written; }
  bool closed() const override { return false; }
  int64_t bytes_written() const { return m_bytes_written; }
private:
  int m_fd;
  int64_t m_bytes_written;
};

class PipeInputStream : public arrow::io::InputStream {
public:
  explicit PipeInputStream(int fd) : m_fd(fd), m_bytes_read(0) {}
  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    int64_t bytes_read = 0;
    while (bytes_read < nbytes) {
      auto res = read(m_fd, static_cast<char*>(out) + bytes_read, nbytes - bytes_read);
      if (res < 0) {
        if (errno == EINTR) continue;
        return arrow::Status::IOError("pipe read failed: ", strerror(errno));
      }
      if (res == 0) {
        break; // EOF
      }
      bytes_read += res;
    }
    m_bytes_read += bytes_read;
    return bytes_read;
  }
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    auto result = arrow::AllocateBuffer(nbytes);
    if (!result.ok()) return result.status();
    std::shared_ptr<arrow::Buffer> buf = std::move(result).ValueOrDie();
    auto read_res = Read(nbytes, buf->mutable_data());
    if (!read_res.ok()) return read_res.status();
    auto bytes_read = read_res.ValueOrDie();
    if (bytes_read < nbytes) {
      buf = arrow::SliceBuffer(buf, 0, bytes_read);
    }
    return buf;
  }
  arrow::Status Close() override {
    close(m_fd);
    return arrow::Status::OK();
  }
  arrow::Result<int64_t> Tell() const override { return m_bytes_read; }
  bool closed() const override { return false; }
  int64_t bytes_read() const { return m_bytes_read; }
private:
  int m_fd;
  int64_t m_bytes_read;
};

void writer_thread_func(
    int write_fd,
    std::vector<podio::Frame>& preloaded_frames,
    const std::string& input_file,
    const std::string& category,
    unsigned num_events,
    bool preload,
    const std::vector<std::string>& selected_cols,
    std::vector<StageTimings>& thread_timings,
    std::atomic<bool>& error_flag,
    std::atomic<int64_t>& total_serialized_bytes
) {
  try {
    auto pipe_stream = std::make_shared<PipeOutputStream>(write_fd);
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;

    podio::ROOTReader root_reader;
    if (!preload) {
      root_reader.openFile(input_file);
    }

    for (unsigned i = 0; i < num_events; ++i) {
      StageTimings timings;
      podio::Frame frame;

      auto t0 = std::chrono::high_resolution_clock::now();
      if (preload) {
        frame = std::move(preloaded_frames[i]);
      } else {
        auto frameData = root_reader.readEntry(category, i);
        if (!frameData) {
          throw std::runtime_error("Failed to read event " + std::to_string(i));
        }
        frame = podio::Frame(std::move(frameData));
      }
      auto t1 = std::chrono::high_resolution_clock::now();
      timings.root_read = std::chrono::duration<double>(t1 - t0).count();

      auto t2 = std::chrono::high_resolution_clock::now();
      auto table = podio::convertFrameToTable(frame, selected_cols);
      auto t3 = std::chrono::high_resolution_clock::now();
      timings.frame_to_arrow = std::chrono::duration<double>(t3 - t2).count();

      auto t4 = std::chrono::high_resolution_clock::now();
      if (!writer) {
        auto make_res = arrow::ipc::MakeStreamWriter(pipe_stream.get(), table->schema());
        if (!make_res.ok()) {
          throw std::runtime_error("Failed to create stream writer: " + make_res.status().ToString());
        }
        writer = make_res.ValueOrDie();
      }
      auto write_status = writer->WriteTable(*table);
      if (!write_status.ok()) {
        throw std::runtime_error("Writer failed to write table: " + write_status.ToString());
      }
      auto t5 = std::chrono::high_resolution_clock::now();
      timings.stream_write = std::chrono::duration<double>(t5 - t4).count();

      thread_timings.push_back(timings);
    }

    if (writer) {
      auto close_status = writer->Close();
      if (!close_status.ok()) {
        throw std::runtime_error("Writer failed to close: " + close_status.ToString());
      }
    }
    total_serialized_bytes = pipe_stream->bytes_written();
    (void)pipe_stream->Close();
  } catch (const std::exception& e) {
    std::cerr << "Writer thread error: " << e.what() << std::endl;
    error_flag = true;
    close(write_fd);
  }
}

void reader_thread_func(
    int read_fd,
    unsigned num_events,
    const std::vector<std::string>& selected_cols,
    std::vector<StageTimings>& thread_timings,
    std::vector<double>& checksums,
    std::atomic<bool>& error_flag
) {
  try {
    auto pipe_stream = std::make_shared<PipeInputStream>(read_fd);
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader;

    for (unsigned i = 0; i < num_events; ++i) {
      StageTimings timings;

      auto t0 = std::chrono::high_resolution_clock::now();
      if (!reader) {
        auto open_res = arrow::ipc::RecordBatchStreamReader::Open(pipe_stream);
        if (!open_res.ok()) {
          throw std::runtime_error("Reader failed to open stream: " + open_res.status().ToString());
        }
        reader = open_res.ValueOrDie();
      }

      std::shared_ptr<arrow::RecordBatch> batch;
      auto read_status = reader->ReadNext(&batch);
      if (!read_status.ok()) {
        throw std::runtime_error("Reader failed to read next batch: " + read_status.ToString());
      }
      if (!batch) {
        throw std::runtime_error("Reader encountered premature EOF at event " + std::to_string(i));
      }
      auto read_table = arrow::Table::FromRecordBatches(reader->schema(), {batch}).ValueOrDie();
      auto t1 = std::chrono::high_resolution_clock::now();
      timings.stream_read = std::chrono::duration<double>(t1 - t0).count();

      auto t2 = std::chrono::high_resolution_clock::now();
      auto recFrame = podio::convertTableToFrame(read_table, 0);
      auto t3 = std::chrono::high_resolution_clock::now();
      timings.arrow_to_frame = std::chrono::duration<double>(t3 - t2).count();

      auto t4 = std::chrono::high_resolution_clock::now();
      double chk = touch_and_checksum(*recFrame, selected_cols);
      auto t5 = std::chrono::high_resolution_clock::now();
      timings.materialization = std::chrono::duration<double>(t5 - t4).count();

      checksums.push_back(chk);
      thread_timings.push_back(timings);
    }

    (void)pipe_stream->Close();
  } catch (const std::exception& e) {
    std::cerr << "Reader thread error: " << e.what() << std::endl;
    error_flag = true;
    close(read_fd);
  }
}

std::string format_stats_json(const std::string& name, const Stats& s) {
  std::stringstream ss;
  ss << "\"" << name << "\": {"
     << "\"min\": " << s.min << ", "
     << "\"max\": " << s.max << ", "
     << "\"mean\": " << s.mean << ", "
     << "\"median\": " << s.median << ", "
     << "\"stddev\": " << s.stddev << "}";
  return ss.str();
}

int main(int argc, char* argv[]) {
  Options options = parse_args(argc, argv);

  for (const auto& lib : options.load_libs) {
    std::cout << "Loading library: " << lib << std::endl;
    void* handle = dlopen(lib.c_str(), RTLD_NOW | RTLD_GLOBAL);
    if (!handle) {
      std::cerr << "Error: failed to load library " << lib << ": " << dlerror() << std::endl;
      return 1;
    }
  }

  podio::ROOTReader reader;
  reader.openFile(options.input);
  unsigned total_entries = reader.getEntries(options.category);
  unsigned num_events = total_entries;
  if (options.events != "all") {
    num_events = std::min(total_entries, static_cast<unsigned>(std::stoul(options.events)));
  }

  std::cout << "ROOT File Size: " << get_file_size(options.input) << " bytes" << std::endl;
  std::cout << "Total entries in category '" << options.category << "': " << total_entries << std::endl;
  std::cout << "Events to process per iteration: " << num_events << std::endl;
  std::cout << "Measured iterations: " << options.iterations << std::endl;
  std::cout << "Mode: " << options.mode << std::endl;
  std::cout << "Preload: " << options.preload_frames << std::endl;

  auto firstFrameData = reader.readEntry(options.category, 0);
  if (!firstFrameData) {
    std::cerr << "Error: Failed to read first entry from category '" << options.category << "'" << std::endl;
    return 1;
  }
  podio::Frame firstFrame(std::move(firstFrameData));

  std::vector<std::string> skipped_names, skipped_types;
  std::vector<std::string> selected_cols = get_selected_collections(
      firstFrame, options.collections, options.strict_collections, skipped_names, skipped_types);

  std::cout << "Selected collections (" << selected_cols.size() << "): ";
  for (const auto& name : selected_cols) std::cout << name << " ";
  std::cout << std::endl;

  std::cout << "Skipped collections (" << skipped_names.size() << "):" << std::endl;
  for (size_t i = 0; i < skipped_names.size(); ++i) {
    std::cout << "  " << skipped_names[i] << " (" << skipped_types[i] << ")" << std::endl;
  }

  bool preload = (options.preload_frames == "on");

  std::vector<double> iter_durations;
  std::vector<double> all_root_read;
  std::vector<double> all_frame_to_arrow;
  std::vector<double> all_stream_write;
  std::vector<double> all_stream_read;
  std::vector<double> all_arrow_to_frame;
  std::vector<double> all_materialization;

  int64_t measured_serialized_bytes = 0;
  double final_checksum = 0.0;
  int pipe_sz_result = -1;

  for (int iter = 0; iter < options.iterations; ++iter) {
    std::cout << "--- Measured iteration " << (iter + 1) << " ---" << std::endl;

    std::vector<podio::Frame> preloaded_frames;
    if (preload) {
      podio::ROOTReader temp_reader;
      temp_reader.openFile(options.input);
      preloaded_frames.reserve(num_events);
      for (unsigned i = 0; i < num_events; ++i) {
        auto fd = temp_reader.readEntry(options.category, i);
        preloaded_frames.emplace_back(std::move(fd));
      }
    }

    std::vector<StageTimings> iter_timings;
    std::vector<double> iter_checksums;
    int64_t iter_bytes = 0;

    auto t_start = std::chrono::high_resolution_clock::now();

    if (options.mode == "sequential") {
      podio::ROOTReader seq_reader;
      if (!preload) {
        seq_reader.openFile(options.input);
      }

      auto buffer_output = arrow::io::BufferOutputStream::Create().ValueOrDie();
      std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;

      struct TempSeqTimings {
        double root_read = 0.0;
        double frame_to_arrow = 0.0;
        double stream_write = 0.0;
      };
      std::vector<TempSeqTimings> write_stage_timings;
      write_stage_timings.reserve(num_events);

      // Loop 1: Serialize all events into a single Arrow IPC Stream
      for (unsigned i = 0; i < num_events; ++i) {
        TempSeqTimings timings;
        podio::Frame frame;

        auto t0 = std::chrono::high_resolution_clock::now();
        if (preload) {
          frame = std::move(preloaded_frames[i]);
        } else {
          auto frameData = seq_reader.readEntry(options.category, i);
          frame = podio::Frame(std::move(frameData));
        }
        auto t1 = std::chrono::high_resolution_clock::now();
        timings.root_read = std::chrono::duration<double>(t1 - t0).count();

        auto t2 = std::chrono::high_resolution_clock::now();
        auto table = podio::convertFrameToTable(frame, selected_cols);
        auto t3 = std::chrono::high_resolution_clock::now();
        timings.frame_to_arrow = std::chrono::duration<double>(t3 - t2).count();

        auto t4 = std::chrono::high_resolution_clock::now();
        if (!writer) {
          writer = arrow::ipc::MakeStreamWriter(buffer_output.get(), table->schema()).ValueOrDie();
        }
        auto write_status = writer->WriteTable(*table);
        if (!write_status.ok()) {
          throw std::runtime_error("Writer failed to write table: " + write_status.ToString());
        }
        auto t5 = std::chrono::high_resolution_clock::now();
        timings.stream_write = std::chrono::duration<double>(t5 - t4).count();

        write_stage_timings.push_back(timings);
      }

      if (writer) {
        auto close_status = writer->Close();
        if (!close_status.ok()) {
          throw std::runtime_error("Writer failed to close: " + close_status.ToString());
        }
      }
      auto buffer = buffer_output->Finish().ValueOrDie();
      iter_bytes = buffer->size();

      // Loop 2: Deserialize all events batch-by-batch from the single stream
      auto buffer_input = std::make_shared<arrow::io::BufferReader>(buffer);
      auto reader_stream = arrow::ipc::RecordBatchStreamReader::Open(buffer_input).ValueOrDie();

      for (unsigned i = 0; i < num_events; ++i) {
        StageTimings timings;
        timings.root_read = write_stage_timings[i].root_read;
        timings.frame_to_arrow = write_stage_timings[i].frame_to_arrow;
        timings.stream_write = write_stage_timings[i].stream_write;

        auto t6 = std::chrono::high_resolution_clock::now();
        std::shared_ptr<arrow::RecordBatch> batch;
        auto read_status = reader_stream->ReadNext(&batch);
        if (!read_status.ok()) {
          throw std::runtime_error("Reader failed to read next batch: " + read_status.ToString());
        }
        if (!batch) {
          throw std::runtime_error("Reader encountered premature EOF at event " + std::to_string(i));
        }
        auto read_table = arrow::Table::FromRecordBatches(reader_stream->schema(), {batch}).ValueOrDie();
        auto t7 = std::chrono::high_resolution_clock::now();
        timings.stream_read = std::chrono::duration<double>(t7 - t6).count();

        auto t8 = std::chrono::high_resolution_clock::now();
        auto recFrame = podio::convertTableToFrame(read_table, 0);
        auto t9 = std::chrono::high_resolution_clock::now();
        timings.arrow_to_frame = std::chrono::duration<double>(t9 - t8).count();

        auto t10 = std::chrono::high_resolution_clock::now();
        double chk = touch_and_checksum(*recFrame, selected_cols);
        auto t11 = std::chrono::high_resolution_clock::now();
        timings.materialization = std::chrono::duration<double>(t11 - t10).count();

        iter_timings.push_back(timings);
        iter_checksums.push_back(chk);
      }
    } else {
      int pipe_fds[2];
      if (pipe(pipe_fds) < 0) {
        std::cerr << "Error: failed to create pipe." << std::endl;
        return 1;
      }

#ifdef F_SETPIPE_SZ
      pipe_sz_result = fcntl(pipe_fds[1], F_SETPIPE_SZ, options.pipe_size);
      if (pipe_sz_result < 0) {
        std::cerr << "Warning: F_SETPIPE_SZ to " << options.pipe_size << " failed: " << strerror(errno) << std::endl;
      }
#endif

      std::atomic<bool> error_flag{false};
      std::atomic<int64_t> thread_bytes{0};
      std::vector<StageTimings> writer_timings;
      std::vector<StageTimings> reader_timings;

      std::thread writer_thread(
          writer_thread_func,
          pipe_fds[1],
          std::ref(preloaded_frames),
          options.input,
          options.category,
          num_events,
          preload,
          std::ref(selected_cols),
          std::ref(writer_timings),
          std::ref(error_flag),
          std::ref(thread_bytes)
      );

      std::thread reader_thread(
          reader_thread_func,
          pipe_fds[0],
          num_events,
          std::ref(selected_cols),
          std::ref(reader_timings),
          std::ref(iter_checksums),
          std::ref(error_flag)
      );

      writer_thread.join();
      reader_thread.join();

      if (error_flag) {
        std::cerr << "Error occurred in pipe threads, aborting." << std::endl;
        return 1;
      }

      iter_bytes = thread_bytes;

      for (unsigned i = 0; i < num_events; ++i) {
        StageTimings timings;
        timings.root_read = writer_timings[i].root_read;
        timings.frame_to_arrow = writer_timings[i].frame_to_arrow;
        timings.stream_write = writer_timings[i].stream_write;
        timings.stream_read = reader_timings[i].stream_read;
        timings.arrow_to_frame = reader_timings[i].arrow_to_frame;
        timings.materialization = reader_timings[i].materialization;
        iter_timings.push_back(timings);
      }
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double>(t_end - t_start).count();

    std::cout << "  Duration: " << duration << " s" << std::endl;
    std::cout << "  Throughput: " << (num_events / duration) << " frames/s" << std::endl;

    double sum_chk = std::accumulate(iter_checksums.begin(), iter_checksums.end(), 0.0);
    std::cout << "  Checksum: " << sum_chk << std::endl;

    iter_durations.push_back(duration);
    measured_serialized_bytes += iter_bytes;
    final_checksum = sum_chk;

    for (const auto& t : iter_timings) {
      all_root_read.push_back(t.root_read);
      all_frame_to_arrow.push_back(t.frame_to_arrow);
      all_stream_write.push_back(t.stream_write);
      all_stream_read.push_back(t.stream_read);
      all_arrow_to_frame.push_back(t.arrow_to_frame);
      all_materialization.push_back(t.materialization);
    }
  }

  std::cout << "=== Execution Completed ===" << std::endl;

  Stats duration_stats = compute_stats(iter_durations);
  Stats root_read_stats = compute_stats(all_root_read);
  Stats f2a_stats = compute_stats(all_frame_to_arrow);
  Stats write_stats = compute_stats(all_stream_write);
  Stats read_stats = compute_stats(all_stream_read);
  Stats a2f_stats = compute_stats(all_arrow_to_frame);
  Stats mat_stats = compute_stats(all_materialization);

  double median_duration = duration_stats.median;
  double frames_per_sec = num_events / median_duration;

  int64_t file_size = get_file_size(options.input);
  double root_bytes_per_frame = options.root_bytes_per_frame;
  if (root_bytes_per_frame < 0) {
    root_bytes_per_frame = static_cast<double>(file_size) / total_entries;
  }
  double root_MBps = frames_per_sec * root_bytes_per_frame / 1e6;

  double ipc_bytes_per_frame = static_cast<double>(measured_serialized_bytes) / (options.iterations * num_events);
  double ipc_MBps = frames_per_sec * ipc_bytes_per_frame / 1e6;

  std::cout << "Results:" << std::endl;
  std::cout << "  Frames/sec (median duration): " << frames_per_sec << std::endl;
  std::cout << "  ROOT-equivalent rate: " << root_MBps << " MB/s (approx)" << std::endl;
  std::cout << "  Arrow Stream rate: " << ipc_MBps << " MB/s" << std::endl;
  std::cout << "  Checksum Verification: " << final_checksum << std::endl;

  std::cout << "Per-Stage Timing Stats (s/frame):" << std::endl;
  std::cout << "  ROOT read:       Median=" << root_read_stats.median << ", Mean=" << root_read_stats.mean << std::endl;
  std::cout << "  Frame -> Table:  Median=" << f2a_stats.median << ", Mean=" << f2a_stats.mean << std::endl;
  std::cout << "  Stream write:    Median=" << write_stats.median << ", Mean=" << write_stats.mean << std::endl;
  std::cout << "  Stream read:     Median=" << read_stats.median << ", Mean=" << read_stats.mean << std::endl;
  std::cout << "  Table -> Frame:  Median=" << a2f_stats.median << ", Mean=" << a2f_stats.mean << std::endl;
  std::cout << "  Materialize:     Median=" << mat_stats.median << ", Mean=" << mat_stats.mean << std::endl;

  if (!options.output_json.empty()) {
    std::ofstream out(options.output_json);
    if (out.is_open()) {
      out << "{\n"
          << "  \"config\": {\n"
          << "    \"input_file\": \"" << options.input << "\",\n"
          << "    \"category\": \"" << options.category << "\",\n"
          << "    \"events\": " << num_events << ",\n"
          << "    \"mode\": \"" << options.mode << "\",\n"
          << "    \"preload_frames\": \"" << options.preload_frames << "\",\n"
          << "    \"pipe_size_requested\": " << options.pipe_size << ",\n"
          << "    \"pipe_size_actual\": " << pipe_sz_result << "\n"
          << "  },\n"
          << "  \"metrics\": {\n"
          << "    \"frames_per_sec\": " << frames_per_sec << ",\n"
          << "    \"root_equivalent_mbps\": " << root_MBps << ",\n"
          << "    \"arrow_ipc_mbps\": " << ipc_MBps << ",\n"
          << "    \"root_bytes_per_frame\": " << root_bytes_per_frame << ",\n"
          << "    \"arrow_ipc_bytes_per_frame\": " << ipc_bytes_per_frame << ",\n"
          << "    \"checksum\": " << final_checksum << "\n"
          << "  },\n"
          << "  \"timings\": {\n"
          << "    " << format_stats_json("duration", duration_stats) << ",\n"
          << "    " << format_stats_json("root_read", root_read_stats) << ",\n"
          << "    " << format_stats_json("frame_to_arrow", f2a_stats) << ",\n"
          << "    " << format_stats_json("stream_write", write_stats) << ",\n"
          << "    " << format_stats_json("stream_read", read_stats) << ",\n"
          << "    " << format_stats_json("arrow_to_frame", a2f_stats) << ",\n"
          << "    " << format_stats_json("materialization", mat_stats) << "\n"
          << "  }\n"
          << "}\n";
      out.close();
      std::cout << "Written stats to JSON: " << options.output_json << std::endl;
    }
  }

  return 0;
}
