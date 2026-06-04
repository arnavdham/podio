#include "catch2/catch_test_macros.hpp"

#include "podio/ArrowWriter.h"
#include "podio/Frame.h"

// Test datatypes
#include "datamodel/EventInfoCollection.h"
#include "datamodel/ExampleClusterCollection.h"
#include "datamodel/ExampleHitCollection.h"

// Arrow headers
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>

#include <filesystem>

TEST_CASE("ArrowWriter - Multi-Category Buffering and Writing", "[arrow]") {
  const std::string filename = "test_arrow_output.arrow";

  // Cleanup existing file
  if (std::filesystem::exists(filename)) {
    std::filesystem::remove(filename);
  }

  {
    podio::ArrowWriter writer(filename);

    // 1. Create a "runs" frame
    podio::Frame runFrame1;
    ExampleClusterCollection clusters1;
    runFrame1.put(std::move(clusters1), "ExampleClusters");

    // 2. Create an "events" frame
    podio::Frame eventFrame1;
    EventInfoCollection eventInfo1;
    ExampleHitCollection hits1;
    eventFrame1.put(std::move(eventInfo1), "EventInfo");
    eventFrame1.put(std::move(hits1), "ExampleHits");

    // 3. Create another "events" frame
    podio::Frame eventFrame2;
    EventInfoCollection eventInfo2;
    ExampleHitCollection hits2;
    eventFrame2.put(std::move(eventInfo2), "EventInfo");
    eventFrame2.put(std::move(hits2), "ExampleHits");

    // 4. Create another "runs" frame
    podio::Frame runFrame2;
    ExampleClusterCollection clusters2;
    runFrame2.put(std::move(clusters2), "ExampleClusters");

    // Write them interspersed
    writer.writeFrame(runFrame1, "runs");
    writer.writeFrame(eventFrame1, "events");
    writer.writeFrame(eventFrame2, "events");
    writer.writeFrame(runFrame2, "runs");

    writer.finish();
  }

  // Verify the output file
  REQUIRE(std::filesystem::exists(filename));

  // Re-open using Arrow IPC Reader
  auto fileResult = arrow::io::ReadableFile::Open(filename);
  REQUIRE(fileResult.ok());
  auto file = fileResult.ValueOrDie();

  auto readerResult = arrow::ipc::RecordBatchFileReader::Open(file);
  REQUIRE(readerResult.ok());
  auto reader = readerResult.ValueOrDie();

  auto schema = reader->schema();
  REQUIRE(schema != nullptr);

  // Schema should have: __category__ (utf8), runs (struct), events (struct)
  auto catField = schema->GetFieldByName("__category__");
  REQUIRE(catField != nullptr);
  REQUIRE(catField->type()->id() == arrow::Type::STRING);

  auto runsField = schema->GetFieldByName("runs");
  REQUIRE(runsField != nullptr);
  REQUIRE(runsField->type()->id() == arrow::Type::STRUCT);

  auto eventsField = schema->GetFieldByName("events");
  REQUIRE(eventsField != nullptr);
  REQUIRE(eventsField->type()->id() == arrow::Type::STRUCT);

  // Struct fields check
  auto runsStruct = std::static_pointer_cast<arrow::StructType>(runsField->type());
  REQUIRE(runsStruct->GetFieldByName("ExampleClusters") != nullptr);

  auto eventsStruct = std::static_pointer_cast<arrow::StructType>(eventsField->type());
  REQUIRE(eventsStruct->GetFieldByName("EventInfo") != nullptr);
  REQUIRE(eventsStruct->GetFieldByName("ExampleHits") != nullptr);

  // Read the record batch and verify rows
  REQUIRE(reader->num_record_batches() == 1);
  auto batchResult = reader->ReadRecordBatch(0);
  REQUIRE(batchResult.ok());
  auto batch = batchResult.ValueOrDie();

  REQUIRE(batch->num_rows() == 4);

  // Verify __category__ values
  auto catArray = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
  REQUIRE(catArray->GetString(0) == "runs");
  REQUIRE(catArray->GetString(1) == "events");
  REQUIRE(catArray->GetString(2) == "events");
  REQUIRE(catArray->GetString(3) == "runs");

  // Verify runs struct nullability/validity
  auto runsArray = std::static_pointer_cast<arrow::StructArray>(batch->GetColumnByName("runs"));
  REQUIRE(runsArray != nullptr);
  REQUIRE(runsArray->IsValid(0));
  REQUIRE(runsArray->IsNull(1));
  REQUIRE(runsArray->IsNull(2));
  REQUIRE(runsArray->IsValid(3));

  // Verify events struct nullability/validity
  auto eventsArray = std::static_pointer_cast<arrow::StructArray>(batch->GetColumnByName("events"));
  REQUIRE(eventsArray != nullptr);
  REQUIRE(eventsArray->IsNull(0));
  REQUIRE(eventsArray->IsValid(1));
  REQUIRE(eventsArray->IsValid(2));
  REQUIRE(eventsArray->IsNull(3));

  // Clean up
  std::filesystem::remove(filename);
}
