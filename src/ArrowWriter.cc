#include "podio/ArrowWriter.h"
#include "podio/CollectionBase.h"
#include "podio/Frame.h"
#include "podio/utilities/ArrowTypeRegistry.h"
#include "podio/utilities/ArrowConverterRegistry.h"
#include "podio/utilities/MiscHelpers.h"

#include <arrow/api.h>

#include <arrow/array/concatenate.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/vector.h>

#include <algorithm>
#include <iterator>
#include <sstream>
#include <stdexcept>

namespace podio {
namespace {
  std::tuple<std::vector<std::string>, std::vector<std::string>>
  getInconsistentColls(std::vector<std::string> existingColls, std::vector<std::string> candidateColls) {
    std::ranges::sort(existingColls);
    std::ranges::sort(candidateColls);

    std::vector<std::string> onlyExisting{};
    std::ranges::set_difference(existingColls, candidateColls, std::back_inserter(onlyExisting));

    std::vector<std::string> onlyCandidate{};
    std::ranges::set_difference(candidateColls, existingColls, std::back_inserter(onlyCandidate));

    return {std::move(onlyExisting), std::move(onlyCandidate)};
  }

  std::string getInconsistentCollsMsg(const std::vector<std::string>& existingColls,
                                      const std::vector<std::string>& candidateColls) {
    const auto [onlyExisting, onlyCandidates] = getInconsistentColls(existingColls, candidateColls);

    std::stringstream errMsg{};
    if (!onlyExisting.empty()) {
      errMsg << "Missing collections: ";
      for (const auto& collName : onlyExisting) {
        errMsg << collName << ", ";
      }
    }
    if (!onlyCandidates.empty()) {
      errMsg << "Additional collections: ";
      for (const auto& collName : onlyCandidates) {
        errMsg << collName << ", ";
      }
    }

    return errMsg.str();
  }
} // namespace

ArrowWriter::ArrowWriter(const std::string& filename) : m_filename(filename) {
}

ArrowWriter::~ArrowWriter() {
  finish();
}

void ArrowWriter::writeFrame(const podio::Frame& frame, std::string_view category) {
  writeFrame(frame, category, frame.getAvailableCollections());
}

void ArrowWriter::writeFrame(const podio::Frame& frame, std::string_view category,
                             const std::vector<std::string>& collsToWrite) {
  auto& catInfo = getCategoryInfo(category);

  if (catInfo.collsToWrite.empty() && catInfo.frameCount == 0) {
    initializeCategory(catInfo, frame, category, collsToWrite);
  } else {
    const auto [onlyExisting, onlyCandidates] = getInconsistentColls(catInfo.collsToWrite, collsToWrite);
    if (!onlyExisting.empty() || !onlyCandidates.empty()) {
      const auto inconsistencyMessage = getInconsistentCollsMsg(catInfo.collsToWrite, collsToWrite);
      throw std::runtime_error("Trying to write category '" + std::string(category) +
                               "' with inconsistent collection content. " + inconsistencyMessage);
    }
  }

  ++catInfo.frameCount;

  std::unordered_map<std::string, std::shared_ptr<arrow::Array>> collectionArrays;
  for (size_t i = 0; i < catInfo.collsToWrite.size(); ++i) {
    const auto& collName = catInfo.collsToWrite[i];
    const auto* coll = frame.getCollectionForWrite(collName);
    if (!coll) {
      throw std::runtime_error("Collection '" + collName + "' not found in frame of category " + std::string(category));
    }
    const auto& converter = catInfo.collConverters[i];
    auto array = converter(coll);
    if (!array) {
      throw std::runtime_error("Arrow converter returned a null array for collection '" + collName + "'");
    }
    if (array->length() != 1) {
      throw std::runtime_error("Arrow converter for collection '" + collName + "' returned " +
                               std::to_string(array->length()) + " rows, expected exactly one row per Frame");
    }
    collectionArrays[collName] = std::move(array);
  }

  m_bufferedFrames.push_back({std::string(category), std::move(collectionArrays)});
}

void ArrowWriter::finish() {
  if (m_finished) {
    return;
  }
  m_finished = true;

  if (m_bufferedFrames.empty()) {
    return;
  }

  // 1. Build unified schema
  std::vector<std::shared_ptr<arrow::Field>> topLevelFields;
  topLevelFields.push_back(arrow::field("__category__", arrow::utf8(), /*nullable=*/false));

  for (const auto& [catName, catInfo] : m_categories) {
    std::vector<std::shared_ptr<arrow::Field>> structFields;
    for (size_t i = 0; i < catInfo.collsToWrite.size(); ++i) {
      structFields.push_back(arrow::field(catInfo.collsToWrite[i], catInfo.collTypes[i]));
    }
    // Struct representing this category is nullable
    topLevelFields.push_back(arrow::field(std::string(catName), arrow::struct_(structFields), /*nullable=*/true));
  }
  auto wideSchema = arrow::schema(topLevelFields);

  // 2. Build Column Arrays

  // __category__ column
  arrow::StringBuilder catBuilder;
  for (const auto& bf : m_bufferedFrames) {
    auto status = catBuilder.Append(bf.category);
    if (!status.ok()) {
      throw std::runtime_error("Failed to append category to builder: " + status.ToString());
    }
  }
  std::shared_ptr<arrow::Array> catArray;
  auto status = catBuilder.Finish(&catArray);
  if (!status.ok()) {
    throw std::runtime_error("Failed to finish category builder: " + status.ToString());
  }

  std::vector<std::shared_ptr<arrow::Array>> topLevelArrays;
  topLevelArrays.push_back(catArray);

  // Build each category struct array in the exact order of the schema fields
  for (size_t fieldIdx = 1; fieldIdx < topLevelFields.size(); ++fieldIdx) {
    const auto& catName = topLevelFields[fieldIdx]->name();
    const auto& catInfo = m_categories.find(catName)->second;

    std::vector<std::shared_ptr<arrow::Array>> childArrays;
    for (size_t colIdx = 0; colIdx < catInfo.collsToWrite.size(); ++colIdx) {
      const auto& collName = catInfo.collsToWrite[colIdx];
      const auto& collType = catInfo.collTypes[colIdx];

      std::vector<std::shared_ptr<arrow::Array>> individualArrays;
      for (const auto& bf : m_bufferedFrames) {
        if (bf.category == catName) {
          const auto it = bf.collectionArrays.find(collName);
          if (it == bf.collectionArrays.end() || !it->second) {
            throw std::runtime_error("Collection '" + collName + "' not found in buffered frame of category " +
                                     catName);
          }
          individualArrays.push_back(it->second);
        } else {
          auto nullArray = arrow::MakeArrayOfNull(collType, 1).ValueOrDie();
          individualArrays.push_back(nullArray);
        }
      }
      auto concatenated = arrow::Concatenate(individualArrays).ValueOrDie();
      childArrays.push_back(concatenated);
    }

    // Build the null bitmap for the struct array
    arrow::TypedBufferBuilder<bool> bitmapBuilder;
    for (const auto& bf : m_bufferedFrames) {
      auto appendStatus = bitmapBuilder.Append(bf.category == catName);
      if (!appendStatus.ok()) {
        throw std::runtime_error("Failed to append to bitmap builder: " + appendStatus.ToString());
      }
    }
    std::shared_ptr<arrow::Buffer> nullBitmap;
    status = bitmapBuilder.Finish(&nullBitmap);
    if (!status.ok()) {
      throw std::runtime_error("Failed to finish null bitmap builder: " + status.ToString());
    }

    std::vector<std::shared_ptr<arrow::Field>> structFields;
    for (size_t i = 0; i < catInfo.collsToWrite.size(); ++i) {
      structFields.push_back(arrow::field(catInfo.collsToWrite[i], catInfo.collTypes[i]));
    }

    const auto nullCount = static_cast<int64_t>(m_bufferedFrames.size() - catInfo.frameCount);
    auto structArray = arrow::StructArray::Make(childArrays, structFields, nullBitmap, nullCount).ValueOrDie();
    topLevelArrays.push_back(structArray);
  }

  // 3. Make a single record batch and write it to file
  auto batch = arrow::RecordBatch::Make(wideSchema, static_cast<int64_t>(m_bufferedFrames.size()), topLevelArrays);
  const auto validateStatus = batch->ValidateFull();
  if (!validateStatus.ok()) {
    throw std::runtime_error("Failed to build a valid Arrow record batch: " + validateStatus.ToString());
  }

  auto outFileResult = arrow::io::FileOutputStream::Open(m_filename);
  if (!outFileResult.ok()) {
    throw std::runtime_error("Failed to open file output stream '" + m_filename +
                             "': " + outFileResult.status().ToString());
  }
  auto outFile = outFileResult.ValueOrDie();

  auto writerResult = arrow::ipc::MakeFileWriter(outFile, wideSchema);
  if (!writerResult.ok()) {
    throw std::runtime_error("Failed to create Arrow IPC file writer: " + writerResult.status().ToString());
  }
  auto writer = writerResult.ValueOrDie();

  auto writeStatus = writer->WriteRecordBatch(*batch);
  if (!writeStatus.ok()) {
    throw std::runtime_error("Failed to write Arrow record batch to IPC file: " + writeStatus.ToString());
  }

  auto closeStatus = writer->Close();
  if (!closeStatus.ok()) {
    throw std::runtime_error("Failed to close Arrow IPC writer: " + closeStatus.ToString());
  }
}

std::tuple<std::vector<std::string>, std::vector<std::string>>
ArrowWriter::checkConsistency(const std::vector<std::string>& collsToWrite, std::string_view category) const {
  const auto it = m_categories.find(category);
  if (it != m_categories.end()) {
    return getInconsistentColls(it->second.collsToWrite, collsToWrite);
  }

  return {std::vector<std::string>{}, collsToWrite};
}

ArrowWriter::CategoryInfo& ArrowWriter::getCategoryInfo(std::string_view category) {
  const auto it = m_categories.find(category);
  if (it != m_categories.end()) {
    return it->second;
  }

  const auto [newIt, _] = m_categories.emplace(category, CategoryInfo{});
  return newIt->second;
}

void ArrowWriter::initializeCategory(CategoryInfo& catInfo, const podio::Frame& frame, std::string_view category,
                                     const std::vector<std::string>& collsToWrite) {
  catInfo.collsToWrite = podio::utils::sortAlphabeticaly(collsToWrite);

  for (const auto& name : catInfo.collsToWrite) {
    const auto* coll = frame.getCollectionForWrite(name);
    if (!coll) {
      throw std::runtime_error("Collection '" + name + "' in category '" + std::string(category) +
                               "' is not available in Frame");
    }

    m_datamodelCollector.registerDatamodelDefinition(coll, name);

    const std::string typeName = std::string(coll->getValueTypeName());
    auto arrowType = podio::ArrowTypeRegistry::instance().getType(typeName);
    if (!arrowType) {
      throw std::runtime_error("Arrow type mapping not registered for collection: " + name + " of type " + typeName);
    }
    auto converter = podio::ArrowConverterRegistry::instance().getConverter(typeName);
    if (!converter) {
      throw std::runtime_error("Arrow converter callback not registered for collection: " + name +
                               " of type " + typeName);
    }
    catInfo.collTypes.push_back(std::move(arrowType));
    catInfo.collConverters.push_back(std::move(converter));
  }
}

} // namespace podio
