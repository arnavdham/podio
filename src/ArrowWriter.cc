#include "podio/ArrowWriter.h"
#include "podio/CollectionBase.h"
#include "podio/Frame.h"
#include "podio/utilities/ArrowTypeRegistry.h"
#include "podio/utilities/MiscHelpers.h"

#include <arrow/api.h>

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
  // todo-task: Initialize Arrow output file stream
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

  // todo-task: Implement conversion of collections to Arrow arrays/record batches
  throw std::runtime_error("ArrowWriter category setup is available, Arrow array conversion yet to be implemented");
}

void ArrowWriter::finish() {
  if (m_finished) {
    return;
  }

  // todo-task: Write final schema with accumulated metadata, close/finalize files
  m_finished = true;
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

  std::vector<std::shared_ptr<arrow::Field>> schemaFields;
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
      throw std::runtime_error("Arrow type mapping not registered for collection: " + name +
                               " of type " + typeName);
    }
    schemaFields.push_back(arrow::field(name, std::move(arrowType)));
  }

  [[maybe_unused]] auto categorySchema = arrow::schema(std::move(schemaFields));
}

} // namespace podio
