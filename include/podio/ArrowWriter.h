#ifndef PODIO_ARROWWRITER_H
#define PODIO_ARROWWRITER_H

#include "podio/utilities/ArrowTypeRegistry.h"
#include "podio/utilities/ArrowConverterRegistry.h"
#include "podio/utilities/DatamodelRegistryIOHelpers.h"
#include "podio/utilities/StringKeyMap.h"

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "podio/Frame.h"

namespace arrow {
class DataType;
class Array;
}

namespace podio {

/// The ArrowWriter writes podio frames into Apache Arrow IPC files.
///
/// Each PODIO category is mapped to an Arrow table / record batch stream where
/// rows correspond to Frames and columns correspond to collections plus frame
/// metadata.
class ArrowWriter {
public:
  /// Create an ArrowWriter to write to a file.
  ///
  /// @note Existing files will be overwritten without warning once writing is
  /// implemented.
  ///
  /// @param filename The path to the file that will be created.
  ArrowWriter(const std::string& filename);

  /// ArrowWriter destructor
  ///
  /// This also takes care of writing all necessary metadata once writing is
  /// implemented.
  ~ArrowWriter();

  ArrowWriter(const ArrowWriter&) = delete;
  ArrowWriter& operator=(const ArrowWriter&) = delete;
  ArrowWriter(ArrowWriter&&) = default;
  ArrowWriter& operator=(ArrowWriter&&) = default;

  /// Store the given frame with the given category.
  ///
  /// This stores all available collections from the Frame.
  ///
  /// @param frame    The Frame to store
  /// @param category The category name under which this Frame should be stored
  void writeFrame(const podio::Frame& frame, std::string_view category);

  /// Store the given Frame with the given category.
  ///
  /// This stores only the desired collections and not the complete frame.
  ///
  /// @param frame        The Frame to store
  /// @param category     The category name under which this Frame should be
  ///                     stored
  /// @param collsToWrite The collection names that should be written
  void writeFrame(const podio::Frame& frame, std::string_view category, const std::vector<std::string>& collsToWrite);

  /// Finish writing the file.
  void finish();

  /// Check whether the collsToWrite are consistent with the state of the passed
  /// category.
  std::tuple<std::vector<std::string>, std::vector<std::string>>
  checkConsistency(const std::vector<std::string>& collsToWrite, std::string_view category) const;

private:
  struct CategoryInfo {
    std::vector<std::string> collsToWrite{};
    std::vector<std::shared_ptr<arrow::DataType>> collTypes{};
    std::vector<ArrowConverterRegistry::CreatorFunc> collConverters{};
    size_t frameCount{0};
  };

  struct BufferedFrame {
    std::string category;
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> collectionArrays;
  };

  CategoryInfo& getCategoryInfo(std::string_view category);
  void initializeCategory(CategoryInfo& catInfo, const podio::Frame& frame, std::string_view category,
                          const std::vector<std::string>& collsToWrite);

  std::string m_filename{};
  bool m_finished{false};
  podio::StringKeyMap<CategoryInfo> m_categories{};
  std::vector<BufferedFrame> m_bufferedFrames{};
  DatamodelDefinitionCollector m_datamodelCollector{};
};

} // namespace podio

#endif // PODIO_ARROWWRITER_H
