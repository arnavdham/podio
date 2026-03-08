#include "podio/ArrowWriter.h"
#include "podio/UserDataCollection.h"
#include <arrow/api.h> 
#include <iostream>
#include <vector>
#include <string>

namespace podio {

ArrowWriter::ArrowWriter(const std::string& filename) : m_filename(filename) {}
ArrowWriter::~ArrowWriter() {}

arrow::Status ArrowWriter::writeFrame(const podio::Frame& frame) {
  arrow::FloatBuilder x_builder;
  arrow::FloatBuilder y_builder;
  arrow::Int32Builder energy_builder;

  std::vector<std::string> collections = frame.getAvailableCollections();

  for (size_t i = 0; i < collections.size(); i++) {
    std::string collName = collections[i];
    const podio::CollectionBase* base_coll = frame.get(collName);
    if (base_coll == nullptr) continue;

    auto float_coll = dynamic_cast<const podio::UserDataCollection<float>*>(base_coll);
    if (float_coll != nullptr) {
      for (size_t row = 0; row < float_coll->size(); row++) {
        float value = float_coll->at(row);
        
        // Wrap Appends in the error checking macro
        if (collName == "X_Coordinates") ARROW_RETURN_NOT_OK(x_builder.Append(value));
        if (collName == "Y_Coordinates") ARROW_RETURN_NOT_OK(y_builder.Append(value));
      }
      continue;
    }

    auto int_coll = dynamic_cast<const podio::UserDataCollection<int>*>(base_coll);
    if (int_coll != nullptr) {
      for (size_t row = 0; row < int_coll->size(); row++) {
        int value = int_coll->at(row);
        
        // Wrap Appends in the error checking macro
        if (collName == "EnergyValues") ARROW_RETURN_NOT_OK(energy_builder.Append(value));
      }
    }
  }

  std::shared_ptr<arrow::Array> x_array;
  std::shared_ptr<arrow::Array> y_array;
  std::shared_ptr<arrow::Array> energy_array;

  // Wrap Finishes in the error checking macro
  ARROW_RETURN_NOT_OK(x_builder.Finish(&x_array));
  ARROW_RETURN_NOT_OK(y_builder.Finish(&y_array));
  ARROW_RETURN_NOT_OK(energy_builder.Finish(&energy_array));

  std::cout << "--- FINAL APACHE ARROW ARRAYS ---" << std::endl;
  std::cout << "Arrow X Array: " << x_array->ToString() << std::endl;
  std::cout << "Arrow Y Array: " << y_array->ToString() << std::endl;
  std::cout << "Arrow Energy Array: " << energy_array->ToString() << std::endl;

  return arrow::Status::OK();
}

} // namespace podio

// void ArrowWriter::writeFrame(const podio::Frame& frame) {
//   std::vector<std::string> collections = frame.getAvailableCollections();

//   for (size_t i = 0; i < collections.size(); i++) {
//     std::string collName = collections[i];
    
//     // 1. Get the generic collection
//     const podio::CollectionBase* base_coll = frame.get(collName);
//     if (base_coll == nullptr) continue;

//     std::cout << "--- Reading Collection: " << collName << " ---" << std::endl;

//     // 2. Attempt to unlock it as a collection of FLOATs (For X and Y coords)
//     auto float_coll = dynamic_cast<const podio::UserDataCollection<float>*>(base_coll);
//     if (float_coll != nullptr) {
//       for (size_t row = 0; row < float_coll->size(); row++) {
//         float value = float_coll->at(row);
//         std::cout << "  Row " << row << " -> " << value << std::endl;
//       }
//       continue; 
//     }

//     // 3. Attempt to unlock it as a collection of INTs (For Energy)
//     auto int_coll = dynamic_cast<const podio::UserDataCollection<int>*>(base_coll);
//     if (int_coll != nullptr) {
//       for (size_t row = 0; row < int_coll->size(); row++) {
//         int value = int_coll->at(row);
//         std::cout << "  Row " << row << " -> " << value << std::endl;
//       }
//     }
//   }
// }