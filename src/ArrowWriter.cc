#include "podio/ArrowWriter.h"
#include <iostream>

namespace podio {

ArrowWriter::ArrowWriter(const std::string& filename) : m_filename(filename) {}
ArrowWriter::~ArrowWriter() {}

void ArrowWriter::testArrowConnection() {
  // This uses Arrow's API to build a simple array!
  arrow::Int64Builder builder;
  builder.Append(100);
  builder.Append(200);
  
  std::shared_ptr<arrow::Array> array;
  builder.Finish(&array);
  
  std::cout << "SUCCESS! Arrow linked. Array length: " << array->length() << std::endl;
}

} // namespace podio