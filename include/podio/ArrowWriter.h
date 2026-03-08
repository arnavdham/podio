#ifndef PODIO_ARROWWRITER_H
#define PODIO_ARROWWRITER_H

#include <arrow/api.h>
#include "podio/Frame.h"
#include <string>

namespace podio {

class ArrowWriter {
public:
  ArrowWriter(const std::string& filename);
  ~ArrowWriter();

  arrow::Status writeFrame(const podio::Frame& frame);

private:
  std::string m_filename;
};

} // namespace podio

#endif