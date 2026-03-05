#ifndef PODIO_ARROWWRITER_H
#define PODIO_ARROWWRITER_H

#include <arrow/api.h>
#include <string>

namespace podio {

class ArrowWriter {
public:
  ArrowWriter(const std::string& filename);
  ~ArrowWriter();

  // A simple test function to prove Arrow is working
  void testArrowConnection();

private:
  std::string m_filename;
};

} // namespace podio

#endif