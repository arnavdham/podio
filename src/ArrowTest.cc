#include "podio/ArrowWriter.h"
#include <iostream>

int main() {
    std::cout << "--- PODIO Apache Arrow Integration Test ---" << std::endl;
    
    // Create an instance of your new writer
    podio::ArrowWriter writer("dummy_physics_data.arrow");
    
    // Call the function that builds the Arrow array
    writer.testArrowConnection();
    
    std::cout << "-------------------------------------------" << std::endl;
    return 0;
}