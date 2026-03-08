#include "podio/ArrowWriter.h"
#include "podio/Frame.h"
#include "podio/UserDataCollection.h"
#include <iostream>

int main() {
    std::cout << "--- PODIO Apache Arrow Integration Test ---" << std::endl;
    
    // 1. Create a blank PODIO Frame
    podio::Frame eventFrame;
    
    //Create 3 separate collections
    auto colX = std::make_unique<podio::UserDataCollection<float>>();
    auto colY = std::make_unique<podio::UserDataCollection<float>>();
    auto colEnergy = std::make_unique<podio::UserDataCollection<int>>();
    
    //Add 3 "rows" of data to each column
    // Row 1
    colX->push_back(42.5f);
    colY->push_back(18.2f);
    colEnergy->push_back(100);
    
    // Row 2
    colX->push_back(1.1f);
    colY->push_back(2.5f);
    colEnergy->push_back(999);
    
    // Row 3
    colX->push_back(7.9f);
    colY->push_back(2.1f);
    colEnergy->push_back(450);
    
    //Lock all 3 columns into the Frame
    eventFrame.put(std::move(colX), "X_Coordinates");
    eventFrame.put(std::move(colY), "Y_Coordinates");
    eventFrame.put(std::move(colEnergy), "EnergyValues");
    
    //Create your Arrow Writer and pass it the Frame
    podio::ArrowWriter writer("dummy_physics_data.arrow");

    // Catch the status and check if it failed
    arrow::Status status = writer.writeFrame(eventFrame);
    if (!status.ok()) {
        std::cerr << "CRITICAL ARROW ERROR: " << status.ToString() << std::endl;
        return 1; 
    }
    
    std::cout << "--- Test Finished ---" << std::endl;
    return 0;
}