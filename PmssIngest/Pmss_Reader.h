/*  
 *  Copyright (c) 2012, Adrian M. Partl <apartl@aip.de>, 
 *                      Kristin Riebe <kriebe@aip.de>,
 *                      eScience team AIP Potsdam
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership. You may obtain a copy
 *  of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <Reader.h>
#include <string>
#include <fstream>
#include <stdio.h>
#include <assert.h>

#ifndef Pmss_Pmss_Reader_h
#define Pmss_Pmss_Reader_h

using namespace DBReader;
using namespace DBDataSchema;
using namespace std;

namespace Pmss {
    
    class PmssReader : public Reader {
    private:
        std::string fileName;
        
        std::ifstream fileStream;
        
        int currRow;
        unsigned long numFieldPerRow;
        
        std::string buffer;
        std::string oneLine;
        
        std::string endLineFlag;

        //this is here for performance reasons. used to be in getItemInRow, but is allocated statically here to get rid
        //of many frees
        std::string tmpStr;
              
        string pmssString;
        int counter;
        int countInBlock;   // counter for particles in each data block

        int numBytesPerRow;	

        // items from file
        float x;
        float y;
        float z;
        float vx;
        float vy;
        float vz;
        long  id;
        
        //fields to be generated/converted/...
        float idfactor;
        int snapnum;
        long int particleId;
        int phkey;
        int nrecord;
        float Box;

        float xLeft, xRight, yLeft, yRight, zLeft, zRight;
        int fileNum, nx,ny,nz;

        int bswap;

        // to be passed on from main
        int startRow;   // at which row should we start ingesting
        int maxRows;    // max. number of rows to ingest


    public:
        PmssReader();
        PmssReader(std::string newFileName, int swap, int snapnum, float idfactor, int nrecord, int startRow, int maxRows);          
        ~PmssReader();

        void openFile(std::string newFileName);

        void closeFile();

        void readHeader();

        void setBoundary();
        
        void offsetFileStream();
        
        int getNextRow();
        
        int assignInt(int *n, char *memblock, int bswap);
        int assignLong(long int *n, char *memblock, int bswap);
        int assignFloat(float *n, char *memblock, int bswap);   
        
        bool getItemInRow(DBDataSchema::DataObjDesc * thisItem, bool applyAsserters, bool applyConverters, void* result);

        bool getDataItem(DBDataSchema::DataObjDesc * thisItem, void* result);

        void getConstItem(DBDataSchema::DataObjDesc * thisItem, void* result);
    };
    
}

#endif