/*  
 *  Copyright (c) 2012, Adrian M. Partl <apartl@aip.de>, 
 *			Kristin Riebe <kriebe@aip.de>,
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

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>	// sqrt, pow
#include "pmssingest_error.h"

#include "Pmss_Reader.h"

namespace Pmss {
    PmssReader::PmssReader() {
        //counter = 0;
        //currRow = -1;
    }
    
    PmssReader::PmssReader(std::string newFileName, int newSwap, int newSnapnum, float newIdfactor, int newNrecord, int newStartRow, int newMaxRows) {          
        // this->box = box;     
        bswap = newSwap;
        snapnum = newSnapnum;
        idfactor = newIdfactor;
        nrecord = newNrecord;
        startRow = newStartRow;
        maxRows = newMaxRows;
        
        currRow = 0;
        counter = 0; // count number of particles
        countInBlock = 0; // counting particles in each data block
        
        numBytesPerRow = 6*sizeof(float)+1*sizeof(long);
       
        openFile(newFileName);
        readHeader();
        setBoundary();
        //offsetFileStream();
        //exit(0);
    }
    
    
    PmssReader::~PmssReader() {
        closeFile();
    }
    
    void PmssReader::openFile(string newFileName) {
        if (fileStream.is_open())
            fileStream.close();

        // open binary file
        fileStream.open(newFileName.c_str(), ios::in | ios::binary);
        
        if (!(fileStream.is_open())) {
            PmssIngest_error("PmssReader: Error in opening file.\n");
        }
        
        fileName = newFileName;
    }
    
    void PmssReader::closeFile() {
        if (fileStream.is_open())
            fileStream.close();
    }

    void PmssReader::readHeader() {

        char memchunk[100];
        int skipsize, iskip, datasize;
        float aexpn, mass, Om0;
        float xL,xR, yL, yR, zL, zR;
        int np, nrecord;
        streamoff ipos;

        printf("Read header ...\n");
        // TODO: read it first completely, then slice into 
        // individual pieces?

        skipsize = 4;
        fileStream.read(memchunk,skipsize);
        datasize = 4;
        fileStream.read(memchunk,datasize);
        assignFloat(&aexpn, &memchunk[0], bswap);
        printf("aexpn: %f\n", aexpn);

        fileStream.read(memchunk,datasize);
        assignFloat(&Om0, &memchunk[0], bswap);
        printf("Om0: %f\n", Om0);
        fileStream.read(memchunk,datasize);//Oml
        fileStream.read(memchunk,datasize);//Hubble
        fileStream.read(memchunk,datasize);//Box
        assignFloat(&Box, &memchunk[0], bswap);
        printf("Box: %f\n", Box);
        fileStream.read(memchunk,datasize);//mass
        assignFloat(&mass, &memchunk[0], bswap);
        printf("Mass: %f\n", mass);

        fileStream.read(memchunk,skipsize);
        fileStream.read(memchunk,skipsize);

        datasize=4;
        fileStream.read(memchunk,datasize); //i_node
        assignInt(&fileNum, &memchunk[0], bswap);
        fileStream.read(memchunk,datasize); // nx
        assignInt(&nx, &memchunk[0], bswap);
        fileStream.read(memchunk,datasize); // ny
        assignInt(&ny, &memchunk[0], bswap);
        fileStream.read(memchunk,datasize); // nz
        assignInt(&nz, &memchunk[0], bswap);        
        fileStream.read(memchunk,datasize); // dBuffer
        fileStream.read(memchunk,datasize); // nBuffer
       
        printf("fileNum, nx, ny, nz: %d %d %d %d\n", fileNum, nx, ny, nz);

        fileStream.read(memchunk,skipsize);
        fileStream.read(memchunk,skipsize);

        datasize=4;
        fileStream.read(memchunk,datasize); // xL
        assignFloat(&xL, &memchunk[0], bswap);
        fileStream.read(memchunk,datasize); // xR
        assignFloat(&xR, &memchunk[0], bswap);
        fileStream.read(memchunk,datasize); // yL
        assignFloat(&yL, &memchunk[0], bswap);
        fileStream.read(memchunk,datasize); // yR
        assignFloat(&yR, &memchunk[0], bswap);
        fileStream.read(memchunk,datasize); // zL
        assignFloat(&zL, &memchunk[0], bswap);
        fileStream.read(memchunk,datasize); // zR
        assignFloat(&zR, &memchunk[0], bswap);

        printf("range: xL,xR,yL,yR,zL,zR: %.2f %.2f %.2f %.2f %.2f %.2f\n", xL,xR,yL,yR,zL,zR);

        fileStream.read(memchunk,skipsize);
        fileStream.read(memchunk,skipsize);

        fileStream.read(memchunk,datasize); // np
        assignInt(&np, &memchunk[0], bswap);
       
        fileStream.read(memchunk,skipsize);
        fileStream.read(memchunk,skipsize);

        fileStream.read(memchunk,datasize); // nrecord
        assignInt(&nrecord, &memchunk[0], bswap);

        printf("np, nrecord: %d %d\n", np, nrecord);

        fileStream.read(memchunk,skipsize);
        
        // now the "real data" would follow after another beginning skip

        // go back before nrecord
        // so that in getNextRow I can start with nrecord properly
       
        ipos = (streamoff) ( (long) sizeof(nrecord)+2*sizeof(skipsize) );
        fileStream.seekg(-ipos, ios::cur);

        //if (!fileStream.read(memchunk,numBytesPerRow)) {
        //    return 0;
        //}

        printf("Header done.\n");

    }
    
    /* Set the "true" boundary (without overlap) for the given fileNum */
    void PmssReader::setBoundary() {
        int i,j,k, ifile, ii;
        float qx, qy, qz;

        k = (fileNum-1)/(nx*ny)+1;
        j = (fileNum- (k-1)*nx*ny-1)/nx +1;
        i = fileNum- (k-1)*nx*ny-(j-1)*nx; 

        ifile= i +(j-1)*nx +(k-1)*nx*ny;  // must be equal to fileNum, otherwise problem!

        // corresponding left/right boundary:
        qx = Box/nx;
        qy = Box/ny;
        qz = Box/nz;

        xLeft = (i-1)*qx;
        xRight = i*qx;
        yLeft = (j-1)*qy;
        yRight = j*qy;
        zLeft = (k-1)*qz;
        zRight = k*qz;
        
        printf("Boundary set to: x: %.2f - %.2f, y: %.2f - %.2f, z: %.2f - %.2f\n", 
            xLeft,xRight,yLeft,yRight,zLeft,zRight);
    }


    void PmssReader::offsetFileStream() {
        int irow;
        streamoff ipos;
        int iblock;
        
        assert(fileStream.is_open());

        // position pointer at beginning of the row where ingestion should start
        numBytesPerRow = 6*sizeof(float)+1*sizeof(long);
        //ipos = (streamoff) ( (long) startRow * (long) numBytesPerRow );
        
        // skip 264 data blocks after header:
        iblock = 264;
        ipos = (streamoff) (   iblock * ( (long) nrecord * (long) numBytesPerRow + 5*sizeof(int) )  );

        fileStream.seekg(ipos, ios::cur);
        
        // update currow
        startRow = iblock*nrecord;
        currRow = startRow;
 
        cout<<"offset currRow: "<<currRow<<endl;
    }
    
    int PmssReader::getNextRow() {
        // read one line
        assert(fileStream.is_open());
        
        char memchunk[numBytesPerRow];
        int skipsize; 
        int datasize;
        int particleInside;
        
        //cout<<"reader: currRow: "<<currRow<<endl;
        skipsize = 4;
        datasize = 4;

        particleInside = 0;

        // read values until a particle inside the boundaries is found
        while (!particleInside) {

            if (countInBlock == nrecord || counter == 0) {
                // end of data block/start of new one is reached!

                // need to skip the integer that ends the previous block,
                // if not very first block
                if (counter > 0) {
                    printf("Reached end of data block.\n");
                    fileStream.read(memchunk,skipsize);
                }
                printf("Skipping nrecord-header for next data block.\n");

                // skip+read block with "nrecord"
                if (!fileStream.read(memchunk,skipsize)) {
                    printf("End of file reached.\n");
                    return 0;
                }

                fileStream.read(memchunk,datasize); // nrecord
                assignInt(&nrecord, &memchunk[0], bswap);

                printf("nrecord: %d\n", nrecord);

                fileStream.read(memchunk,skipsize);

                // also need to skip integer that starts 
                // the next data block 
                fileStream.read(memchunk,skipsize);

                // reset countInBlock:
                countInBlock = 0;

            }

            /*if (counter > 1000000) {
                exit(0);
            }
            */
            
            // read the line (one particle) at once
            // TODO: Could also read all nrecord-lines, i.e. the whole datablock
            // at once, then always use the next value in array
            // or, if at end of array, read the next data block.
            if (!fileStream.read(memchunk,numBytesPerRow)) {
                return 0;
            }

            // now parse the line and assign it to the proper variables using memcpy
            assignFloat(&x, &memchunk[0], bswap);
            assignFloat(&y, &memchunk[sizeof(float)], bswap);
            assignFloat(&z, &memchunk[2*sizeof(float)], bswap);
            assignFloat(&vx, &memchunk[3*sizeof(float)], bswap);
            assignFloat(&vy, &memchunk[4*sizeof(float)], bswap);
            assignFloat(&vz, &memchunk[5*sizeof(float)], bswap);
            assignLong(&id, &memchunk[6*sizeof(float)], bswap);
            
            if (counter % 100000 == 0) 
                printf("counter, x,y,z, vx,vy,vz: %d %f %f %f, %f %f %f\n", counter, x,y,z, vx,vy,vz);

            currRow++;
            counter++;
            countInBlock++;


            // Check, if values are inside of boundary.
            // If not: skip this line, i.e. read the next line, until a valid
            // line was foúnd.

            // include "=="" on both sides, since we don't want to miss particles at box boundary
            // rather have duplicates and correct for them later on that missing particles. 

            if (x >= xLeft && x <= xRight 
             && y >= yLeft && y <= yRight
             && z >= zLeft && z <= zRight) {
                // fine
                particleInside = 1;
            } else {
                 // read again
                particleInside = 0;
            }
        }


	    // stop after reading maxRows, but only if it is not -1
	    // Note: Could this be accelerated? It's unnecessary most of the time,
	    // but now it is evaluated for each row ...
        // (But doesn't affect performance that much. Bottle-neck is rather
        // sending of data to database server)
        if (maxRows != -1) {
            if (counter > maxRows) {
                printf("Maximum number of rows to be ingested is reached (%d).\n", maxRows);
                return 0;
	       }

	    }
	
        return 1;       
    }

    // write part from memoryblock to integer; byteswap, if necessary (TODO: use global 'swap' or locally submit?)
    int PmssReader::assignInt(int *n, char *memblock, int bswap) {
        
        unsigned char *cptr,tmp;

        if (sizeof(int) != 4) {
            fprintf(stderr,"assignInt: sizeof(int)=%ld and not 4. Can't handle that.\n",sizeof(int));
            return 0;
        }

        if (!memcpy(n,  memblock, sizeof(int))) {
            fprintf(stderr,"Error: Encountered end of memory block or other trouble when reading the memory block.\n");
            return 0;
        }

        if (bswap) {
            cptr = (unsigned char *) n;
            tmp     = cptr[0];
            cptr[0] = cptr[3];    
            cptr[3] = tmp;
            tmp     = cptr[1];
            cptr[1] = cptr[2];
            cptr[2] = tmp;
        }
    }

    
    // write part from memoryblock to long; byteswap, if necessary
    int PmssReader::assignLong(long *n, char *memblock, int bswap) {
        
        unsigned char *cptr,tmp;

        if (sizeof(long) != 8) {
            fprintf(stderr,"assignLong: sizeof(long)=%ld and not 8. Can't handle that.\n",sizeof(long));
            return 0;
        }

        if (!memcpy(n,  memblock, sizeof(long))) {
            fprintf(stderr,"Error: Encountered end of memory block or other trouble when reading the memory block.\n");
            return 0;
        }

        if (bswap) {
            cptr = (unsigned char *) n;
            tmp     = cptr[0];
            cptr[0] = cptr[7];
            cptr[7] = tmp;
            tmp     = cptr[1];
            cptr[1] = cptr[6];
            cptr[6] = tmp;
            tmp     = cptr[2];
            cptr[2] = cptr[5];
            cptr[5] = tmp;
            tmp     = cptr[3];
            cptr[3] = cptr[4];
            cptr[4] = tmp;
        }

    }

    // write part from memoryblock to float; byteswap, if necessary
    int PmssReader::assignFloat(float *n, char *memblock, int bswap) {
        
        unsigned char *cptr,tmp;
        
        if (sizeof(float) != 4) {
            fprintf(stderr,"assignFloat: sizeof(float)=%ld and not 4. Can't handle that.\n",sizeof(float));
            return 0;
        }
        
        if (!memcpy(n, memblock, sizeof(float))) {
            printf("Error: Encountered end of memory block or other trouble when reading the memory block.\n");
            return 0;
        }
        
        if (bswap) {
            cptr = (unsigned char *) n;
            tmp     = cptr[0];
            cptr[0] = cptr[3];    
            cptr[3] = tmp;
            tmp     = cptr[1];
            cptr[1] = cptr[2];
            cptr[2] = tmp;
        }
    }

    
    bool PmssReader::getItemInRow(DBDataSchema::DataObjDesc * thisItem, bool applyAsserters, bool applyConverters, void* result) {
        
        bool isNull;

        isNull = false;

        //reroute constant items:
        if(thisItem->getIsConstItem() == true) {
            getConstItem(thisItem, result);
            isNull = false;
        } else if (thisItem->getIsHeaderItem() == true) {
            printf("We never told you to read headers...\n");
            exit(EXIT_FAILURE);
        } else {
            isNull = getDataItem(thisItem, result);
        }
        
        // following things are not necessary here:

        //check assertions
        //checkAssertions(thisItem, result);
        
        //apply conversion
        //applyConversions(thisItem, result);

        // return value: if True: just NULL is written, result is ignored.
        //if False: the value in result is used.
        return isNull;
    }
    
    bool PmssReader::getDataItem(DBDataSchema::DataObjDesc * thisItem, void* result) {

        //check if this is "Col1" etc. and if yes, assign corresponding value
        //the variables are declared already in Pmss_Reader.h 
        //and the values were read in getNextRow()
        bool isNull;

        isNull = false;
        if(thisItem->getDataObjName().compare("Col1") == 0) {           
            *(float*)(result) = x;
        } else if (thisItem->getDataObjName().compare("Col2") == 0) {
            *(float*)(result) = y;
        } else if (thisItem->getDataObjName().compare("Col3") == 0) {
            *(float*)(result) = z;
        } else if (thisItem->getDataObjName().compare("Col4") == 0) {
            *(float*)(result) = vx;
        } else if (thisItem->getDataObjName().compare("Col5") == 0) {
            *(float*)(result) = vy;
        } else if (thisItem->getDataObjName().compare("Col6") == 0) {
            *(float*)(result) = vz;
        } else if (thisItem->getDataObjName().compare("Col7") == 0) {
            particleId = (long int) ( (snapnum)*idfactor + id );
            *(long*)(result) = id;
        } else if (thisItem->getDataObjName().compare("Col8") == 0) {
            phkey = 0;
            *(int*)(result) = phkey;
            // better: let DBIngestor insert Null at this column
            // => need to return 1, so that Null will be written.
            isNull = true;
        } else {
            printf("Something went wrong...\n");
            exit(EXIT_FAILURE);
        }

        return isNull;

    }

    void PmssReader::getConstItem(DBDataSchema::DataObjDesc * thisItem, void* result) {
        memcpy(result, thisItem->getConstData(), DBDataSchema::getByteLenOfDType(thisItem->getDataObjDType()));
    }
}
