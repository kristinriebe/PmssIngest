/*  
 *  Copyright (c) 2016, Kristin Riebe <kriebe@aip.de>,
 *                      Adrian M. Partl <apartl@aip.de>,
 *                      E-Science team AIP Potsdam
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
    
    PmssReader::PmssReader(std::string newFileName, int newSwap, int newSnapnum, double newIdfactor, int newNrecord, int newStartRow, int newMaxRows) {          
             // this->box = box;     
        bswap = newSwap;
        snapnum = newSnapnum;
        idfactor = newIdfactor;
        nrecord = newNrecord;
        startRow = newStartRow;
        maxRows = newMaxRows;
        
        currRow = 0;
        counter = 0; // counts all particles
        countInBlock = 0; // counts particles in each data block
        
        numBytesPerRow = 6*sizeof(float)+1*sizeof(long);
       
        openFile(newFileName);
        readPmssHeader();
        setBoundary();
        //offsetFileStream();
        //exit(0);  // only enable, if checking header etc.
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


    /* Read header into one global structure at once, byteswap (if needed) */
    void PmssReader::readPmssHeader() {
        
        assert(fileStream.is_open());
        
        fileStream.read(reinterpret_cast<char *>(&header), sizeof(header));
        
        // byteswap header (if needed)
        header = swapPmssHeader(header, bswap);

        printf("aexpn, Omega0, OmegaL0, hubble, box, particleMass: %f %f %f %f %f %f\n", 
            header.aexpn, header.Omega0, header.OmegaL0, header.hubble, header.box, header.particleMass);
        printf("nodeNum,nx,ny,nz,dBuffer,nBuffer: %d %d %d %d %f %d\n", 
            header.nodeNum,header.nx,header.ny,header.nz,header.dBuffer,header.nBuffer);
        printf("xL,xR,yL,yR,zL,zR: %f %f %f %f %f %f\n", header.xL,header.xR,header.yL,header.yR,header.zL,header.zR);
        printf("Np: %d\n", header.np);

        // set some important variables which we'll still need later on
        box = header.box;
        nx = header.nx;
        ny = header.ny;
        nz = header.nz;
        fileNum = header.nodeNum;

    }
    
    /* Set the "true" boundary (without overlap) for the given fileNum */
    void PmssReader::setBoundary() {

        int i,j,k;
        //int ifile;
        float qx, qy, qz;
        float tolerance; // tolerance


        k = (fileNum-1)/(nx*ny)+1;
        j = (fileNum- (k-1)*nx*ny-1)/nx +1;
        i = fileNum- (k-1)*nx*ny-(j-1)*nx; 

        /*ifile= i +(j-1)*nx +(k-1)*nx*ny;  // must be equal to fileNum, otherwise problem!
        if (ifile != fileNum) {
            printf("Problem: ifile and fileNum do not match!\n");
            exit(0);
        }*/

        // corresponding left/right boundary:
        qx = box/nx;
        qy = box/ny;
        qz = box/nz;

        xLeft = (i-1)*qx;
        xRight = i*qx;
        yLeft = (j-1)*qy;
        yRight = j*qy;
        zLeft = (k-1)*qz;
        zRight = k*qz;
        
        printf("Boundary set to: x: %.2f - %.2f, y: %.2f - %.2f, z: %.2f - %.2f\n", 
            xLeft,xRight,yLeft,yRight,zLeft,zRight);

        // cross check, if this seems OK:
        tolerance = 0.001;
        if ( fabs(xLeft-header.dBuffer  - header.xL) > tolerance
            || fabs(xRight+header.dBuffer  - header.xR) > tolerance
            || fabs(yLeft-header.dBuffer  - header.yL) > tolerance
            || fabs(yRight+header.dBuffer  - header.yR) > tolerance
            || fabs(zLeft-header.dBuffer  - header.zL) > tolerance
            || fabs(zRight+header.dBuffer  - header.zR) > tolerance ) {

            printf("Problem: Mismatch between boundaries!\n");
            printf("One or more calculated boundary values vary more than dBuffer=%f [units] from boundary in file. Maybe check your header?\n", header.dBuffer);
            fflush(stdout);
        }
    }

    /* Offset to the desired row and start ingesting from there on.
     * NOT FULLY IMPLEMENTED YET (just use for testing) */
    void PmssReader::offsetFileStream() {
        streamoff ipos;
        int iblock;
        
        assert(fileStream.is_open());

        // position pointer at beginning of the row where ingestion should start
        numBytesPerRow = 6*sizeof(float)+1*sizeof(long);
       
        // skip 264 data blocks after header, assuming that nrecord is the same for all blocks:
        // TODO: Would need to jump from block to block, always reading nrecord beforehand
        // so variable lengths for data blocks can be supported.
        iblock = 264;
        nrecord = 500000;
        ipos = (streamoff) (   iblock * ( (long) nrecord * (long) numBytesPerRow + 5*sizeof(int) )  );

        fileStream.seekg(ipos, ios::cur);
        
        // update currow
        startRow = iblock*nrecord;
        currRow = startRow;

        cout<<"offset currRow: "<<currRow<<endl;
    }
    
    // read one line
    int PmssReader::getNextRow() {
        
        assert(fileStream.is_open());
        
        char memchunk[numBytesPerRow];
        int skipsize, iskip; 
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
                // -- skip (4)
                if (!fileStream.read(memchunk,skipsize)) {
                    printf("End of file reached.\n");
                    return false;
                }

                // -- nrecord
                fileStream.read(memchunk,datasize); // nrecord
                assignInt(&nrecord, &memchunk[0], bswap);
                printf("nrecord: %d\n", nrecord);
                if (nrecord <= 0) {
                    printf("Problem: nrecord is %d and not > 0\n", nrecord);
                    return false;
                }

                // -- skip (4)
                fileStream.read(memchunk,skipsize);
                assignInt(&iskip, &memchunk[0], bswap);
                // check if this integer is 4. If not, something went wrong
                // and it would be better to just stop here.
                if (iskip != 4) {
                    printf("Error: trailing integer after nrecord is not 4, but %d. Exit.\n",
                        iskip);
                    return false;
                }

                // also need to skip integer that starts 
                // the next data block 
                fileStream.read(memchunk,skipsize);
                assignInt(&iskip, &memchunk[0], bswap);
                // include one more check here:
                if (iskip != (nrecord*numBytesPerRow)) {
                    printf("Error: block size (%d) does not agree with nrecord*numBytesPerRow (%d). Exit.\n",
                        iskip, nrecord*numBytesPerRow);
                    return false;
                }

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
                return false;
            }

            // now parse the line and assign it to the proper variables
            assignFloat(&x, &memchunk[0], bswap);
            assignFloat(&y, &memchunk[sizeof(float)], bswap);
            assignFloat(&z, &memchunk[2*sizeof(float)], bswap);
            assignFloat(&vx, &memchunk[3*sizeof(float)], bswap);
            assignFloat(&vy, &memchunk[4*sizeof(float)], bswap);
            assignFloat(&vz, &memchunk[5*sizeof(float)], bswap);
            assignLong(&id, &memchunk[6*sizeof(float)], bswap);
            

            // Create another id from number of file and row.
            // This helps to check ingestions and remove particles from the
            // database that were ingested from the same file, if something
            // went wrong during ingestion process (e.g. connection was lost).
            // 
            fileRowId = (long int) (fileNum * idfactor + currRow);

            if (counter % 100000 == 0) 
                printf("   check: counter, fileRowId, id, x,y,z, vx,vy,vz: %d, %ld %ld, %f %f %f, %f %f %f\n", 
                    counter, fileRowId, id, x,y,z, vx,vy,vz);

            currRow++;
            counter++;
            countInBlock++;


            // Check, if values are inside of boundary.
            // If not: skip this line, i.e. read the next line, until a valid
            // line was foúnd.

            // include "=="" on both sides, since we don't want to miss particles at box boundary
            // rather have duplicates and correct for them later on than have missing particles. 

            if (x >= xLeft && x < xRight 
             && y >= yLeft && y < yRight
             && z >= zLeft && z < zRight) {
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
                return false;
	       }

	    }
	
        return true;       
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
        
        // assertions and conversions could be applied here
        // but do not need them now.

        // return value: if true: just NULL is written, result is ignored.
        // if false: the value in result is used.
        return isNull;
    }
    
    bool PmssReader::getDataItem(DBDataSchema::DataObjDesc * thisItem, void* result) {

        //check if this is "Col1" etc. and if yes, assign corresponding value
        //the variables are declared already in Pmss_Reader.h 
        //and the values were read in getNextRow()
        bool isNull;

        //printf("   counter: fileRowId, id, x,y,z, vx,vy,vz: %d: %ld %ld %f %f %f, %f %f %f\n", 
        //            counter, fileRowId, id, x,y,z, vx,vy,vz);

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
            //particleId = (long int) ( (snapnum)*idfactor + id );
            *(long*)(result) = id;
        } else if (thisItem->getDataObjName().compare("Col8") == 0) {
            phkey = 0;
            *(int*)(result) = phkey;
            // better: let DBIngestor insert Null at this column
            // => need to return 1, so that Null will be written.
            isNull = true;
        } else if (thisItem->getDataObjName().compare("Col9") == 0) {
            *(long*)(result) = fileRowId;
        } else {
            printf("Something went wrong...\n");
            exit(EXIT_FAILURE);
        }

        return isNull;

    }

    void PmssReader::getConstItem(DBDataSchema::DataObjDesc * thisItem, void* result) {
        memcpy(result, thisItem->getConstData(), DBDataSchema::getByteLenOfDType(thisItem->getDataObjDType()));
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

        return 1;
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

        return 1;
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
        return 1;
    }

    // swap each header item
    pmssHeader PmssReader::swapPmssHeader(pmssHeader header, int swap) {

        if (swap) {
            header.ilead1 = swapInt(header.ilead1,swap);
            header.aexpn = swapFloat(header.aexpn, swap);
            header.Omega0 = swapFloat(header.Omega0, swap);
            header.OmegaL0 = swapFloat(header.OmegaL0, swap);
            header.hubble = swapFloat(header.hubble, swap);
            header.box = swapFloat(header.box, swap);
            header.particleMass = swapFloat(header.particleMass, swap);
            header.itrail1 = swapInt(header.itrail1,swap);

            header.ilead2 = swapInt(header.ilead2,swap);
            header.nodeNum = swapInt(header.nodeNum,swap);
            header.nx = swapInt(header.nx,swap);
            header.ny = swapInt(header.ny,swap);
            header.nz = swapInt(header.nz,swap);
            header.dBuffer = swapFloat(header.dBuffer,swap);
            header.nBuffer = swapInt(header.nBuffer,swap);
            header.itrail2 = swapInt(header.itrail2,swap);
            
            header.ilead3 = swapInt(header.ilead3,swap);
            header.xL = swapFloat(header.xL,swap);
            header.xR = swapFloat(header.xR,swap);
            header.yL = swapFloat(header.yL,swap);
            header.yR = swapFloat(header.yR,swap);
            header.zL = swapFloat(header.zL,swap);
            header.zR = swapFloat(header.zR,swap);
            header.itrail3 = swapInt(header.itrail3,swap);

            header.ilead4 = swapInt(header.ilead4,swap);
            header.np = swapInt(header.np,swap);
            header.itrail4 = swapInt(header.itrail4,swap);
        }

        return header;
    }

    int PmssReader::swapInt(int i, int bswap) {
        unsigned char *cptr,tmp;
        
        if ( (int)sizeof(int) != 4 ) {
            fprintf( stderr,"Swap int: sizeof(int)=%d and not 4\n", (int)sizeof(int) );
            exit(0);
        }
        
        if (bswap) {
            cptr = (unsigned char *) &i;
            tmp     = cptr[0];
            cptr[0] = cptr[3];
            cptr[3] = tmp;
            tmp     = cptr[1];
            cptr[1] = cptr[2];
            cptr[2] = tmp;
        }

        return i;
    }

    float PmssReader::swapFloat(float f, int bswap) {
        unsigned char *cptr,tmp;
        
        if (sizeof(float) != 4) {
        fprintf(stderr,"Swap float: sizeof(float)=%d and not 4\n",(int)sizeof(float));
        exit(0);
        }
         
        if (bswap) {
            cptr = (unsigned char *)&f;
            tmp     = cptr[0];
            cptr[0] = cptr[3];
            cptr[3] = tmp;
            tmp     = cptr[1];
            cptr[1] = cptr[2];
            cptr[2] = tmp;
        }

        return f;
    }

}
