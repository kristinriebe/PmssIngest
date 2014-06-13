PMssIngest
===========

This code uses the DBIngestor library (https://github.com/adrpar/DBIngestor) to ingest 
particle data from PMss files into a database.

Hence it is very specific and probably only useful for a handful of people. 
But it can serve as an example for how to use the DBIngestor library and write 
your own binary reader.

The code is based on the HelloWord program delivered with the DBIngestor.

For any questions, please contact me at
Kristin Riebe, kriebe@aip.de


Data files
-----------
The PMss files were created from multiple Gadget files 
by Anatoly Klypin. I haven't checked yet if there are other 
types of PMss files with different formats.

The files were written with Fortran, option "unformatted" (thus binary) 
and with the default sequential access, so each record is wrapped by a 
leading and a trailing (4 byte) integer (I call them 'skipint' here).

Each file is structured as follows:

* header:
skipint
6 floats: aexpn, Om0, Oml0, hubble, Box, MassOne
skipint
skipint
4 ints, 1 float, 1 int: i_node,Nx,Ny,Nz,dBuffer,nBuffer
skipint
skipint
6 floats: xL,xR,yL,yR,zL,zR
skipint
skipint
1 int: Np
skipint

* data blocks:
There are blocks of "nrecord" size, each block is preceded by 
nrecord and integers around it, i.e.:

skipint
nrecord
skipint
skipint
datablock: nrecord rows with: 6 floats: x,y,z,vx,vy,vz; 1 long: id
skipint

The skipint-integers define the size of the next/previous record.

Each file contains a subbox of the cosmological simulation, with an overlap 
of dBuffer Mpc/h in each direction. These boundaries are xL, xR etc. as 
given in the header. 
The Pmss_Reader calculates the "true" boundary of the subbox, i.e. without 
the overlap, based on the file number. This mapping between a subbox and
its location in the cosmological box is taken from Anatoly Klypin's read-routine
for these files. 

The Pmss_Reader class checks for each read particle if it is inside the "true"
boundary. If not, this particle is skipped and the next row is read.

If particles lie *exactly* at the boundary between subboxes, they are included 
in each adjacent box. Thus one needs to check the table afterwards for duplicate values.


Features
----------
* Read given datafile and ingest following data into a database table:

particleId: id of a particles, read from file
x, y, z: coordinates of the particles, as written from file, in file units (probably Mpc/h)
vx,vy,vz: velocity components of the particle, in file units (probably km/s)
phkey: Peano-Hilbert key for the grid cell in which the particle is located, 
		is just filled with NULLs, values can be updated via the database 
        server using e.g. libhilbert (https://github.com/adrpar/libhilbert)
fileRowId: is constructed from number of file (read from file's header) and 
        the current row, useful for checking number of ingested particles 
        per file and for deleting all particles from a file, if they need to 
        be reingested


The data are mapped to following data types on the database: 
position and velocity: 4 byte floats
particleId: 8 byte integer
phkey: 4 byte integer

* Only particles from the main region are uploaded (not from overlapping boundary) 
to minimize duplicates and upload time

* If swap=1 is given, values will be byteswapped


Installation
--------------
see INSTALL


Example
--------
An example data file and create-table statement are given in the 
Example directory.
Data can be ingested with a command line like this:

PmssIngest/build/PmssIngest.x -s mysql -D TestDB -T Particles -U myusername -P mypassword -O 3306 -H 127.0.0.1 -d PMss.001_example.DAT

Replace myusername and mypassword with your own credentials. 
-s: type of database (e.g. mysql, unix_sqlsrv_odbc)
-D: database name
-T: table name
-O: port
-d: data file 

NOTE: Rather do not use -R 1. This would try to resume the connection, 
if something fails. But here it's probably better to stop then, check 
manually and restart from scratch. 

The database table could have been created like this 
(see also Example/dbtable-mysql.sql):

mysql> create table Particles (
    particleId  bigint not null,
    x  float not null,
    y  float not null,
    z  float not null,
    vx  float not null,
    vy  float not null,
    vz  float not null,
    phkey int,
    fileRowId bigint not null
) ENGINE=MyISAM;

An example data file is also given in the Example directory.



TODO
-----
- offsetting file by certain number of rows currently not working


