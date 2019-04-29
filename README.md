# Overview
The program uploads a given text file to the specified MongoDB instance and counts the number of occurrences for each word (case in-sensitive). Before being uploaded, the file is split into what is onwards called __chunk files__, each of which represent a part of the given file. 

Two different 'modes' of the program exists - should the path to a given text file be specified, it is fully processed and results, presented and the program is terminated. Should the file not be specified, the program just acts as a work node helping to process files specified by other instances.

## Command line arguments
* __-m, --mongo:__	Mongo database URI, default: _mongodb://localhost:27017_
* __-f, --file:__ Input file. If unspecified, runs as the _worker process_
* __-w, --workers:__ Number of processing threads, default: 4
* __-v, --view:__ The number of most/least words in the file to print, default: 10
* __--chunk-size__ The (MB) size of chunk files


## Terminology
* __File__: A text file serving as an input for the program
* __Chunk file__: A text file stored in the database containing part of the inputted file
* __Worker process__: An instance of the program running in the 'worker' mode (no specific file given to process)
* __Word__: A character sequence ending with space (' '), independent of capitals (i.e. WoRd == word)

## Database structure
All data is stored in a __wordcount__ database. The database contains following collections and fields:
* __file_register__: Contains file information
	* __checksum__: Checksum identifying the file
	* __fullyUploaded__: Number of full file uploads (0 or 1)
	* __finalised__: Number of times final results have been calculated (0 or 1)
* __metafiles__: Contains information about chunk files
	* __id__: _ObjectId_ of the file stored in the GridFS
	* __checksum__: Checksum identifying the file
	* __numOfLines__: Number of lines in the chunk file
	* __downloaded__: Number of times the chunk file has been downloaded to process
	* __processed__: Number of times the chunk file has been processed (and results saved)
* __partial_results__: Stores word counts for each chunk file
	* __\_fileId__: _ObjectId_ of the chunk file
	* __results__: Array of nested Documents
		* __word__: Word string
		* __value__: The count of given word for the chunk file
* __results__: Final results
	* __\_fileChecksum__: Checksum identifying the file
	* __results__: Array of nested Documents
		* __word__: Word string
		* __value__: The count of given word for the file

The chunk files themselves are stored using a __GridFS__ in a __wordfiles__ bucket with following metadata:
* __checksum__: Checksum identifying the file
* __numOfLines__: Number of lines in the chunk file

## Classes
__App__: Contains the top-level program flow and _main_ method. \
__DBConnection__: Represents a database client and provides methods to access database. \
__ProcessRunnable__: Runnable to process parts of the chunk files and produce results (word counts) \
__ChunkFile__: POJO containing _ChunkFileMeta_ and  _String_ content of a chunk file. \
__ChunkFileMeta__: POJO containing metadata of a chunk file. Mirrors the _metafiles_ collection document \
__FileUploadStage__: Enum indicating stages of upload of a file \
__Pair__: Generic pair of two objects \
__Util__: Contains static util methods \

# Checksum
A checksum is calculated before uploading any file. This is to ensure uniqueness of (input) files in stored in the database and also serves as a unique file identifier across the application, both of which means the application can simply get the already calculated results from the database/continue processing the file after the initial upload.

# File upload
### Concurrency
File upload is done in the main thread and no chunk file processing is done until the file is fully uploaded. This is done so as I did not have sufficient time to implement a separate thread version, which would allow for processing the chunk files while waiting for disk read (and further increasing the speed by removing a database round trip to download the file). 

On the other hand, should there be sufficient amount of worker processes, the gap between the efficiency of the two solutions will become negligible.

### File division
The chunk file split is based on given (max) chunk file size. This is to ensure relatively similar processing time for each chunk file. The default is 16MB, but the value is adjustable by the command line parameter ``--chunk-size``. Higher values remove some communication and processing overhead, but decreases the amount of parallelization.

The word preservation is guaranteed while dividing the files. 

# Processing
### Download of chunk files
Processing is done file by file (i.e. input file). When specified the input file, only the one file will be processed. When running in this mode, the processing is done in the main thread (in _App_), except for the line splitting itself done by processing threads running _ProcessRunnable_. When running as a worker process (without specific input file), downloading of new chunk files is done in a separate thread (_ChunkDownloadRunnable_) to keep the threads working on the next chunk file when reducing the results of the previous chunk file in the main thread. This processing is a step towards the approach described in File upload - Concurrency.

### Reducing the partial results
In either case, the downloaded chunk file is split into lines which are fed into the queue (_ProcessRunnable.linesToProcess_) to be processed by processing threads (_ProcessRunnable_). The results are stored in map (_ProcessRunnable.reduceMap_) and collected by the main thread, which then saves them into the database as partial results.

### Reducing the final results
Once all partial results for a given file are in the database, they are reduced into the final result by _App_ and again uploaded back to the database.

# Redundancy
The application is communicates only with the database server and is written in such a way it can recover from a possible communication/consistency failure. 
### File upload
* Should the failure occur before upload of the file starts, no changes are done to the database.
* If the failure occurs mid-upload (not all chunk files are uploaded or all are uploaded but the _fullyUploaded_ flag is not yet to 1+), on the next attempt to upload the file all the chunk files previously uploaded will be deleted.`
### Processing chunk files
* Should a chunk file be downloaded to the client which then fails, eventually another client will download and process the file 
* Should all the chunk files be processed (their _processed_ flag > 0), but the process fails to do the final results reduction and write to the database, another process will eventually finish the process
* Should the final results be written into the database but the process fails before _finalised_ flag is raised, another process will eventually raise the flag
### Manual editing the database
Manual editing of the database (or editing by other programs) can cause failures. Manual editing of _finalised_ flag is however safe to change as changing the flag enables testing of the program to continue with a predefined state of word counting for a given file.

# More possible improvements

### Better database structure
Having started working on this project I head near zero experience with NoSQL and Mongo, so the current database structure has been done in iterations of what was needed at the time and could use a good remake using some clever nested documents and indexes.

Currently the final results are not sorted when uploaded and the sorting is done for each viewing of the results by downloading all the records and sorting in the Java application. This is ineffective and the results should ideally be either sorted via MongoDB itself using the _$sort_ or sorted using the same mechanism on each download.

Additionally several racing conditions exist in several methods, which could be dealt with using transactions and/or nested documents. Example of this is checking whether a document with results for a given file exists in one step and uploading a new document if none exists.

### Callbacks
All the concurrency dependencies (threads waiting for each other) are currently handled by sleeping the threads and waiting for a given amount of time. While this is fine when waiting for new content in the database, the internal waits could be replaced with callbacks. This would make the code more complex, but would increase the effectiveness of the program

### Use bigger unit than a line 
The processing threads currently operate the split operation on a single line. Considering the effectiveness of the operation, that is possibly not the most effective solution. It would be beneficial to split the chunk files into bigger units in the processing queue (e.g. paragraphs, but there is no guarantee the input file has any), or even process the whole chunk file at once.

This change would need testing and measuring the effectiveness of differently size units (even size of the chunk files themselves). I had no time to do such testing and the results would be dependent on the used data (as there is also no minimum/maximum length of words/lines making this quite difficult).

Probably the best option seems to be to use a set amount of memory size (preventing word-splitting the same way as the line splitting works).

### Chunk file compressing
Currently the chunk files are being uploaded as a plain text. It should be possible to speed up file transfer by storing a compressed version, which would be (de)compressed by clients. This would also result in less storage needed by the database.

### More testing
While I have been doing a fair amount of manual testing, the project currently has no formal tests.  This is due to lack of time and ever changing structure. Anyway, more testing would definitely be useful.

### Smarter results clash resolving
Currently should the results be calculated multiple times (by multiple processes), the results after the first one are ignored (not saved). Maybe a check whether they are identical would be beneficial in case of some unexpected failures.


