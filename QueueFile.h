#include<cinttypes>
#include<string>

/**
 * -----------------------------------------------------------------------------------
 * | This is a one-to-one translation of the Tape library from Square to C++.        |
 * |                                                                                 |
 * | For completeness, Java-specific comments and copyright notices have been kept.  |
 * |                                                                                 |
 * | The original project in Java is available at https://github.com/square/tape     |
 * | Author: Paulo Costa                                                             |
 * -----------------------------------------------------------------------------------
 */


/*
 * Copyright (C) 2010 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <cstdint>

namespace com { namespace squareup { namespace tape2 {


class IOException {
public:
    const int num;
    const char* message;
    
    IOException(const char* message);
    IOException(int num);
};


/** A pointer to an element. */
class Element {
public:
    /** Position in file. */
    int64_t position;

    /** The length of the data. */
    int32_t length;

    /**
    * Constructs a new element.
    *
    * @param position within file
    * @param length of data
    */
    Element(int64_t position, int32_t length);

    std::string toString();
};

/**
 * A reliable, efficient, file-based, FIFO queue. Additions and removals are O(1). All operations
 * are atomic. Writes are synchronous; data will be written to disk before an operation returns.
 * The underlying file is structured to survive process and even system crashes. If an I/O
 * exception is thrown during a mutating change, the change is aborted. It is safe to continue to
 * use a {@code QueueFile} instance after an exception.
 *
 * <p><strong>Note that this implementation is not synchronized.</strong>
 *
 * <p>In a traditional queue, the remove operation returns an element. In this queue,
 * {@link #peek} and {@link #remove} are used in conjunction. Use
 * {@code peek} to retrieve the first element, and then {@code remove} to remove it after
 * successful processing. If the system crashes after {@code peek} and during processing, the
 * element will remain in the queue, to be processed when the system restarts.
 *
 * <p><strong>NOTE:</strong> The current implementation is built for file systems that support
 * atomic segment writes (like YAFFS). Most conventional file systems don't support this; if the
 * power goes out while writing a segment, the segment will contain garbage and the file will be
 * corrupt. We'll add journaling support so this class can be used with more file systems later.
 *
 * @author Bob Lee (bob@squareup.com)
 */
class QueueFile {
private:
    /**
    * The underlying file. Uses a ring buffer to store entries. Designed so that a modification
    * isn't committed or visible until we write the header. The header is much smaller than a
    * segment. So long as the underlying file system supports atomic segment writes, changes to the
    * queue are atomic. Storing the file length ensures we can recover from a failed expansion
    * (i.e. if setting the file length succeeds but the process dies before the data can be copied).
    * <p>
    * This implementation supports two versions of the on-disk format.
    * <pre>
    * Format:
    *   16-32 bytes      Header
    *   ...              Data
    *
    * Header (32 bytes):
    *   1 bit            Versioned indicator [0 = legacy (see "Legacy Header"), 1 = versioned]
    *   31 bits          Version, always 1
    *   8 bytes          File length
    *   4 bytes          Element count
    *   8 bytes          Head element position
    *   8 bytes          Tail element position
    *
    * Legacy Header (16 bytes):
    *   1 bit            Legacy indicator, always 0 (see "Header")
    *   31 bits          File length
    *   4 bytes          Element count
    *   4 bytes          Head element position
    *   4 bytes          Tail element position
    *
    * Element:
    *   4 bytes          Data length
    *   ...              Data
    * </pre>
    */
    int fd;

    /** True when using the versioned header format. Otherwise use the legacy format. */
    bool versioned;

    /** When true, removing an element will also overwrite data with zero bytes. */
    bool zero;

    /** When true, the file has been closed and this queue can no longer be used. */
    bool closed;
    
    /**
    * The number of times this file has been structurally modified — it is incremented during
    * {@link #remove(int)} and {@link #add(byte[], int, int)}. Used by {@link ElementIterator}
    * to guard against concurrent modification.
    */
    int32_t modCount;
    
    /** The header length in bytes: 16 or 32. */
    int32_t headerLength;

    /** Cached file length. Always a power of 2. */
    int64_t fileLength;

    /** Number of elements. */
    int32_t elementCount;

    /** Pointer to first (or eldest) element. */
    Element first;

    /** Pointer to last (or newest) element. */
    Element last;
    

public:
    /**
    * Constructs a new queue backed by the given file. Only one instance should access a given file
    * at a time.
    *
    * @param zero When true, removing an element will also overwrite data with zero bytes.
    * @param forceLegacy When true, only the legacy (Tape 1.x) format will be used.
    */
    QueueFile(const char* file, bool zero=true, bool forceLegacy=false) throw (IOException);
    QueueFile(int fd, bool zero=true, bool forceLegacy=false) throw (IOException);

    /** Returns the number of elements in this queue. */
    int32_t size();

    /** Returns true if this queue contains no entries. */
    bool isEmpty();
    
    /** Number of bytes used on the file */
    int64_t usedBytes();

    /** Number of bytes free on the file */
    int64_t remainingBytes();
   
    /**
    * Adds an element to the end of the queue.
    *
    * @param data to copy bytes from
    * @param count number of bytes to copy
    */
    void add(const uint8_t* data, int32_t count) throw (IOException);
   
    /**
    * Removes the eldest {@code n} elements.
    *
    * @throws NoSuchElementException if the queue is empty
    */
    void remove(int32_t n=1) throw (IOException);
    
    /** 
     * Reads the eldest element. Returns null if the queue is empty. 
     * You have to free the buffer after using it.
     */
    void peek(uint8_t* &buffer, int32_t &length) throw (IOException);

    /** 
     * Clears this queue. Truncates the file to the initial size. 
     */
    void clear() throw (IOException);
    
    void close() throw (IOException);

    std::string toString();
    
    /**
    * Returns an iterator over elements in this QueueFile.
    *
    * <p>The iterator disallows modifications to be made to the QueueFile during iteration. Removing
    * elements from the head of the QueueFile is permitted during iteration using
    * {@link Iterator#remove()}.
    *
    * <p>The iterator may throw an unchecked {@link RuntimeException} during {@link Iterator#next()}
    * or {@link Iterator#remove()}.
    */
    //TODO Iterator begin();
    //TODO Iterator end();
    
private:
    /**
    * Writes header atomically. The arguments contain the updated values. The class member fields
    * should not have changed yet. This only updates the state in the file. It's up to the caller to
    * update the class member variables *after* this call succeeds. Assumes segment writes are
    * atomic in the underlying file system.
    */
    void writeHeader(int64_t fileLength, int32_t elementCount, int64_t firstPosition, int64_t lastPosition) throw (IOException);

    Element readElement(int64_t position) throw (IOException);

    /** Wraps the position if it exceeds the end of the file. */
    int64_t wrapPosition(int64_t position);

    /**
    * Writes count bytes from buffer to position in file. Automatically wraps write if position is
    * past the end of the file or if buffer overlaps it.
    *
    * @param position in file to write to
    * @param buffer to write from
    * @param count # of bytes to write
    */
    void ringWrite(int64_t position, const uint8_t* buffer, int32_t count) throw (IOException);

    /**
    * Reads count bytes into buffer from file. Wraps if necessary.
    *
    * @param position in file to read from
    * @param buffer to read into
    * @param count # of bytes to read
    */
    void ringRead(int64_t position, uint8_t* buffer, int32_t count) throw (IOException);

    void ringErase(int64_t position, int64_t length) throw (IOException);

    /**
    * If necessary, expands the file to accommodate an additional element of the given length.
    *
    * @param dataLength length of data being added
    */
    void expandIfNecessary(int64_t dataLength) throw (IOException);
};


class ElementIterator {
    //TODO
};

}}}  // namespace com.squareup.tape2
