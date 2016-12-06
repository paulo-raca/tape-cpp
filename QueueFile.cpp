#include "QueueFile.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

using namespace com::squareup::tape2;
using namespace std;

namespace com { namespace squareup { namespace tape2 {

/** Leading bit set to 1 indicating a versioned header and the version of 1. */
#define VERSIONED_HEADER 0x80000001

/** Initial file size in bytes. */
#define INITIAL_LENGTH 4096

/** Length of element header in bytes. */
#define ELEMENT_HEADER_LENGTH 4

/** A block of nothing to write over old data. */
static const uint8_t ZEROES[INITIAL_LENGTH] = {0};

static const Element NULL_ELEMENT(0, 0);


IOException::IOException(const char* message)
: num(0), message(message)
{ }

IOException::IOException(int num)
: num(num), message(strerror(num))
{ }

Element::Element(int64_t position, int32_t length) 
: position(position), 
  length(length)
{ }

string Element::toString() {
    return "Element [position=" + to_string(position)
          + ", length=" + to_string(length)
          + "]";
}

/**
 * Stores an {@code int} in the {@code byte[]}. The behavior is equivalent to calling
 * {@link RandomAccessFile#writeInt}.
 */
static void writeInt(uint8_t* buffer, int32_t value) {
    buffer[0] = (value >> 24);
    buffer[1] = (value >> 16);
    buffer[2] = (value >> 8);
    buffer[3] = value;
}


/** Reads an {@code int} from the {@code byte[]}. */
static int32_t readInt(uint8_t* buffer) {
    return ((buffer[0] & 0xff) << 24)
        +  ((buffer[1] & 0xff) << 16)
        +  ((buffer[2] & 0xff) << 8)
        +   (buffer[3] & 0xff);
}

/**
  * Stores an {@code long} in the {@code byte[]}. The behavior is equivalent to calling
  * {@link RandomAccessFile#writeLong}.
  */
static void writeLong(uint8_t* buffer, int64_t value) {
    buffer[0] = (value >> 56);
    buffer[1] = (value >> 48);
    buffer[2] = (value >> 40);
    buffer[3] = (value >> 32);
    buffer[4] = (value >> 24);
    buffer[5] = (value >> 16);
    buffer[6] = (value >>  8);
    buffer[7] = value;
}

/** Reads an {@code long} from the {@code byte[]}. */
static int64_t readLong(uint8_t* buffer) {
    return ((buffer[0] & 0xffL) << 56)
        +  ((buffer[1] & 0xffL) << 48)
        +  ((buffer[2] & 0xffL) << 40)
        +  ((buffer[3] & 0xffL) << 32)
        +  ((buffer[4] & 0xffL) << 24)
        +  ((buffer[5] & 0xffL) << 16)
        +  ((buffer[6] & 0xffL) <<  8)
        +   (buffer[7] & 0xffL);
}


namespace io {
    template<typename T>
    static inline T checkError(T ret) throw (IOException) {
      if (ret < 0) {
        throw IOException(ret);
      } else {
        return ret;
      }
    }
    
    /** Read a block of data from the file */
    static void read(int fd, off_t offset, uint8_t* buffer, size_t count) throw (IOException) {
        //A read operation probably won't be split in multiple chunks, but we have to handle it, just in case...
        while (count) {
            ssize_t amount = checkError(pread(fd, buffer, count, offset));
            if (amount == 0) {
                throw IOException("EOF");
            } else {
                count -= amount;
                offset += amount;
            }
        }
    }

    /** Write a block of data from the file */
    static void write(int fd, off_t offset, const uint8_t* buffer, size_t count) throw (IOException) {
        //A write operation probably won't be split in multiple chunks, but we have to handle it, just in case...
        while (count) {
            ssize_t amount = checkError(pwrite(fd, buffer, count, offset));
            if (amount == 0) {
                throw IOException("EOF");
            } else {
                count -= amount;
                offset += amount;
            }
        }
    }

    /** Copy a slice of a file into another location */
    static void move(int fd, off_t src_offset, off_t dest_offset, size_t count, bool zero=false) throw (IOException) {
        //FIXME -- I can do much better!  Maybe with copy_file_range syscall?
        uint8_t* buffer = new uint8_t[count];
        io::read(fd, src_offset, buffer, count);
        if (zero) {
            uint8_t* zeros = new uint8_t[count];
            io::write(fd, src_offset, zeros, count);
            delete(zeros);
        }    
        io::write(fd, dest_offset, buffer, count);
        delete(buffer);
    }


    /** Return the file length from the file descriptor */
    static off_t getFileLength(int fd) throw (IOException) {
        struct stat buf;
        checkError(fstat(fd, &buf));
        return buf.st_size;
    }


    /** Return the file length from the file descriptor */
    static void setFileLength(int fd, off_t length) throw (IOException) {
        checkError(ftruncate(fd, length));  
    }
}

static int initializeFromFile(const char* file, bool forceLegacy) throw (IOException) {
    int fd = open(file, O_RDWR | O_SYNC);
    if (fd == -1) {
        if (errno != ENOENT) { 
          // Some random IO error
          throw IOException(errno);
        } else {
            // No such file or directory -- create a new file!
            
            uint8_t initial_contents[INITIAL_LENGTH] = {};
            if (forceLegacy) {
                writeInt(initial_contents + 0, INITIAL_LENGTH);
            } else {
                writeInt(initial_contents  + 0, VERSIONED_HEADER);
                writeLong(initial_contents + 4, INITIAL_LENGTH);
            }

            // Write to temporary file
            string tmp_file = string(file) + ".tmp";
            fd = io::checkError(open(tmp_file.c_str(), O_RDWR | O_SYNC | O_CREAT, S_IRUSR | S_IWUSR));
            try {
                io::write(fd, 0, initial_contents, sizeof(initial_contents));
            } catch (...) {
                close(fd);
                throw;
            }            
            close(fd);
            
            // Move temp to real location
            io::checkError(rename(tmp_file.c_str(), file));
            
            // Open file
            fd = io::checkError(open(file, O_RDWR | O_SYNC));
        }
    }
    return fd;
}

QueueFile::QueueFile(const char* file, bool zero, bool forceLegacy) throw (IOException)
  : QueueFile(initializeFromFile(file, forceLegacy), zero, forceLegacy)
{ }

QueueFile::QueueFile(int fd, bool zero, bool forceLegacy) throw (IOException)
  : fd(fd), 
    closed(false),
    zero(zero),
    modCount(0),
    first(NULL_ELEMENT),
    last(NULL_ELEMENT)
{
    try {
        uint8_t buffer[32];
        io::read(fd, 0, buffer, sizeof(buffer));
        
        this->versioned = !forceLegacy && (buffer[0] & 0x80) != 0;
        int64_t firstOffset;
        int64_t lastOffset;
        if (versioned) {
            this->headerLength = 32;

            int32_t version = readInt(buffer + 0) & 0x7FFFFFFF;
            if (version != 1) {
                throw new IOException("Unable to read version format. Supported versions are 1 and legacy.");
            }
            this->fileLength = readLong(buffer + 4);
            this->elementCount = readInt(buffer + 12);
            firstOffset = readLong(buffer + 16);
            lastOffset = readLong(buffer + 24);
        } else {
            this->headerLength = 16;

            this->fileLength = readInt(buffer + 0);
            this->elementCount = readInt(buffer + 4);
            firstOffset = readInt(buffer + 8);
            lastOffset = readInt(buffer + 12);
        }
        
        if (this->fileLength != io::getFileLength(fd)) {
            throw new IOException("File is corrupt; length stored in header doesn't match the actual file length");
        }

        this->first = readElement(firstOffset);
        this->last = readElement(lastOffset);
    } catch (...) {
        ::close(fd);
        throw;
    }
}

int32_t QueueFile::size() {
    return this->elementCount;
}

bool QueueFile::isEmpty() {
    return this->elementCount == 0;
}
    
int64_t QueueFile::usedBytes() {
    if (this->elementCount == 0) {
        return this->headerLength;
        
    } else if (this->last.position >= this->first.position) {
      // Contiguous queue.
      return (this->last.position - this->first.position)   // all but last entry
          + ELEMENT_HEADER_LENGTH + this->last.length       // last entry
          + headerLength;
    } else {
      // tail < head. The queue wraps.
      return this->last.position                            // buffer front + header
          + ELEMENT_HEADER_LENGTH + this->last.length       // last entry
          + this->fileLength - this->first.position;        // buffer end
    }
}

int64_t QueueFile::remainingBytes() {
    return this->fileLength - this->usedBytes();
}

void QueueFile::add(const uint8_t* data, int32_t count) throw (IOException) {
//     if (data == null) {
//         throw new NullPointerException("data == null");
//     }
//     if ((offset | count) < 0 || count > data.length - offset) {
//         throw new IndexOutOfBoundsException();
//     }    
    if (this->closed) {
        throw IOException("closed");
    }
    this->expandIfNecessary(count);

    // Insert a new element after the current last element.
    bool wasEmpty = isEmpty();
    long position = wasEmpty 
        ? this->headerLength
        : wrapPosition(last.position + ELEMENT_HEADER_LENGTH + last.length);
    Element newLast(position, count);

    // Write length.
    uint8_t buffer[ELEMENT_HEADER_LENGTH];
    writeInt(buffer, count);
    ringWrite(newLast.position, buffer, ELEMENT_HEADER_LENGTH);

    // Write data.
    ringWrite(newLast.position + ELEMENT_HEADER_LENGTH, data, count);

    // Commit the addition. If wasEmpty, first == last.
    uint64_t firstPosition = wasEmpty 
        ? newLast.position 
        : this->first.position;
    writeHeader(this->fileLength, this->elementCount + 1, firstPosition, newLast.position);
    last = newLast;
    elementCount++;
    modCount++;
    if (wasEmpty) {
        first = last;  // first element
    }
}

void QueueFile::remove(int32_t n) throw (IOException) {
//     if (n < 0) {
//         throw new IllegalArgumentException("Cannot remove negative (" + n + ") number of elements.");
//     }
    if (n == 0) {
      return;
    }
    if (n == this->elementCount) {
      this->clear();
      return;
    }
//     if (isEmpty()) {
//         throw new NoSuchElementException();
//     }
//     if (n > elementCount) {
//         throw new IllegalArgumentException(
//             "Cannot remove more elements (" + n + ") than present in queue (" + elementCount + ").");
//     }

    int64_t eraseStartPosition = this->first.position;
    int64_t eraseTotalLength = 0;

    // Read the position and length of the new first element.
    Element newFirstElement = this->first;
    for (int i = 0; i < n; i++) {
        eraseTotalLength += ELEMENT_HEADER_LENGTH + newFirstElement.length;
        newFirstElement = readElement(newFirstElement.position + ELEMENT_HEADER_LENGTH + newFirstElement.length);
    }

    // Commit the header.
    writeHeader(this->fileLength, this->elementCount - n, newFirstElement.position, this->last.position);
    elementCount -= n;
    modCount++;
    first = newFirstElement;

    if (zero) {
        ringErase(eraseStartPosition, eraseTotalLength);
    }
}

    
/** 
  * Reads the eldest element. Returns null if the queue is empty. 
  * You have to free the buffer after using it.
  */
void QueueFile::peek(uint8_t* &buffer, int32_t &length) throw (IOException) {
    //TODO
}

void QueueFile::clear() throw (IOException) {
    if (closed) {
        throw IOException("closed");
    }

    // Commit the header.
    writeHeader(INITIAL_LENGTH, 0, 0, 0);

    if (this->zero) {
      ringErase(headerLength, INITIAL_LENGTH - headerLength);
    }

    this->elementCount = 0;
    this->first = NULL_ELEMENT;
    this->last = NULL_ELEMENT;
    if (this->fileLength != INITIAL_LENGTH) {
        io::setFileLength(fd, INITIAL_LENGTH);
        fileLength = INITIAL_LENGTH;
    }
    modCount++;
}

void QueueFile::close() throw (IOException) {
    if (!closed) {
        ::close(fd);
        closed = true;
    }
}

std::string QueueFile::toString() {
    return "QueueFile"
          "[length=" + to_string(this->fileLength)
        + ", size=" + to_string(elementCount)
        + ", first=" + first.toString()
        + ", last=" + last.toString()
        + "]";
}

    
void QueueFile::writeHeader(int64_t fileLength, int32_t elementCount, int64_t firstPosition, int64_t lastPosition) throw (IOException) {
    if (versioned) {
        uint8_t buffer[32];
        writeInt (buffer +  0, VERSIONED_HEADER);
        writeLong(buffer +  4, fileLength);
        writeInt (buffer + 12, elementCount);
        writeLong(buffer + 16, firstPosition);
        writeLong(buffer + 24, lastPosition);
        io::write(fd, 0, buffer, sizeof(buffer));
    } else {
        // Legacy queue header.
        uint8_t buffer[16];
        writeInt(buffer +  0, (int32_t) fileLength); // Signed, so leading bit is always 0 aka legacy.
        writeInt(buffer +  4, elementCount);
        writeInt(buffer +  8, (int32_t) firstPosition);
        writeInt(buffer + 12, (int32_t) lastPosition);
        io::write(fd, 0, buffer, sizeof(buffer));
    }
}

Element QueueFile::readElement(int64_t position) throw (IOException) {
    if (position == 0) {
        return NULL_ELEMENT;
    } else {
        uint8_t buffer[ELEMENT_HEADER_LENGTH];
        ringRead(position, buffer, sizeof(buffer));
        int32_t length = readInt(buffer);
        return Element(position, length);
    }
}

int64_t QueueFile::wrapPosition(int64_t position) {
    return position < this->fileLength 
        ? position 
        : this->headerLength + position - this->fileLength;
}

void QueueFile::ringWrite(int64_t position, const uint8_t* buffer, int32_t count) throw (IOException) {
    position = wrapPosition(position);
    if (position + count <= this->fileLength) {
        io::write(fd, position, buffer, count);
    } else {
      // The write overlaps the EOF.
      // # of bytes to write before the EOF. Guaranteed to be less than Integer.MAX_VALUE.
      int32_t beforeEof = (int32_t) (this->fileLength - position);
      io::write(fd, position, buffer, beforeEof);
      io::write(fd, headerLength, buffer + beforeEof, count - beforeEof);
    }
}

void QueueFile::ringRead(int64_t position, uint8_t* buffer, int32_t count) throw (IOException) {
    position = wrapPosition(position);
    if (position + count <= this->fileLength) {
        io::read(fd, position, buffer, count);
    } else {
      // The read overlaps the EOF.
      // # of bytes to read before the EOF. Guaranteed to be less than Integer.MAX_VALUE.
      int beforeEof = (int32_t) (this->fileLength - position);
      io::read(fd, position, buffer, beforeEof);
      io::read(fd, headerLength, buffer + beforeEof, count - beforeEof);
    }
}

void QueueFile::ringErase(int64_t position, int64_t length) throw (IOException) {
  while (length > 0) {
      int32_t chunk = length < sizeof(ZEROES) 
          ? length 
          : sizeof(ZEROES);          
      ringWrite(position, ZEROES, chunk);
      length -= chunk;
      position += chunk;
  }
}
    
void QueueFile::expandIfNecessary(int64_t dataLength) throw (IOException) {
    int64_t elementLength = ELEMENT_HEADER_LENGTH + dataLength;
    int64_t usedBytes = this->usedBytes();
    if (usedBytes + elementLength <= fileLength) return;

    // Double the length until we can fit the new data.
    int64_t newLength = this->fileLength;
    do {
        newLength = newLength << 1;
    } while (usedBytes + elementLength > newLength);

    io::setFileLength(this->fd, newLength);

    // Calculate the position of the tail end of the data in the ring buffer
    int64_t endOfLastElement = wrapPosition(this->last.position + ELEMENT_HEADER_LENGTH + this->last.length);

    // If the buffer is split, we need to make it contiguous
    if (endOfLastElement <= this->first.position) {
        int64_t count = endOfLastElement - this->headerLength;
        io::move(this->fd, this->headerLength, this->fileLength, count, this->zero);
    }

    // Commit the expansion.
    if (this->last.position < this->first.position) {
        int64_t newLastPosition = this->fileLength + this->last.position - this->headerLength;
        writeHeader(newLength, this->elementCount, this->first.position, newLastPosition);
        this->last.position = newLastPosition;
    } else {
        writeHeader(newLength, this->elementCount, this->first.position, this->last.position);
    }
    
    this->fileLength = newLength;
}
    

}}}  // namespace com.squareup.tape2

int main() {
    com::squareup::tape2::QueueFile queue("file.bin");
}
