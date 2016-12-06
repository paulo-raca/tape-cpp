# Tape2 C++

I need a fast, file-backed Queue in C++.

I started using an SQLite DB -- SQLite is great, but it takes a lot of IOs to insert or access an element.

After some research, [Square's Tape2 library](https://github.com/square/tape/) seems perfect, but it is a Java library.

I'm too lazy to start from scratch, so I'm porting it to C++.
