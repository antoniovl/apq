#!/usr/bin/env python
"""
Dummy script to emulate postqueue.
"""

if __name__ == "__main__":
    f = open("mailq-out.txt", "r")
    print(f.read())
