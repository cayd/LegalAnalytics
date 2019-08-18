#!/usr/bin/env python

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse
import sys
import json
import functools
from itertools import tee

def sanitize(text):
    # step 1
    text = re.sub('\s+', ' ', text)
    
    # step 2
    url_regex = re.compile(
        r"""(?i)\b((?:[a-z][\w-]+:(?:/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>\[\]]+|\(([^\s()<>\[\]]+|(\([^\s()<>\[\]]+\)))*\))+(?:\(([^\s()<>\[\]]+|(\([^\s()<>\[\]]+\)))*\)|[^\s`!(){};:'".,<>?\[\]]))""")
    text = url_regex.sub("", text)
    text = re.sub(u"(\u2018|\u2019)", "'", text)
    
    # step 4
    tokens = re.split(" ", text)

    # step 5
    i = 0
    while i < len(tokens):
        while not (tokens[i][:1].isalnum() or len(tokens[i]) <= 1):
            tokens.insert(i, tokens[i][:1])
            tokens[i+1] = tokens[i+1][1:]
            i += 1
        while not (len(tokens[i]) <= 1 or tokens[i][-1:].isalnum()):
            tokens.insert(i+1, tokens[i][-1:])
            tokens[i] = tokens[i][:-1]
        i += 1

    # step 6
    i = 0
    allowable_tokens = ['.', '!', '?', ',', ';', ':']
    while i < len(tokens):
        if len(tokens[i]) == 1 and not tokens[i].isalnum() and not tokens[i] in allowable_tokens:
            del tokens[i]
        else:
            i += 1

    # step 7
    for i in range(len(tokens)):
        tokens[i] = tokens[i].lower()
        
    # step 9
    parsed_text, unigrams, bigrams, trigrams = "", "", "", ""
    for token in tokens:
        parsed_text += token + " "
        if token[:1].isalnum():
            unigrams += token + " "
            
    for t1, t2 in zip(tokens, tokens[1:]):
        if t1[:1].isalnum() and t2[:1].isalnum():
            bigrams += t1 + "_" + t2 + " "
            
    for t1, t2, t3 in zip(tokens, tokens[1:], tokens[2:]):
        if t1[:1].isalnum() and t2[:1].isalnum() and t3[:1].isalnum():
            trigrams += t1 + "_" + t2 + "_" + t3 + " "

    return [parsed_text.lstrip().rstrip(), unigrams.lstrip().rstrip(),
            bigrams.lstrip().rstrip(), trigrams.lstrip().rstrip()]


if __name__ == "__main__":
    filename = sys.argv[1]
    with open(filename, 'r') as f:
        lines = f.readlines()
        
    for line in lines:
        str = json.loads(line)["opinion"]
        print(set(sanitize(str)))




                                                                                                                                                                                                                                                                                                                                                                        
