#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2010 Radim Rehurek <radimrehurek@seznam.cz>
# Licensed under the GNU LGPL v2.1 - http://www.gnu.org/licenses/lgpl.html

"""
This module contains various general utility functions.
"""

from __future__ import with_statement

import logging, warnings

logger = logging.getLogger(__name__)

# NEED THIS (FIRST) TRY/EXCEPT STATEMENT
try:
    from html.entities import name2codepoint as n2cp
except ImportError:
    from htmlentitydefs import name2codepoint as n2cp

import re
import unicodedata
import os
import random
import itertools
import tempfile
from functools import wraps  # for `synchronous` function lock
import multiprocessing
import shutil
import sys
from contextlib import contextmanager
import subprocess

import numpy
import scipy.sparse

if sys.version_info[0] >= 3:
    unicode = str

from six import iterkeys, iteritems, u, string_types, unichr
from six.moves import xrange

# NEED THIS FUNCTION
PAT_ALPHABETIC = re.compile('(((?![\d])\w)+)', re.UNICODE)
RE_HTML_ENTITY = re.compile(r'&(#?)([xX]?)(\w{1,8});', re.UNICODE)


# NEED THIS FUNCTION
def deaccent(text):
    """
    Remove accentuation from the given string. Input text is either a unicode string or utf8 encoded bytestring.

    Return input string with accents removed, as unicode.

    >>> deaccent("Šéf chomutovských komunistů dostal poštou bílý prášek")
    u'Sef chomutovskych komunistu dostal postou bily prasek'

    """
    if not isinstance(text, unicode):
        # assume utf8 for byte strings, use default (strict) error handling
        text = text.decode('utf8')
    norm = unicodedata.normalize("NFD", text)
    result = u('').join(ch for ch in norm if unicodedata.category(ch) != 'Mn')
    return unicodedata.normalize("NFC", result)


# NEED THIS FUNCTION
def tokenize(text, lowercase=False, deacc=False, errors="strict", to_lower=False, lower=False):
    """
    Iteratively yield tokens as unicode strings, removing accent marks
    and optionally lowercasing the unidoce string by assigning True
    to one of the parameters, lowercase, to_lower, or lower.

    Input text may be either unicode or utf8-encoded byte string.

    The tokens on output are maximal contiguous sequences of alphabetic
    characters (no digits!).

    >>> list(tokenize('Nic nemůže letět rychlostí vyšší, než 300 tisíc kilometrů za sekundu!', deacc = True))
    [u'Nic', u'nemuze', u'letet', u'rychlosti', u'vyssi', u'nez', u'tisic', u'kilometru', u'za', u'sekundu']

    """
    lowercase = lowercase or to_lower or lower
    text = to_unicode(text, errors=errors)
    if lowercase:
        text = text.lower()
    if deacc:
        text = deaccent(text)
    for match in PAT_ALPHABETIC.finditer(text):
        yield match.group()



# NEED THIS FUNCTION
def any2unicode(text, encoding='utf8', errors='strict'):
    """Convert a string (bytestring in `encoding` or unicode), to unicode."""
    if isinstance(text, unicode):
        return text
    return unicode(text, encoding, errors=errors)
to_unicode = any2unicode


# NEED THIS FUNCTION
def safe_unichr(intval):
    try:
        return unichr(intval)
    except ValueError:
        # ValueError: unichr() arg not in range(0x10000) (narrow Python build)
        s = "\\U%08x" % intval
        # return UTF16 surrogate pair
        return s.decode('unicode-escape')

# NEED THIS FUNCTION
def decode_htmlentities(text):
    """
    Decode HTML entities in text, coded as hex, decimal or named.

    Adapted from http://github.com/sku/python-twitter-ircbot/blob/321d94e0e40d0acc92f5bf57d126b57369da70de/html_decode.py

    >>> u = u'E tu vivrai nel terrore - L&#x27;aldil&#xE0; (1981)'
    >>> print(decode_htmlentities(u).encode('UTF-8'))
    E tu vivrai nel terrore - L'aldilà (1981)
    >>> print(decode_htmlentities("l&#39;eau"))
    l'eau
    >>> print(decode_htmlentities("foo &lt; bar"))
    foo < bar

    """
    def substitute_entity(match):
        try:
            ent = match.group(3)
            if match.group(1) == "#":
                # decoding by number
                if match.group(2) == '':
                    # number is in decimal
                    return safe_unichr(int(ent))
                elif match.group(2) in ['x', 'X']:
                    # number is in hex
                    return safe_unichr(int(ent, 16))
            else:
                # they were using a name
                cp = n2cp.get(ent)
                if cp:
                    return safe_unichr(cp)
                else:
                    return match.group()
        except:
            # in case of errors, return original input
            return match.group()

    return RE_HTML_ENTITY.sub(substitute_entity, text)

# NEED THIS FUNCTION
def chunkize_serial(iterable, chunksize, as_numpy=False):
    """
    Return elements from the iterable in `chunksize`-ed lists. The last returned
    element may be smaller (if length of collection is not divisible by `chunksize`).

    >>> print(list(grouper(range(10), 3)))
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

    """
    import numpy
    it = iter(iterable)
    while True:
        if as_numpy:
            # convert each document to a 2d numpy array (~6x faster when transmitting
            # chunk data over the wire, in Pyro)
            wrapped_chunk = [[numpy.array(doc) for doc in itertools.islice(it, int(chunksize))]]
        else:
            wrapped_chunk = [list(itertools.islice(it, int(chunksize)))]
        if not wrapped_chunk[0]:
            break
        # memory opt: wrap the chunk and then pop(), to avoid leaving behind a dangling reference
        yield wrapped_chunk.pop()

grouper = chunkize_serial




# NEED THIS FUNCTION
if os.name == 'nt':
    warnings.warn("detected Windows; aliasing chunkize to chunkize_serial")

    def chunkize(corpus, chunksize, maxsize=0, as_numpy=False):
        for chunk in chunkize_serial(corpus, chunksize, as_numpy=as_numpy):
            yield chunk
else:
    def chunkize(corpus, chunksize, maxsize=0, as_numpy=False):
        """
        Split a stream of values into smaller chunks.
        Each chunk is of length `chunksize`, except the last one which may be smaller.
        A once-only input stream (`corpus` from a generator) is ok, chunking is done
        efficiently via itertools.

        If `maxsize > 1`, don't wait idly in between successive chunk `yields`, but
        rather keep filling a short queue (of size at most `maxsize`) with forthcoming
        chunks in advance. This is realized by starting a separate process, and is
        meant to reduce I/O delays, which can be significant when `corpus` comes
        from a slow medium (like harddisk).

        If `maxsize==0`, don't fool around with parallelism and simply yield the chunksize
        via `chunkize_serial()` (no I/O optimizations).

        >>> for chunk in chunkize(range(10), 4): print(chunk)
        [0, 1, 2, 3]
        [4, 5, 6, 7]
        [8, 9]

        """
        assert chunksize > 0

        if maxsize > 0:
            q = multiprocessing.Queue(maxsize=maxsize)
            worker = InputQueue(q, corpus, chunksize, maxsize=maxsize, as_numpy=as_numpy)
            worker.daemon = True
            worker.start()
            while True:
                chunk = [q.get(block=True)]
                if chunk[0] is None:
                    break
                yield chunk.pop()
        else:
            for chunk in chunkize_serial(corpus, chunksize, as_numpy=as_numpy):
                yield chunk


# NEED THIS FUNCTION
def has_pattern():
    """
    Function which returns a flag indicating whether pattern is installed or not
    """
    try:
        from pattern.en import parse
        return True
    except ImportError:
        return False

# NEED THIS FUNCTION
def lemmatize(content, allowed_tags=re.compile('(NN|VB|JJ|RB)'), light=False,
        stopwords=frozenset(), min_length=2, max_length=15):
    """
    This function is only available when the optional 'pattern' package is installed.

    Use the English lemmatizer from `pattern` to extract UTF8-encoded tokens in
    their base form=lemma, e.g. "are, is, being" -> "be" etc.
    This is a smarter version of stemming, taking word context into account.

    Only considers nouns, verbs, adjectives and adverbs by default (=all other lemmas are discarded).

    >>> lemmatize('Hello World! How is it going?! Nonexistentword, 21')
    ['world/NN', 'be/VB', 'go/VB', 'nonexistentword/NN']

    >>> lemmatize('The study ranks high.')
    ['study/NN', 'rank/VB', 'high/JJ']

    >>> lemmatize('The ranks study hard.')
    ['rank/NN', 'study/VB', 'hard/RB']

    """
    if not has_pattern():
        raise ImportError("Pattern library is not installed. Pattern library is needed in order to use lemmatize function")
    from pattern.en import parse

    if light:
        import warnings
        warnings.warn("The light flag is no longer supported by pattern.")

    # tokenization in `pattern` is weird; it gets thrown off by non-letters,
    # producing '==relate/VBN' or '**/NN'... try to preprocess the text a little
    # FIXME this throws away all fancy parsing cues, including sentence structure,
    # abbreviations etc.
    content = u(' ').join(tokenize(content, lower=True, errors='ignore'))

    parsed = parse(content, lemmata=True, collapse=False)
    result = []
    for sentence in parsed:
        for token, tag, _, _, lemma in sentence:
            if min_length <= len(lemma) <= max_length and not lemma.startswith('_') and lemma not in stopwords:
                if allowed_tags.match(tag):
                    lemma += "/" + tag[:2]
                    result.append(lemma.encode('utf8'))
    return result
