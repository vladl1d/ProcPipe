# -*- coding: utf-8 -*-
"""
Created on Mon Oct 29 15:52:11 2018

@author: V-Liderman
"""
import sys 
import io
from yajl import (
    YajlContentHandler, YajlGen, YajlParser,
    YajlGenException,
)

class YajlParserStr(YajlParser):
     def parse(self, f=sys.stdin, ctx=None):
         if isinstance(f, str):
            f = io.BytesIO(f.encode('utf-8'))

         super(YajlParserStr, self).parse(f, ctx)
    
class ReformatContentHandler(YajlContentHandler):
    '''
    Content handler to reformat a json file using yajl_gen
    '''
    def __init__(self, beautify=True, indent_string=b"  ", stream=False):
        self.out = sys.stdout
        self.beautify = beautify
        self.indent_string = indent_string
        self.stream = stream
    def parse_start(self):
        self.g = YajlGen(
            beautify=self.beautify,
            indent_string=self.indent_string,
        )
    def parse_buf(self):
        self.out.write(self.g.yajl_gen_get_buf().decode('utf-8'))
    def parse_complete(self):
        if not self.stream:
            # not necessary, gc will do this @ python shutdown
            del self.g
    def check_and_return(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except YajlGenException as e:
            if self.stream and e.value == 'yajl_gen_generation_complete':
                self.g.yajl_gen_reset(b'\n')
                func(*args, **kwargs)
            else:
                raise
    def yajl_null(self, ctx):
        self.check_and_return(
            self.g.yajl_gen_null,
        )
    def yajl_boolean(self, ctx, boolVal):
        self.check_and_return(
            self.g.yajl_gen_bool,
            boolVal
        )
    def yajl_number(self, ctx, stringNum):
        self.check_and_return(
            self.g.yajl_gen_number,
            stringNum
        )
    def yajl_string(self, ctx, stringVal):
        self.check_and_return(
            self.g.yajl_gen_string,
            stringVal
        )
    def yajl_start_map(self, ctx):
        self.check_and_return(
            self.g.yajl_gen_map_open,
        )
    def yajl_map_key(self, ctx, stringVal):
        self.check_and_return(
            self.g.yajl_gen_string,
            stringVal
        )
    def yajl_end_map(self, ctx):
        self.check_and_return(
            self.g.yajl_gen_map_close,
        )
    def yajl_start_array(self, ctx):
        self.check_and_return(
            self.g.yajl_gen_array_open,
        )
    def yajl_end_array(self, ctx):
        self.check_and_return(
            self.g.yajl_gen_array_close,
        )


def main():
    ch = ReformatContentHandler(
        beautify=True,
        stream=True,
    )
    # initialize the parser
    yajl_parser = YajlParserStr(ch)
    yajl_parser.allow_comments = True  # let's allow comments by default
    yajl_parser.allow_multiple_values = True
    yajl_parser.dont_validate_strings = True
    yajl_parser.parse('{"c":2}')
    
main()