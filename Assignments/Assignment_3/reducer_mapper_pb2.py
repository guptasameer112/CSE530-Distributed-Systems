# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: reducer_mapper.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x14reducer_mapper.proto\x12\x0ereducer_mapper\"A\n\x10PartitionRequest\x12-\n\npartitions\x18\x01 \x03(\x0b\x32\x19.reducer_mapper.Partition\"0\n\tPartition\x12\x13\n\x0b\x63\x65ntroid_id\x18\x01 \x01(\x05\x12\x0e\n\x06values\x18\x02 \x03(\x02\"\x0f\n\rEmptyResponse2^\n\x07Reducer\x12S\n\x10ReceivePartition\x12 .reducer_mapper.PartitionRequest\x1a\x1d.reducer_mapper.EmptyResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'reducer_mapper_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_PARTITIONREQUEST']._serialized_start=40
  _globals['_PARTITIONREQUEST']._serialized_end=105
  _globals['_PARTITION']._serialized_start=107
  _globals['_PARTITION']._serialized_end=155
  _globals['_EMPTYRESPONSE']._serialized_start=157
  _globals['_EMPTYRESPONSE']._serialized_end=172
  _globals['_REDUCER']._serialized_start=174
  _globals['_REDUCER']._serialized_end=268
# @@protoc_insertion_point(module_scope)
