# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: master_mapper_reducer.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1bmaster_mapper_reducer.proto\"\x1d\n\x05Point\x12\t\n\x01x\x18\x01 \x01(\x01\x12\t\n\x01y\x18\x02 \x01(\x01\"6\n\tDataPoint\x12\x13\n\x0b\x63\x65ntroid_id\x18\x01 \x01(\x05\x12\t\n\x01x\x18\x02 \x01(\x01\x12\t\n\x01y\x18\x03 \x01(\x01\"e\n\nMapRequest\x12\x13\n\x0bstart_index\x18\x01 \x03(\x05\x12\x11\n\tend_index\x18\x02 \x03(\x05\x12\x14\n\x0cnum_reducers\x18\x03 \x01(\x05\x12\x19\n\tcentroids\x18\x04 \x03(\x0b\x32\x06.Point\"\x1e\n\x0bMapResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\"\'\n\x11ReturnDataRequest\x12\x12\n\nreducer_id\x18\x01 \x01(\x05\"5\n\x12ReturnDataResponse\x12\x1f\n\x0b\x64\x61ta_points\x18\x01 \x03(\x0b\x32\n.DataPoint\"(\n\x12StartReduceRequest\x12\x12\n\nmapper_ids\x18\x01 \x03(\x05\"&\n\x13StartReduceResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\x32\x65\n\x06Mapper\x12\"\n\x03Map\x12\x0b.MapRequest\x1a\x0c.MapResponse\"\x00\x12\x37\n\nReturnData\x12\x12.ReturnDataRequest\x1a\x13.ReturnDataResponse\"\x00\x32\x45\n\x07Reducer\x12:\n\x0bStartReduce\x12\x13.StartReduceRequest\x1a\x14.StartReduceResponse\"\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'master_mapper_reducer_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_POINT']._serialized_start=31
  _globals['_POINT']._serialized_end=60
  _globals['_DATAPOINT']._serialized_start=62
  _globals['_DATAPOINT']._serialized_end=116
  _globals['_MAPREQUEST']._serialized_start=118
  _globals['_MAPREQUEST']._serialized_end=219
  _globals['_MAPRESPONSE']._serialized_start=221
  _globals['_MAPRESPONSE']._serialized_end=251
  _globals['_RETURNDATAREQUEST']._serialized_start=253
  _globals['_RETURNDATAREQUEST']._serialized_end=292
  _globals['_RETURNDATARESPONSE']._serialized_start=294
  _globals['_RETURNDATARESPONSE']._serialized_end=347
  _globals['_STARTREDUCEREQUEST']._serialized_start=349
  _globals['_STARTREDUCEREQUEST']._serialized_end=389
  _globals['_STARTREDUCERESPONSE']._serialized_start=391
  _globals['_STARTREDUCERESPONSE']._serialized_end=429
  _globals['_MAPPER']._serialized_start=431
  _globals['_MAPPER']._serialized_end=532
  _globals['_REDUCER']._serialized_start=534
  _globals['_REDUCER']._serialized_end=603
# @@protoc_insertion_point(module_scope)