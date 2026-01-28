from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class splitRequest(_message.Message):
    __slots__ = ("outputs", "tid", "sid", "keep_alive")
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    TID_FIELD_NUMBER: _ClassVar[int]
    SID_FIELD_NUMBER: _ClassVar[int]
    KEEP_ALIVE_FIELD_NUMBER: _ClassVar[int]
    outputs: _containers.RepeatedCompositeFieldContainer[Dict]
    tid: str
    sid: str
    keep_alive: bool
    def __init__(self, outputs: _Optional[_Iterable[_Union[Dict, _Mapping]]] = ..., tid: _Optional[str] = ..., sid: _Optional[str] = ..., keep_alive: bool = ...) -> None: ...

class splitResponse(_message.Message):
    __slots__ = ("inputs", "sid")
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    SID_FIELD_NUMBER: _ClassVar[int]
    inputs: _containers.RepeatedScalarFieldContainer[str]
    sid: str
    def __init__(self, inputs: _Optional[_Iterable[str]] = ..., sid: _Optional[str] = ...) -> None: ...

class Dict(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: str
    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
