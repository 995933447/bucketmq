// Code generated by protoc-gen-go. DO NOT EDIT.
// source: pkg/proto/ha.proto

package ha // import "github.com/995933447/bucketmq/pkg/rpc/ha"

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type MsgFileType int32

const (
	MsgFileType_MsgFileTypeNil      MsgFileType = 0
	MsgFileType_MsgFileTypeIdx      MsgFileType = 1
	MsgFileType_MsgFileTypeData     MsgFileType = 2
	MsgFileType_MsgFileTypeFinish   MsgFileType = 3
	MsgFileType_MsgFileTypeMsgId    MsgFileType = 4
	MsgFileType_MsgFileTypeLoadBoot MsgFileType = 5
)

var MsgFileType_name = map[int32]string{
	0: "MsgFileTypeNil",
	1: "MsgFileTypeIdx",
	2: "MsgFileTypeData",
	3: "MsgFileTypeFinish",
	4: "MsgFileTypeMsgId",
	5: "MsgFileTypeLoadBoot",
}
var MsgFileType_value = map[string]int32{
	"MsgFileTypeNil":      0,
	"MsgFileTypeIdx":      1,
	"MsgFileTypeData":     2,
	"MsgFileTypeFinish":   3,
	"MsgFileTypeMsgId":    4,
	"MsgFileTypeLoadBoot": 5,
}

func (x MsgFileType) String() string {
	return proto.EnumName(MsgFileType_name, int32(x))
}
func (MsgFileType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_ha_b8573b97f6272f74, []int{0}
}

type SyncMsgFileLogItem struct {
	Topic                string      `protobuf:"bytes,1,opt,name=topic,proto3" json:"topic,omitempty"`
	Subscriber           string      `protobuf:"bytes,2,opt,name=subscriber,proto3" json:"subscriber,omitempty"`
	FileBuf              []byte      `protobuf:"bytes,3,opt,name=file_buf,json=fileBuf,proto3" json:"file_buf,omitempty"`
	MsgFileType          MsgFileType `protobuf:"varint,4,opt,name=msg_file_type,json=msgFileType,proto3,enum=broker.MsgFileType" json:"msg_file_type,omitempty"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
	XXX_sizecache        int32       `json:"-"`
}

func (m *SyncMsgFileLogItem) Reset()         { *m = SyncMsgFileLogItem{} }
func (m *SyncMsgFileLogItem) String() string { return proto.CompactTextString(m) }
func (*SyncMsgFileLogItem) ProtoMessage()    {}
func (*SyncMsgFileLogItem) Descriptor() ([]byte, []int) {
	return fileDescriptor_ha_b8573b97f6272f74, []int{0}
}
func (m *SyncMsgFileLogItem) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SyncMsgFileLogItem.Unmarshal(m, b)
}
func (m *SyncMsgFileLogItem) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SyncMsgFileLogItem.Marshal(b, m, deterministic)
}
func (dst *SyncMsgFileLogItem) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SyncMsgFileLogItem.Merge(dst, src)
}
func (m *SyncMsgFileLogItem) XXX_Size() int {
	return xxx_messageInfo_SyncMsgFileLogItem.Size(m)
}
func (m *SyncMsgFileLogItem) XXX_DiscardUnknown() {
	xxx_messageInfo_SyncMsgFileLogItem.DiscardUnknown(m)
}

var xxx_messageInfo_SyncMsgFileLogItem proto.InternalMessageInfo

func (m *SyncMsgFileLogItem) GetTopic() string {
	if m != nil {
		return m.Topic
	}
	return ""
}

func (m *SyncMsgFileLogItem) GetSubscriber() string {
	if m != nil {
		return m.Subscriber
	}
	return ""
}

func (m *SyncMsgFileLogItem) GetFileBuf() []byte {
	if m != nil {
		return m.FileBuf
	}
	return nil
}

func (m *SyncMsgFileLogItem) GetMsgFileType() MsgFileType {
	if m != nil {
		return m.MsgFileType
	}
	return MsgFileType_MsgFileTypeNil
}

func init() {
	proto.RegisterType((*SyncMsgFileLogItem)(nil), "broker.SyncMsgFileLogItem")
	proto.RegisterEnum("broker.MsgFileType", MsgFileType_name, MsgFileType_value)
}

func init() { proto.RegisterFile("pkg/proto/ha.proto", fileDescriptor_ha_b8573b97f6272f74) }

var fileDescriptor_ha_b8573b97f6272f74 = []byte{
	// 295 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x5c, 0x90, 0xcf, 0x4e, 0x32, 0x31,
	0x14, 0x47, 0xbf, 0xf2, 0xef, 0xd3, 0x8b, 0x62, 0xbd, 0x60, 0x1c, 0x37, 0x86, 0xb8, 0x9a, 0xb0,
	0x98, 0x49, 0x00, 0x43, 0x58, 0x4a, 0x0c, 0x91, 0x04, 0x5c, 0xa0, 0x2b, 0x37, 0x64, 0x3a, 0x94,
	0x99, 0x06, 0x86, 0xd6, 0xb6, 0x93, 0x38, 0x6f, 0xe1, 0x13, 0xf8, 0xac, 0x86, 0xc1, 0xc4, 0xc6,
	0x5d, 0xcf, 0x39, 0x8b, 0xde, 0xfc, 0x00, 0xd5, 0x36, 0x09, 0x95, 0x96, 0x56, 0x86, 0x69, 0x14,
	0x94, 0x0f, 0x6c, 0x30, 0x2d, 0xb7, 0x5c, 0xdf, 0x7d, 0x11, 0xc0, 0x97, 0x62, 0x1f, 0x2f, 0x4c,
	0x32, 0x15, 0x3b, 0x3e, 0x97, 0xc9, 0xcc, 0xf2, 0x0c, 0x3b, 0x50, 0xb7, 0x52, 0x89, 0xd8, 0x23,
	0x5d, 0xe2, 0x9f, 0x2e, 0x8f, 0x80, 0xb7, 0x00, 0x26, 0x67, 0x26, 0xd6, 0x82, 0x71, 0xed, 0x55,
	0xca, 0xe4, 0x18, 0xbc, 0x81, 0x93, 0x8d, 0xd8, 0xf1, 0x15, 0xcb, 0x37, 0x5e, 0xb5, 0x4b, 0xfc,
	0xb3, 0xe5, 0xff, 0x03, 0x4f, 0xf2, 0x0d, 0x8e, 0xe0, 0x3c, 0x33, 0xc9, 0xaa, 0xcc, 0xb6, 0x50,
	0xdc, 0xab, 0x75, 0x89, 0xdf, 0xea, 0xb7, 0x83, 0xe3, 0x1d, 0xc1, 0xcf, 0xff, 0xaf, 0x85, 0xe2,
	0xcb, 0x66, 0xf6, 0x0b, 0xbd, 0x4f, 0x02, 0x4d, 0x27, 0x22, 0x42, 0xcb, 0xc1, 0x67, 0xb1, 0xa3,
	0xff, 0xfe, 0xb8, 0xd9, 0xfa, 0x83, 0x12, 0x6c, 0xc3, 0x85, 0xe3, 0x1e, 0x23, 0x1b, 0xd1, 0x0a,
	0x5e, 0xc1, 0xa5, 0x23, 0xa7, 0x62, 0x2f, 0x4c, 0x4a, 0xab, 0xd8, 0x01, 0xea, 0xe8, 0x85, 0x49,
	0x66, 0x6b, 0x5a, 0xc3, 0x6b, 0x68, 0x3b, 0x76, 0x2e, 0xa3, 0xf5, 0x44, 0x4a, 0x4b, 0xeb, 0xfd,
	0x1a, 0x54, 0x9e, 0x1e, 0x26, 0xbd, 0x37, 0x3f, 0x11, 0x36, 0xcd, 0x59, 0x10, 0xcb, 0x2c, 0x1c,
	0x8f, 0xef, 0xc7, 0x83, 0xc1, 0x70, 0x38, 0x0a, 0x59, 0x1e, 0x6f, 0xb9, 0xcd, 0xde, 0xc3, 0xc3,
	0xea, 0x5a, 0xc5, 0x61, 0x1a, 0xb1, 0x46, 0x39, 0xfa, 0xe0, 0x3b, 0x00, 0x00, 0xff, 0xff, 0x61,
	0xcc, 0xc8, 0xc7, 0x8a, 0x01, 0x00, 0x00,
}
