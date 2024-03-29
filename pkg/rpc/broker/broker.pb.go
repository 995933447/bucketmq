// Code generated by protoc-gen-go. DO NOT EDIT.
// source: pkg/proto/broker.proto

package broker // import "github.com/995933447/bucketmq/pkg/rpc/broker"

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Topic struct {
	Topic                string   `protobuf:"bytes,1,opt,name=topic,proto3" json:"topic,omitempty"`
	NodeGrp              string   `protobuf:"bytes,2,opt,name=node_grp,json=nodeGrp,proto3" json:"node_grp,omitempty"`
	MaxMsgBytes          uint32   `protobuf:"varint,3,opt,name=max_msg_bytes,json=maxMsgBytes,proto3" json:"max_msg_bytes,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Topic) Reset()         { *m = Topic{} }
func (m *Topic) String() string { return proto.CompactTextString(m) }
func (*Topic) ProtoMessage()    {}
func (*Topic) Descriptor() ([]byte, []int) {
	return fileDescriptor_broker_be36a622fce1a135, []int{0}
}
func (m *Topic) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Topic.Unmarshal(m, b)
}
func (m *Topic) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Topic.Marshal(b, m, deterministic)
}
func (dst *Topic) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Topic.Merge(dst, src)
}
func (m *Topic) XXX_Size() int {
	return xxx_messageInfo_Topic.Size(m)
}
func (m *Topic) XXX_DiscardUnknown() {
	xxx_messageInfo_Topic.DiscardUnknown(m)
}

var xxx_messageInfo_Topic proto.InternalMessageInfo

func (m *Topic) GetTopic() string {
	if m != nil {
		return m.Topic
	}
	return ""
}

func (m *Topic) GetNodeGrp() string {
	if m != nil {
		return m.NodeGrp
	}
	return ""
}

func (m *Topic) GetMaxMsgBytes() uint32 {
	if m != nil {
		return m.MaxMsgBytes
	}
	return 0
}

type Subscriber struct {
	Topic                        string   `protobuf:"bytes,1,opt,name=topic,proto3" json:"topic,omitempty"`
	Subscriber                   string   `protobuf:"bytes,2,opt,name=subscriber,proto3" json:"subscriber,omitempty"`
	LoadMsgBootId                uint32   `protobuf:"varint,3,opt,name=load_msg_boot_id,json=loadMsgBootId,proto3" json:"load_msg_boot_id,omitempty"`
	ConcurConsumeNum             uint32   `protobuf:"varint,4,opt,name=concur_consume_num,json=concurConsumeNum,proto3" json:"concur_consume_num,omitempty"`
	MaxConcurConsumeNumPerBucket uint32   `protobuf:"varint,5,opt,name=max_concur_consume_num_per_bucket,json=maxConcurConsumeNumPerBucket,proto3" json:"max_concur_consume_num_per_bucket,omitempty"`
	MsgWeight                    uint32   `protobuf:"varint,6,opt,name=msg_weight,json=msgWeight,proto3" json:"msg_weight,omitempty"`
	LoadMode                     uint32   `protobuf:"varint,7,opt,name=load_mode,json=loadMode,proto3" json:"load_mode,omitempty"`
	StartMsgId                   uint64   `protobuf:"varint,8,opt,name=start_msg_id,json=startMsgId,proto3" json:"start_msg_id,omitempty"`
	IsSerial                     bool     `protobuf:"varint,9,opt,name=is_serial,json=isSerial,proto3" json:"is_serial,omitempty"`
	Consumer                     string   `protobuf:"bytes,10,opt,name=consumer,proto3" json:"consumer,omitempty"`
	MaxConsumeMs                 uint32   `protobuf:"varint,11,opt,name=max_consume_ms,json=maxConsumeMs,proto3" json:"max_consume_ms,omitempty"`
	XXX_NoUnkeyedLiteral         struct{} `json:"-"`
	XXX_unrecognized             []byte   `json:"-"`
	XXX_sizecache                int32    `json:"-"`
}

func (m *Subscriber) Reset()         { *m = Subscriber{} }
func (m *Subscriber) String() string { return proto.CompactTextString(m) }
func (*Subscriber) ProtoMessage()    {}
func (*Subscriber) Descriptor() ([]byte, []int) {
	return fileDescriptor_broker_be36a622fce1a135, []int{1}
}
func (m *Subscriber) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Subscriber.Unmarshal(m, b)
}
func (m *Subscriber) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Subscriber.Marshal(b, m, deterministic)
}
func (dst *Subscriber) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Subscriber.Merge(dst, src)
}
func (m *Subscriber) XXX_Size() int {
	return xxx_messageInfo_Subscriber.Size(m)
}
func (m *Subscriber) XXX_DiscardUnknown() {
	xxx_messageInfo_Subscriber.DiscardUnknown(m)
}

var xxx_messageInfo_Subscriber proto.InternalMessageInfo

func (m *Subscriber) GetTopic() string {
	if m != nil {
		return m.Topic
	}
	return ""
}

func (m *Subscriber) GetSubscriber() string {
	if m != nil {
		return m.Subscriber
	}
	return ""
}

func (m *Subscriber) GetLoadMsgBootId() uint32 {
	if m != nil {
		return m.LoadMsgBootId
	}
	return 0
}

func (m *Subscriber) GetConcurConsumeNum() uint32 {
	if m != nil {
		return m.ConcurConsumeNum
	}
	return 0
}

func (m *Subscriber) GetMaxConcurConsumeNumPerBucket() uint32 {
	if m != nil {
		return m.MaxConcurConsumeNumPerBucket
	}
	return 0
}

func (m *Subscriber) GetMsgWeight() uint32 {
	if m != nil {
		return m.MsgWeight
	}
	return 0
}

func (m *Subscriber) GetLoadMode() uint32 {
	if m != nil {
		return m.LoadMode
	}
	return 0
}

func (m *Subscriber) GetStartMsgId() uint64 {
	if m != nil {
		return m.StartMsgId
	}
	return 0
}

func (m *Subscriber) GetIsSerial() bool {
	if m != nil {
		return m.IsSerial
	}
	return false
}

func (m *Subscriber) GetConsumer() string {
	if m != nil {
		return m.Consumer
	}
	return ""
}

func (m *Subscriber) GetMaxConsumeMs() uint32 {
	if m != nil {
		return m.MaxConsumeMs
	}
	return 0
}

type RegTopicReq struct {
	Topic                *Topic   `protobuf:"bytes,1,opt,name=topic,proto3" json:"topic,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RegTopicReq) Reset()         { *m = RegTopicReq{} }
func (m *RegTopicReq) String() string { return proto.CompactTextString(m) }
func (*RegTopicReq) ProtoMessage()    {}
func (*RegTopicReq) Descriptor() ([]byte, []int) {
	return fileDescriptor_broker_be36a622fce1a135, []int{2}
}
func (m *RegTopicReq) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RegTopicReq.Unmarshal(m, b)
}
func (m *RegTopicReq) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RegTopicReq.Marshal(b, m, deterministic)
}
func (dst *RegTopicReq) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RegTopicReq.Merge(dst, src)
}
func (m *RegTopicReq) XXX_Size() int {
	return xxx_messageInfo_RegTopicReq.Size(m)
}
func (m *RegTopicReq) XXX_DiscardUnknown() {
	xxx_messageInfo_RegTopicReq.DiscardUnknown(m)
}

var xxx_messageInfo_RegTopicReq proto.InternalMessageInfo

func (m *RegTopicReq) GetTopic() *Topic {
	if m != nil {
		return m.Topic
	}
	return nil
}

type RegTopicResp struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RegTopicResp) Reset()         { *m = RegTopicResp{} }
func (m *RegTopicResp) String() string { return proto.CompactTextString(m) }
func (*RegTopicResp) ProtoMessage()    {}
func (*RegTopicResp) Descriptor() ([]byte, []int) {
	return fileDescriptor_broker_be36a622fce1a135, []int{3}
}
func (m *RegTopicResp) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RegTopicResp.Unmarshal(m, b)
}
func (m *RegTopicResp) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RegTopicResp.Marshal(b, m, deterministic)
}
func (dst *RegTopicResp) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RegTopicResp.Merge(dst, src)
}
func (m *RegTopicResp) XXX_Size() int {
	return xxx_messageInfo_RegTopicResp.Size(m)
}
func (m *RegTopicResp) XXX_DiscardUnknown() {
	xxx_messageInfo_RegTopicResp.DiscardUnknown(m)
}

var xxx_messageInfo_RegTopicResp proto.InternalMessageInfo

type RegSubscriberReq struct {
	Subscriber           *Subscriber `protobuf:"bytes,1,opt,name=subscriber,proto3" json:"subscriber,omitempty"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
	XXX_sizecache        int32       `json:"-"`
}

func (m *RegSubscriberReq) Reset()         { *m = RegSubscriberReq{} }
func (m *RegSubscriberReq) String() string { return proto.CompactTextString(m) }
func (*RegSubscriberReq) ProtoMessage()    {}
func (*RegSubscriberReq) Descriptor() ([]byte, []int) {
	return fileDescriptor_broker_be36a622fce1a135, []int{4}
}
func (m *RegSubscriberReq) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RegSubscriberReq.Unmarshal(m, b)
}
func (m *RegSubscriberReq) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RegSubscriberReq.Marshal(b, m, deterministic)
}
func (dst *RegSubscriberReq) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RegSubscriberReq.Merge(dst, src)
}
func (m *RegSubscriberReq) XXX_Size() int {
	return xxx_messageInfo_RegSubscriberReq.Size(m)
}
func (m *RegSubscriberReq) XXX_DiscardUnknown() {
	xxx_messageInfo_RegSubscriberReq.DiscardUnknown(m)
}

var xxx_messageInfo_RegSubscriberReq proto.InternalMessageInfo

func (m *RegSubscriberReq) GetSubscriber() *Subscriber {
	if m != nil {
		return m.Subscriber
	}
	return nil
}

type RegSubscriberResp struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RegSubscriberResp) Reset()         { *m = RegSubscriberResp{} }
func (m *RegSubscriberResp) String() string { return proto.CompactTextString(m) }
func (*RegSubscriberResp) ProtoMessage()    {}
func (*RegSubscriberResp) Descriptor() ([]byte, []int) {
	return fileDescriptor_broker_be36a622fce1a135, []int{5}
}
func (m *RegSubscriberResp) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RegSubscriberResp.Unmarshal(m, b)
}
func (m *RegSubscriberResp) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RegSubscriberResp.Marshal(b, m, deterministic)
}
func (dst *RegSubscriberResp) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RegSubscriberResp.Merge(dst, src)
}
func (m *RegSubscriberResp) XXX_Size() int {
	return xxx_messageInfo_RegSubscriberResp.Size(m)
}
func (m *RegSubscriberResp) XXX_DiscardUnknown() {
	xxx_messageInfo_RegSubscriberResp.DiscardUnknown(m)
}

var xxx_messageInfo_RegSubscriberResp proto.InternalMessageInfo

type GetTopicReq struct {
	Topic                string   `protobuf:"bytes,1,opt,name=topic,proto3" json:"topic,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *GetTopicReq) Reset()         { *m = GetTopicReq{} }
func (m *GetTopicReq) String() string { return proto.CompactTextString(m) }
func (*GetTopicReq) ProtoMessage()    {}
func (*GetTopicReq) Descriptor() ([]byte, []int) {
	return fileDescriptor_broker_be36a622fce1a135, []int{6}
}
func (m *GetTopicReq) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetTopicReq.Unmarshal(m, b)
}
func (m *GetTopicReq) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetTopicReq.Marshal(b, m, deterministic)
}
func (dst *GetTopicReq) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetTopicReq.Merge(dst, src)
}
func (m *GetTopicReq) XXX_Size() int {
	return xxx_messageInfo_GetTopicReq.Size(m)
}
func (m *GetTopicReq) XXX_DiscardUnknown() {
	xxx_messageInfo_GetTopicReq.DiscardUnknown(m)
}

var xxx_messageInfo_GetTopicReq proto.InternalMessageInfo

func (m *GetTopicReq) GetTopic() string {
	if m != nil {
		return m.Topic
	}
	return ""
}

type GetTopicResp struct {
	Topic                *Topic   `protobuf:"bytes,1,opt,name=topic,proto3" json:"topic,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *GetTopicResp) Reset()         { *m = GetTopicResp{} }
func (m *GetTopicResp) String() string { return proto.CompactTextString(m) }
func (*GetTopicResp) ProtoMessage()    {}
func (*GetTopicResp) Descriptor() ([]byte, []int) {
	return fileDescriptor_broker_be36a622fce1a135, []int{7}
}
func (m *GetTopicResp) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetTopicResp.Unmarshal(m, b)
}
func (m *GetTopicResp) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetTopicResp.Marshal(b, m, deterministic)
}
func (dst *GetTopicResp) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetTopicResp.Merge(dst, src)
}
func (m *GetTopicResp) XXX_Size() int {
	return xxx_messageInfo_GetTopicResp.Size(m)
}
func (m *GetTopicResp) XXX_DiscardUnknown() {
	xxx_messageInfo_GetTopicResp.DiscardUnknown(m)
}

var xxx_messageInfo_GetTopicResp proto.InternalMessageInfo

func (m *GetTopicResp) GetTopic() *Topic {
	if m != nil {
		return m.Topic
	}
	return nil
}

type PubReq struct {
	Topic                string   `protobuf:"bytes,1,opt,name=topic,proto3" json:"topic,omitempty"`
	Msg                  []byte   `protobuf:"bytes,2,opt,name=msg,proto3" json:"msg,omitempty"`
	Priority             uint32   `protobuf:"varint,3,opt,name=priority,proto3" json:"priority,omitempty"`
	DelayMs              uint32   `protobuf:"varint,4,opt,name=delay_ms,json=delayMs,proto3" json:"delay_ms,omitempty"`
	RetryCnt             uint32   `protobuf:"varint,5,opt,name=retry_cnt,json=retryCnt,proto3" json:"retry_cnt,omitempty"`
	BucketId             uint32   `protobuf:"varint,6,opt,name=bucket_id,json=bucketId,proto3" json:"bucket_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PubReq) Reset()         { *m = PubReq{} }
func (m *PubReq) String() string { return proto.CompactTextString(m) }
func (*PubReq) ProtoMessage()    {}
func (*PubReq) Descriptor() ([]byte, []int) {
	return fileDescriptor_broker_be36a622fce1a135, []int{8}
}
func (m *PubReq) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PubReq.Unmarshal(m, b)
}
func (m *PubReq) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PubReq.Marshal(b, m, deterministic)
}
func (dst *PubReq) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PubReq.Merge(dst, src)
}
func (m *PubReq) XXX_Size() int {
	return xxx_messageInfo_PubReq.Size(m)
}
func (m *PubReq) XXX_DiscardUnknown() {
	xxx_messageInfo_PubReq.DiscardUnknown(m)
}

var xxx_messageInfo_PubReq proto.InternalMessageInfo

func (m *PubReq) GetTopic() string {
	if m != nil {
		return m.Topic
	}
	return ""
}

func (m *PubReq) GetMsg() []byte {
	if m != nil {
		return m.Msg
	}
	return nil
}

func (m *PubReq) GetPriority() uint32 {
	if m != nil {
		return m.Priority
	}
	return 0
}

func (m *PubReq) GetDelayMs() uint32 {
	if m != nil {
		return m.DelayMs
	}
	return 0
}

func (m *PubReq) GetRetryCnt() uint32 {
	if m != nil {
		return m.RetryCnt
	}
	return 0
}

func (m *PubReq) GetBucketId() uint32 {
	if m != nil {
		return m.BucketId
	}
	return 0
}

type PubResp struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PubResp) Reset()         { *m = PubResp{} }
func (m *PubResp) String() string { return proto.CompactTextString(m) }
func (*PubResp) ProtoMessage()    {}
func (*PubResp) Descriptor() ([]byte, []int) {
	return fileDescriptor_broker_be36a622fce1a135, []int{9}
}
func (m *PubResp) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PubResp.Unmarshal(m, b)
}
func (m *PubResp) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PubResp.Marshal(b, m, deterministic)
}
func (dst *PubResp) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PubResp.Merge(dst, src)
}
func (m *PubResp) XXX_Size() int {
	return xxx_messageInfo_PubResp.Size(m)
}
func (m *PubResp) XXX_DiscardUnknown() {
	xxx_messageInfo_PubResp.DiscardUnknown(m)
}

var xxx_messageInfo_PubResp proto.InternalMessageInfo

func init() {
	proto.RegisterType((*Topic)(nil), "broker.Topic")
	proto.RegisterType((*Subscriber)(nil), "broker.Subscriber")
	proto.RegisterType((*RegTopicReq)(nil), "broker.RegTopicReq")
	proto.RegisterType((*RegTopicResp)(nil), "broker.RegTopicResp")
	proto.RegisterType((*RegSubscriberReq)(nil), "broker.RegSubscriberReq")
	proto.RegisterType((*RegSubscriberResp)(nil), "broker.RegSubscriberResp")
	proto.RegisterType((*GetTopicReq)(nil), "broker.GetTopicReq")
	proto.RegisterType((*GetTopicResp)(nil), "broker.GetTopicResp")
	proto.RegisterType((*PubReq)(nil), "broker.PubReq")
	proto.RegisterType((*PubResp)(nil), "broker.PubResp")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// BrokerClient is the client API for Broker service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type BrokerClient interface {
	RegTopic(ctx context.Context, in *RegTopicReq, opts ...grpc.CallOption) (*RegTopicResp, error)
	GetTopic(ctx context.Context, in *GetTopicReq, opts ...grpc.CallOption) (*GetTopicResp, error)
	RegSubscriber(ctx context.Context, in *RegSubscriberReq, opts ...grpc.CallOption) (*RegSubscriberResp, error)
	Pub(ctx context.Context, in *PubReq, opts ...grpc.CallOption) (*PubResp, error)
}

type brokerClient struct {
	cc *grpc.ClientConn
}

func NewBrokerClient(cc *grpc.ClientConn) BrokerClient {
	return &brokerClient{cc}
}

func (c *brokerClient) RegTopic(ctx context.Context, in *RegTopicReq, opts ...grpc.CallOption) (*RegTopicResp, error) {
	out := new(RegTopicResp)
	err := c.cc.Invoke(ctx, "/broker.Broker/RegTopic", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *brokerClient) GetTopic(ctx context.Context, in *GetTopicReq, opts ...grpc.CallOption) (*GetTopicResp, error) {
	out := new(GetTopicResp)
	err := c.cc.Invoke(ctx, "/broker.Broker/GetTopic", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *brokerClient) RegSubscriber(ctx context.Context, in *RegSubscriberReq, opts ...grpc.CallOption) (*RegSubscriberResp, error) {
	out := new(RegSubscriberResp)
	err := c.cc.Invoke(ctx, "/broker.Broker/RegSubscriber", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *brokerClient) Pub(ctx context.Context, in *PubReq, opts ...grpc.CallOption) (*PubResp, error) {
	out := new(PubResp)
	err := c.cc.Invoke(ctx, "/broker.Broker/Pub", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BrokerServer is the server API for Broker service.
type BrokerServer interface {
	RegTopic(context.Context, *RegTopicReq) (*RegTopicResp, error)
	GetTopic(context.Context, *GetTopicReq) (*GetTopicResp, error)
	RegSubscriber(context.Context, *RegSubscriberReq) (*RegSubscriberResp, error)
	Pub(context.Context, *PubReq) (*PubResp, error)
}

func RegisterBrokerServer(s *grpc.Server, srv BrokerServer) {
	s.RegisterService(&_Broker_serviceDesc, srv)
}

func _Broker_RegTopic_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RegTopicReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BrokerServer).RegTopic(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/broker.Broker/RegTopic",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BrokerServer).RegTopic(ctx, req.(*RegTopicReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Broker_GetTopic_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetTopicReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BrokerServer).GetTopic(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/broker.Broker/GetTopic",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BrokerServer).GetTopic(ctx, req.(*GetTopicReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Broker_RegSubscriber_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RegSubscriberReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BrokerServer).RegSubscriber(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/broker.Broker/RegSubscriber",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BrokerServer).RegSubscriber(ctx, req.(*RegSubscriberReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Broker_Pub_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PubReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BrokerServer).Pub(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/broker.Broker/Pub",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BrokerServer).Pub(ctx, req.(*PubReq))
	}
	return interceptor(ctx, in, info, handler)
}

var _Broker_serviceDesc = grpc.ServiceDesc{
	ServiceName: "broker.Broker",
	HandlerType: (*BrokerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RegTopic",
			Handler:    _Broker_RegTopic_Handler,
		},
		{
			MethodName: "GetTopic",
			Handler:    _Broker_GetTopic_Handler,
		},
		{
			MethodName: "RegSubscriber",
			Handler:    _Broker_RegSubscriber_Handler,
		},
		{
			MethodName: "Pub",
			Handler:    _Broker_Pub_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/proto/broker.proto",
}

func init() { proto.RegisterFile("pkg/proto/broker.proto", fileDescriptor_broker_be36a622fce1a135) }

var fileDescriptor_broker_be36a622fce1a135 = []byte{
	// 627 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x54, 0x5b, 0x4b, 0x1b, 0x41,
	0x14, 0x76, 0x1b, 0x4d, 0x36, 0x27, 0x89, 0x4d, 0x47, 0x29, 0x6b, 0x7a, 0x21, 0x5d, 0x0b, 0xcd,
	0x83, 0x24, 0x90, 0x58, 0xc4, 0xd7, 0x08, 0x8a, 0x0f, 0x29, 0xb2, 0x16, 0x0a, 0xa5, 0xb0, 0xec,
	0x65, 0x58, 0x17, 0x9d, 0xcc, 0x38, 0x33, 0x4b, 0xcd, 0xaf, 0x29, 0xfd, 0x7f, 0xfd, 0x11, 0x65,
	0xce, 0x6c, 0x92, 0x8d, 0x56, 0xe8, 0xdb, 0x9e, 0xcb, 0x77, 0x6e, 0xdf, 0xb7, 0x03, 0xaf, 0xc5,
	0x6d, 0x36, 0x12, 0x92, 0x6b, 0x3e, 0x8a, 0x25, 0xbf, 0xa5, 0x72, 0x88, 0x06, 0xa9, 0x5b, 0xcb,
	0xff, 0x01, 0x3b, 0x5f, 0xb9, 0xc8, 0x13, 0xb2, 0x0f, 0x3b, 0xda, 0x7c, 0x78, 0x4e, 0xdf, 0x19,
	0x34, 0x03, 0x6b, 0x90, 0x03, 0x70, 0xe7, 0x3c, 0xa5, 0x61, 0x26, 0x85, 0xf7, 0x02, 0x03, 0x0d,
	0x63, 0x5f, 0x48, 0x41, 0x7c, 0xe8, 0xb0, 0xe8, 0x21, 0x64, 0x2a, 0x0b, 0xe3, 0x85, 0xa6, 0xca,
	0xab, 0xf5, 0x9d, 0x41, 0x27, 0x68, 0xb1, 0xe8, 0x61, 0xa6, 0xb2, 0xa9, 0x71, 0xf9, 0xbf, 0x6a,
	0x00, 0xd7, 0x45, 0xac, 0x12, 0x99, 0xc7, 0x54, 0x3e, 0xd3, 0xe3, 0x3d, 0x80, 0x5a, 0xe5, 0x94,
	0x5d, 0x2a, 0x1e, 0xf2, 0x09, 0xba, 0x77, 0x3c, 0x4a, 0x6d, 0x27, 0xce, 0x75, 0x98, 0xa7, 0x65,
	0xaf, 0x8e, 0xf1, 0x9b, 0x66, 0x9c, 0xeb, 0xcb, 0x94, 0x1c, 0x01, 0x49, 0xf8, 0x3c, 0x29, 0x64,
	0x98, 0xf0, 0xb9, 0x2a, 0x18, 0x0d, 0xe7, 0x05, 0xf3, 0xb6, 0x31, 0xb5, 0x6b, 0x23, 0x67, 0x36,
	0xf0, 0xa5, 0x60, 0xe4, 0x02, 0x3e, 0x98, 0xf9, 0x9f, 0x22, 0x42, 0x41, 0x65, 0x18, 0x17, 0xc9,
	0x2d, 0xd5, 0xde, 0x0e, 0x82, 0xdf, 0xb2, 0xe8, 0xe1, 0xec, 0x11, 0xfe, 0x8a, 0xca, 0x29, 0xe6,
	0x90, 0x77, 0x00, 0x66, 0xb4, 0x9f, 0x34, 0xcf, 0x6e, 0xb4, 0x57, 0x47, 0x44, 0x93, 0xa9, 0xec,
	0x1b, 0x3a, 0xc8, 0x1b, 0x68, 0xda, 0xf1, 0x79, 0x4a, 0xbd, 0x06, 0x46, 0x5d, 0x9c, 0x9b, 0xa7,
	0x94, 0xf4, 0xa1, 0xad, 0x74, 0x24, 0x35, 0x2e, 0x97, 0xa7, 0x9e, 0xdb, 0x77, 0x06, 0xdb, 0x01,
	0xa0, 0x6f, 0xa6, 0xb2, 0xcb, 0xd4, 0xc0, 0x73, 0x15, 0x2a, 0x2a, 0xf3, 0xe8, 0xce, 0x6b, 0xf6,
	0x9d, 0x81, 0x1b, 0xb8, 0xb9, 0xba, 0x46, 0x9b, 0xf4, 0xc0, 0x2d, 0x07, 0x97, 0x1e, 0xe0, 0xe1,
	0x56, 0x36, 0xf9, 0x08, 0xbb, 0xe5, 0x7e, 0xb8, 0x18, 0x53, 0x5e, 0x0b, 0x9b, 0xb7, 0xed, 0x32,
	0xc6, 0x39, 0x53, 0xfe, 0x18, 0x5a, 0x01, 0xcd, 0x50, 0x02, 0x01, 0xbd, 0x27, 0x87, 0x55, 0x86,
	0x5a, 0xe3, 0xce, 0xb0, 0x14, 0x8d, 0x4d, 0xb0, 0x31, 0x7f, 0x17, 0xda, 0x6b, 0x8c, 0x12, 0xfe,
	0x39, 0x74, 0x03, 0x9a, 0xad, 0x79, 0x36, 0x85, 0xc6, 0x1b, 0xa4, 0xda, 0x6a, 0x64, 0x59, 0xad,
	0x92, 0x5a, 0xc9, 0xf2, 0xf7, 0xe0, 0xd5, 0xa3, 0x3a, 0x4a, 0xf8, 0x87, 0xd0, 0xba, 0xa0, 0x7a,
	0x35, 0xe0, 0x3f, 0x25, 0xe4, 0x4f, 0xa0, 0xbd, 0x4e, 0x52, 0xe2, 0xff, 0xd6, 0xf8, 0xed, 0x40,
	0xfd, 0xaa, 0x88, 0x9f, 0xad, 0x4a, 0xba, 0x50, 0x63, 0x2a, 0x43, 0x45, 0xb6, 0x03, 0xf3, 0x69,
	0xee, 0x2d, 0x64, 0xce, 0x65, 0xae, 0x17, 0xa5, 0x04, 0x57, 0xb6, 0xf9, 0x55, 0x52, 0x7a, 0x17,
	0x2d, 0xcc, 0xa5, 0xad, 0xe6, 0x1a, 0x68, 0xcf, 0x94, 0xe1, 0x50, 0x52, 0x2d, 0x17, 0x61, 0x32,
	0x5f, 0x4a, 0xca, 0x45, 0xc7, 0xd9, 0x1c, 0xf5, 0x61, 0xc5, 0x66, 0xf8, 0xb7, 0xea, 0x71, 0xad,
	0xe3, 0x32, 0xf5, 0x9b, 0xd0, 0xc0, 0x11, 0x95, 0x18, 0xff, 0x71, 0xa0, 0x3e, 0xc5, 0x35, 0xc8,
	0x09, 0xb8, 0x4b, 0x02, 0xc8, 0xde, 0x72, 0xb7, 0x0a, 0x8d, 0xbd, 0xfd, 0xa7, 0x4e, 0x25, 0xfc,
	0x2d, 0x03, 0x5c, 0xde, 0x69, 0x0d, 0xac, 0x9c, 0x77, 0x0d, 0xac, 0x9e, 0xd3, 0xdf, 0x22, 0xe7,
	0xd0, 0xd9, 0xa0, 0x86, 0x78, 0x95, 0x0e, 0x1b, 0xcc, 0xf7, 0x0e, 0x9e, 0x89, 0x60, 0x9d, 0x01,
	0xd4, 0xae, 0x8a, 0x98, 0xec, 0x2e, 0x73, 0xec, 0xfd, 0x7b, 0x2f, 0x37, 0x6c, 0x93, 0x39, 0x1d,
	0x7e, 0x3f, 0xca, 0x72, 0x7d, 0x53, 0xc4, 0xc3, 0x84, 0xb3, 0xd1, 0xe9, 0xe9, 0xe7, 0xd3, 0xc9,
	0xe4, 0xf8, 0xf8, 0x64, 0x64, 0x4f, 0xc3, 0xee, 0x47, 0xe6, 0x61, 0x93, 0x22, 0x29, 0x9f, 0xb5,
	0xb8, 0x8e, 0xef, 0xda, 0xe4, 0x6f, 0x00, 0x00, 0x00, 0xff, 0xff, 0xc4, 0xd7, 0xab, 0x85, 0xf1,
	0x04, 0x00, 0x00,
}
