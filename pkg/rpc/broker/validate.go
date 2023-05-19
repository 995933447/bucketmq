package broker

import (
	"github.com/995933447/bucketmq/pkg/rpc"
	"github.com/995933447/bucketmq/pkg/rpc/errs"
)

var _ rpc.Validator = (*RegTopicReq)(nil)

func (m *RegTopicReq) Validate() *errs.RPCError {
	if m.Topic == nil {
		return errs.RPCErr(errs.ErrCode_ErrCodeArgumentInvalid, "RegTopicReq.Topic is required")
	}

	if m.Topic.Topic == "" {
		return errs.RPCErr(errs.ErrCode_ErrCodeArgumentInvalid, "RegTopicReq.Topic.Topic is required")
	}

	return nil
}

var _ rpc.Validator = (*RegTopicReq)(nil)

func (m *RegSubscriberReq) Validate() *errs.RPCError {
	if m.Subscriber == nil {
		return errs.RPCErr(errs.ErrCode_ErrCodeArgumentInvalid, "RegSubscriberReq.Subscriber is required")
	}

	if m.Subscriber.Topic == "" {
		return errs.RPCErr(errs.ErrCode_ErrCodeArgumentInvalid, "RegSubscriberReq.Subscriber.Topic is required")
	}

	if m.Subscriber.Subscriber == "" {
		return errs.RPCErr(errs.ErrCode_ErrCodeArgumentInvalid, "RegSubscriberReq.Subscriber.Subscriber is required")
	}

	if m.Subscriber.Consumer == "" {
		return errs.RPCErr(errs.ErrCode_ErrCodeArgumentInvalid, "RegSubscriberReq.Subscriber.Consumer is required")
	}

	if m.Subscriber.LoadMsgBootId == 0 {
		return errs.RPCErr(errs.ErrCode_ErrCodeArgumentInvalid, "RegSubscriberReq.Subscriber.LoadMsgBootId is required")
	}

	if m.Subscriber.LoadMode == 0 {
		return errs.RPCErr(errs.ErrCode_ErrCodeArgumentInvalid, "RegSubscriberReq.Subscriber.LoadMode is required")
	}

	if m.Subscriber.MsgWeight == 0 {
		return errs.RPCErr(errs.ErrCode_ErrCodeArgumentInvalid, "RegSubscriberReq.Subscriber.MsgWeight is required")
	}

	return nil
}
