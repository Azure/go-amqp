package amqp

import (
	"github.com/Azure/go-amqp/internal/buffer"
	"github.com/Azure/go-amqp/internal/encoding"
)

// TransactionCapability represents capabilities for a transaction coordinator.
type TransactionCapability encoding.Symbol

const (
	// LocalTransactionCapability indicates the transaction coordinator supports local Transactions.
	LocalTransactionCapability TransactionCapability = "amqp:local-transactions"

	// DistributedTransactionCapability indicates the transaction coordinator supports AMQP Distributed Transactions.
	DistributedTransactionCapability TransactionCapability = "amqp:distributed-transactions"

	// PromotableTransactionCapability indicates the transaction coordinator supports AMQP Promotable Transactions.
	PromotableTransactionCapability TransactionCapability = "amqp:promotable-transactions"

	// MultiTxnPerSessionTransactionCapability indicates the transaction coordinator supports multiple active transactions on a single session.
	MultiTxnPerSessionTransactionCapability TransactionCapability = "amqp:multi-txns-per-ssn"
)

// TransactionDeclare contains values within the Declare message.
type TransactionDeclare struct {
	// Maps to the global-id field of the Declare message.
	GlobalID []byte
}

func (t TransactionDeclare) Marshal(wr *buffer.Buffer) error {
	// TODO: add to typecode
	return encoding.MarshalComposite(wr, encoding.TypeCodeTransactionDeclare, []encoding.MarshalField{
		{Value: t.GlobalID, Omit: t.GlobalID == nil},
	})
}

type TransactionDischarge struct {
	TransactionID []byte

	// Fail indicates that the work associated with this transaction has failed, and the controller
	// wishes the transaction to be rolled back. If the transaction is associated with a global-id
	// this will render the global transaction rollback-only. If the transaction is a local transaction,
	// then this flag controls whether the transaction is committed or aborted when it is discharged.
	// (Note that the specification for distributed transactions within AMQP 1.0 will be provided
	// separately in Part 6 Distributed Transactions).
	Fail bool
}

func (t TransactionDischarge) Marshal(wr *buffer.Buffer) error {
	return encoding.MarshalComposite(wr, encoding.TypeCodeTransactionDischarge, []encoding.MarshalField{
		{Value: t.TransactionID},
		{Value: t.Fail},
	})
}
