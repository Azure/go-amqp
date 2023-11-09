package amqp

import (
	"context"
	"fmt"

	"github.com/Azure/go-amqp/internal/encoding"
)

// TransactionControllerOptions contains the optional settings for configuring a [TransactionController].
type TransactionControllerOptions struct {
	// LocalTransactions requests support for local transactions.
	// Corresponds to the `amqp:local-transactions` txn-capability.
	LocalTransactions bool

	// DistributedTransactions requests support for AMQP Distributed Transactions.
	// Corresponds to the `amqp:distributed-transactions` txn-capability.
	DistributedTransactions bool

	// PromotableTransactions requests support for AMQP Promotable Transactions.
	// Corresponds to the `amqp:promotable-transactions` txn-capability.
	PromotableTransactions bool

	// MultipleTransactionsPerSession requests support for multiple active transactions on a single session.
	// Corresponds to the `amqp:multi-txns-per-ssn` txn-capability.
	MultipleTransactionsPerSession bool

	// MultiSessionsPerTransaction requests support for transactions whose txn-id is used across sessions on one connection.
	// Corresponds to the `amqp:multi-ssns-per-txn` txn-capability.
	MultiSessionsPerTransaction bool
}

// TransactionController can interact with the transaction coordinator
// for a transactional resource.
// Reference: http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html#section-coordination
type TransactionController struct {
	sender *Sender
}

// DischargeOptions contains the optional parameters for the [TransactionController.Discharge] method.
// DischargeOptions contains the optional parameters for the [Client.Discharge] method.
type DischargeOptions struct {
	// placeholder for future optional parameters
}

// Discharge discharges a transaction, either committing it or rolling it back based on
// the values set in the discharge parameter.
//
// Spec: http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html#type-discharge
func (tc *TransactionController) Discharge(ctx context.Context, discharge TransactionDischarge, opts *DischargeOptions) error {
	return tc.sender.Send(ctx, &Message{
		Value: discharge,
	}, nil)
}

// DeclareOptions contains the optional parameters for the [Client.Declare] method.
type DeclareOptions struct {
	// placeholder for future optional parameters
}

// Declare declares a transaction.
// Returns a transaction ID, if successful, or an error otherwise.
//
// Spec: http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html#section-txn-declare
func (tc *TransactionController) Declare(ctx context.Context, declare TransactionDeclare, opts *DeclareOptions) ([]byte, error) {
	state, err := tc.sender.sendRaw(ctx, &Message{
		Value: declare,
	}, nil)

	if err != nil {
		return nil, err
	}

	declared, ok := state.(*encoding.StateDeclared)

	if !ok {
		return nil, fmt.Errorf("invalid response when declaring transaction (not *StateDeclared, was %T)", state)
	}

	return declared.TransactionID, nil
}

// Close closes the AMQP link for this transaction controller.
func (tc *TransactionController) Close(ctx context.Context) error {
	return tc.sender.Close(ctx)
}
