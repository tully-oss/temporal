package nexusoperation

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/headers"
)

func TestAttachPrincipalHeaders_BothPrincipals(t *testing.T) {
	t.Parallel()

	h := nexus.Header{}
	caller := &commonpb.Principal{Type: "service-accounts", Name: "sa-worker", Account: "12345"}
	endUser := &commonpb.Principal{Type: "users", Name: "alice@example.com", Account: "12345"}

	attachPrincipalHeaders(h, caller, endUser)

	require.Equal(t, "service-accounts", h.Get(headers.PrincipalTypeHeaderName))
	require.Equal(t, "sa-worker", h.Get(headers.PrincipalNameHeaderName))
	require.Equal(t, "12345", h.Get(headers.PrincipalAccountHeaderName))
	require.Equal(t, "users", h.Get(headers.EndUserPrincipalTypeHeaderName))
	require.Equal(t, "alice@example.com", h.Get(headers.EndUserPrincipalNameHeaderName))
	require.Equal(t, "12345", h.Get(headers.EndUserPrincipalAccountHeaderName))
}

func TestAttachPrincipalHeaders_NilPrincipalsOmitted(t *testing.T) {
	t.Parallel()

	h := nexus.Header{}
	attachPrincipalHeaders(h, nil, nil)

	require.Empty(t, h.Get(headers.PrincipalTypeHeaderName))
	require.Empty(t, h.Get(headers.EndUserPrincipalTypeHeaderName))
}

func TestAttachPrincipalHeaders_StandaloneSingleHop(t *testing.T) {
	t.Parallel()

	// Standalone Nexus operations: the SDK client is both the service
	// caller and the end-user. Same principal in both positions.
	h := nexus.Header{}
	p := &commonpb.Principal{Type: "users", Name: "alice", Account: "12345"}

	attachPrincipalHeaders(h, p, p)

	require.Equal(t, "users", h.Get(headers.PrincipalTypeHeaderName))
	require.Equal(t, "alice", h.Get(headers.PrincipalNameHeaderName))
	require.Equal(t, "users", h.Get(headers.EndUserPrincipalTypeHeaderName))
	require.Equal(t, "alice", h.Get(headers.EndUserPrincipalNameHeaderName))
}

func TestAttachPrincipalHeaders_EmptyAccountOmittedNotSentAsBlank(t *testing.T) {
	t.Parallel()

	// OSS principals with no account scoping should not emit a blank
	// account header — handler-side parsing treats absent and empty
	// differently in some downstream paths.
	h := nexus.Header{}
	p := &commonpb.Principal{Type: "jwt", Name: "sub-12345"}

	attachPrincipalHeaders(h, p, nil)

	require.Equal(t, "jwt", h.Get(headers.PrincipalTypeHeaderName))
	require.Equal(t, "sub-12345", h.Get(headers.PrincipalNameHeaderName))
	require.Empty(t, h.Get(headers.PrincipalAccountHeaderName))
}

func TestAttachPrincipalHeaders_OnlyServiceCallerPresent(t *testing.T) {
	t.Parallel()

	// Workflow predates RootCallerPrincipal: end-user is nil, service
	// caller still flows.
	h := nexus.Header{}
	caller := &commonpb.Principal{Type: "service-accounts", Name: "sa-worker", Account: "12345"}

	attachPrincipalHeaders(h, caller, nil)

	require.Equal(t, "sa-worker", h.Get(headers.PrincipalNameHeaderName))
	require.Empty(t, h.Get(headers.EndUserPrincipalNameHeaderName))
}
