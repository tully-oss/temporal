package nexusoperation

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/headers"
)

// principalFromChasmContext reads the immediate-caller principal from the
// chasm context's incoming gRPC metadata. The auth interceptor sets these
// headers on the incoming context after computing the principal from
// authenticated material; StripPrincipal also runs there to drop any
// principal headers an external caller tried to inject. Reading from
// RequestHeader is therefore a server-trusted source.
//
// Returns nil if no principal-bearing header is present (e.g. authorizer
// did not populate one, or this is an OSS deployment without principal
// derivation wired in).
func principalFromChasmContext(ctx chasm.Context) *commonpb.Principal {
	typ := ctx.RequestHeader(headers.PrincipalTypeHeaderName)
	name := ctx.RequestHeader(headers.PrincipalNameHeaderName)
	account := ctx.RequestHeader(headers.PrincipalAccountHeaderName)
	if typ == "" && name == "" && account == "" {
		return nil
	}
	return &commonpb.Principal{Type: typ, Name: name, Account: account}
}

// endUserPrincipalFromChasmContext reads the end-user caller principal from
// the chasm context's incoming gRPC metadata. Distinct from the immediate
// caller principal — see common/headers.SetEndUserPrincipal for the
// semantic difference.
//
//nolint:unused // Used by workflow-initiated Nexus operations once the
// bridging from MutableState to chasm RequestData is wired up.
func endUserPrincipalFromChasmContext(ctx chasm.Context) *commonpb.Principal {
	typ := ctx.RequestHeader(headers.EndUserPrincipalTypeHeaderName)
	name := ctx.RequestHeader(headers.EndUserPrincipalNameHeaderName)
	account := ctx.RequestHeader(headers.EndUserPrincipalAccountHeaderName)
	if typ == "" && name == "" && account == "" {
		return nil
	}
	return &commonpb.Principal{Type: typ, Name: name, Account: account}
}

// attachPrincipalHeaders sets the immediate-caller and end-user principal
// headers on an outbound Nexus operation header map. Headers are emitted
// only for non-nil principals; absent headers downstream signal "no
// principal in this position" rather than an empty string.
//
// The handler-side server strips any inbound principal headers from
// external callers (StripPrincipal / StripPrincipalHTTP) and re-derives
// from authenticated material, so the only path by which these headers
// reach the handler authorizer is via our server-to-server dispatch here.
func attachPrincipalHeaders(h nexus.Header, serviceCaller, endUserCaller *commonpb.Principal) {
	if serviceCaller != nil {
		if t := serviceCaller.GetType(); t != "" {
			h.Set(headers.PrincipalTypeHeaderName, t)
		}
		if n := serviceCaller.GetName(); n != "" {
			h.Set(headers.PrincipalNameHeaderName, n)
		}
		if a := serviceCaller.GetAccount(); a != "" {
			h.Set(headers.PrincipalAccountHeaderName, a)
		}
	}
	if endUserCaller != nil {
		if t := endUserCaller.GetType(); t != "" {
			h.Set(headers.EndUserPrincipalTypeHeaderName, t)
		}
		if n := endUserCaller.GetName(); n != "" {
			h.Set(headers.EndUserPrincipalNameHeaderName, n)
		}
		if a := endUserCaller.GetAccount(); a != "" {
			h.Set(headers.EndUserPrincipalAccountHeaderName, a)
		}
	}
}
