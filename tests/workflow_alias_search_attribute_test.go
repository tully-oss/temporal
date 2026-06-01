package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type WorkflowAliasSearchAttributeTestSuite struct {
	parallelsuite.Suite[*WorkflowAliasSearchAttributeTestSuite]
}

func TestWorkflowAliasSearchAttributeTestSuite(t *testing.T) {
	parallelsuite.RunLegacySequential(t, &WorkflowAliasSearchAttributeTestSuite{}) //nolint:staticcheck // SA1019: suite still requires legacy sequential execution
}

func (s *WorkflowAliasSearchAttributeTestSuite) newTestEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	opts = append([]testcore.TestOption{
		testcore.WithWorkerService("worker-deployment version workflows must run for versioned-poller membership checks"),
	}, opts...)

	env := testcore.NewEnv(s.T(), opts...)
	env.SdkWorker().RegisterWorkflow(s.workflowFunc)
	return env
}

func (s *WorkflowAliasSearchAttributeTestSuite) workflowFunc(ctx workflow.Context) (string, error) {
	return "done!", nil
}

func (s *WorkflowAliasSearchAttributeTestSuite) startVersionedPollerAndValidate(
	env *testcore.TestEnv,
	taskQueueName string,
	deploymentName string,
	buildID string,
) {
	ctx := s.Context()
	taskQueue := &taskqueuepb.TaskQueue{
		Name: taskQueueName,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	// Start versioned poller in background
	go func() {
		_, _ = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: taskQueue,
			Identity:  "versioned-poller",
			DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
				DeploymentName:       deploymentName,
				BuildId:              buildID,
				WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
			},
		})
	}()

	// Validate version is present via matching RPC
	version := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: deploymentName,
		BuildId:        buildID,
	}
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.GetTestCluster().MatchingClient().CheckTaskQueueVersionMembership(
			ctx,
			&matchingservice.CheckTaskQueueVersionMembershipRequest{
				NamespaceId:   env.NamespaceID().String(),
				TaskQueue:     taskQueueName,
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				Version:       version,
			},
		)
		a.NoError(err)
		a.True(resp.GetIsMember())
	}, 10*time.Second, 1*time.Second)
}

func (s *WorkflowAliasSearchAttributeTestSuite) createWorkflow(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
	sa *commonpb.SearchAttributes,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	// Start a versioned poller so that the version, which will be set as an override, is present in the task queue.
	s.startVersionedPollerAndValidate(env, tv.TaskQueue().Name, tv.DeploymentSeries(), tv.BuildID())

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          tv.Any().String(),
		Namespace:          env.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Identity:           tv.WorkerIdentity(),
		VersioningOverride: tv.VersioningOverridePinned(),
		SearchAttributes:   sa,
	}
	return env.FrontendClient().StartWorkflowExecution(s.Context(), request)
}

func (s *WorkflowAliasSearchAttributeTestSuite) terminateWorkflow(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
) (*workflowservice.TerminateWorkflowExecutionResponse, error) {
	return env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
		},
		Reason: "terminate reason",
	})
}

func (s *WorkflowAliasSearchAttributeTestSuite) TestWorkflowAliasSearchAttribute() {
	env := s.newTestEnv()
	ctx := s.Context()

	// Overriding the task queue name, deployment name, and build ID so that we don't hit length limitations
	// for any of these in any of the databases.
	tv := testvars.New(s.T()).
		WithTaskQueue("tq-alias-sa").
		WithDeploymentSeries("dep-alias-sa").
		WithBuildID("build-alias-sa")

	_, err := s.createWorkflow(env, tv, nil)
	s.NoError(err)

	s.EventuallyWithT(
		func(t *assert.CollectT) {
			// Filter by WorkflowId to isolate this test's workflow from other tests
			resp, err := env.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("WorkflowId = '%s'", tv.WorkflowID()),
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp.GetExecutions(), 1)

			queriedResp, err := env.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("%s = 'Pinned' AND WorkflowId = '%s'", sadefs.TemporalWorkflowVersioningBehavior, tv.WorkflowID()),
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, queriedResp.GetExecutions(), 1)

			queriedResp, err = env.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("WorkflowVersioningBehavior = 'Pinned' AND WorkflowId = '%s'", tv.WorkflowID()),
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, queriedResp.GetExecutions(), 1)
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	_, err = s.terminateWorkflow(env, tv)
	s.NoError(err)
}

func (s *WorkflowAliasSearchAttributeTestSuite) TestWorkflowAliasSearchAttribute_CustomSearchAttributeOverride() {
	env := s.newTestEnv()
	ctx := s.Context()

	// Overriding the task queue name, deployment name, and build ID so that we don't hit length limitations
	// for any of these in any of the databases.
	tv := testvars.New(s.T()).
		WithTaskQueue("tq-alias-sa-custom").
		WithDeploymentSeries("dep-alias-sa-custom").
		WithBuildID("build-alias-sa-custom")

	_, err := env.SdkClient().OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace: env.Namespace().String(),
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"WorkflowVersioningBehavior": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	s.NoError(err)

	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"WorkflowVersioningBehavior": payload.EncodeString("user-defined"),
		},
	}

	_, err = s.createWorkflow(env, tv, sa)
	s.NoError(err)

	s.EventuallyWithT(
		func(t *assert.CollectT) {
			// Filter by WorkflowId to isolate this test's workflow from other tests
			queriedResp, err := env.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("%s = 'Pinned' AND WorkflowId = '%s'", sadefs.TemporalWorkflowVersioningBehavior, tv.WorkflowID()),
			})
			require.NoError(t, err)
			require.NotNil(t, queriedResp)
			require.Len(t, queriedResp.GetExecutions(), 1)

			queriedResp, err = env.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("WorkflowVersioningBehavior = 'user-defined' AND WorkflowId = '%s'", tv.WorkflowID()),
			})
			require.NoError(t, err)
			require.NotNil(t, queriedResp)
			require.Len(t, queriedResp.GetExecutions(), 1)
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	_, err = s.terminateWorkflow(env, tv)
	s.NoError(err)
}
