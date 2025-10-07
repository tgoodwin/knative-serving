package kamera

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	filteredinformerfactory "knative.dev/pkg/client/injection/kube/informers/factory/filtered"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/ptr"

	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	// Fake informers for reconcilers
	// The actual reconciler implementations
	// Import the fakes for the informers we need.
	// Import the fakes for the informers we need.
	_ "knative.dev/caching/pkg/client/injection/informers/caching/v1alpha1/image/fake"
	_ "knative.dev/networking/pkg/client/injection/informers/networking/v1alpha1/certificate/fake"
	_ "knative.dev/networking/pkg/client/injection/informers/networking/v1alpha1/ingress/fake"
	_ "knative.dev/networking/pkg/client/injection/informers/networking/v1alpha1/serverlessservice/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/endpoints/fake"

	// for kpa reconciler
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/filtered/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/factory/filtered/fake"

	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/service/fake"
	_ "knative.dev/pkg/injection/clients/dynamicclient/fake"

	"knative.dev/serving/pkg/apis/autoscaling"
	// for cert-manager

	"knative.dev/serving/pkg/apis/serving"
	v1 "knative.dev/serving/pkg/apis/serving/v1"
	"knative.dev/serving/pkg/autoscaler/scaling"

	// these have to be here to initialize the fake informers
	_ "knative.dev/serving/pkg/client/certmanager/injection/informers/acme/v1/challenge/fake"
	_ "knative.dev/serving/pkg/client/certmanager/injection/informers/certmanager/v1/certificate/fake"
	_ "knative.dev/serving/pkg/client/certmanager/injection/informers/certmanager/v1/clusterissuer/fake"
	_ "knative.dev/serving/pkg/client/injection/ducks/autoscaling/v1alpha1/podscalable/fake"
	_ "knative.dev/serving/pkg/client/injection/informers/autoscaling/v1alpha1/metric/fake"
	_ "knative.dev/serving/pkg/client/injection/informers/autoscaling/v1alpha1/podautoscaler/fake"
	_ "knative.dev/serving/pkg/client/injection/informers/serving/v1/configuration/fake"
	_ "knative.dev/serving/pkg/client/injection/informers/serving/v1/revision/fake"
	_ "knative.dev/serving/pkg/client/injection/informers/serving/v1/route/fake"
	_ "knative.dev/serving/pkg/client/injection/informers/serving/v1/service/fake"
	"knative.dev/serving/pkg/reconciler/revision/resources"
	"knative.dev/serving/pkg/reconciler/revision/resources/names"

	client "sigs.k8s.io/controller-runtime/pkg/client"

	event "github.com/tgoodwin/kamera/pkg/event"
	replay "github.com/tgoodwin/kamera/pkg/replay"

	kpareconciler "knative.dev/serving/pkg/reconciler/autoscaling/kpa"
	certreconciler "knative.dev/serving/pkg/reconciler/certificate"
	configurationreconciler "knative.dev/serving/pkg/reconciler/configuration"
	revisionreconciler "knative.dev/serving/pkg/reconciler/revision"
	routereconciler "knative.dev/serving/pkg/reconciler/route"
	sksreconciler "knative.dev/serving/pkg/reconciler/serverlessservice"
	servicereconciler "knative.dev/serving/pkg/reconciler/service"

	"context"
)

type recordedEffect struct {
	Object client.Object
	OpType event.OperationType
}

type FakeRecorder struct {
	effects []recordedEffect
}

var _ replay.EffectRecorder = (*FakeRecorder)(nil)

func (r *FakeRecorder) RecordEffect(ctx context.Context, obj client.Object, opType event.OperationType, precondition *replay.PreconditionInfo) error {
	r.effects = append(r.effects, recordedEffect{
		Object: obj,
		OpType: opType,
	})
	return nil
}

func TestNewKnativeStrategy(t *testing.T) {
	tests := []struct {
		name    string
		factory ControllerFactory
	}{
		{
			name:    "Service Reconciler",
			factory: servicereconciler.NewController,
		},
		{
			name:    "Route Reconciler",
			factory: routereconciler.NewController,
		},
		{
			name:    "Revision Reconciler",
			factory: revisionreconciler.NewController,
		},
		{
			name: "KPA Reconciler",
			factory: func(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
				multiScaler := scaling.NewMultiScaler(ctx.Done(), nil, logging.FromContext(ctx))
				return kpareconciler.NewController(ctx, cmw, multiScaler)
			},
		},
		{
			name:    "SKS Reconciler",
			factory: sksreconciler.NewController,
		},
		{
			name:    "Certificate Reconciler",
			factory: certreconciler.NewController,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When a fake filtered informer is imported, the testing framework expects
			// the context to be initialized with the selector key.
			// For tests that don't need a filtered informer, we can pass an empty selector list.
			selectors := []string{}
			if tt.name == "KPA Reconciler" {
				selectors = append(selectors, serving.RevisionUID)
			}
			reconcilertesting.SetupFakeContext(t, func(ctx context.Context) context.Context {
				// callback ensures that we add the required selectors before SetupFakeContext proceeds.
				return filteredinformerfactory.WithSelectors(ctx, selectors...)
			})
			_, err := NewKnativeStrategy(tt.factory, &FakeRecorder{}, selectors...)
			if err != nil {
				t.Errorf("NewKnativeStrategy() error = %v", err)
			}
		})
	}
}

func TestKnativeStrategyReconciliation(t *testing.T) {
	ns, name := "default", "test-resource"
	key := types.NamespacedName{Namespace: ns, Name: name}

	t.Run("Service Reconciler", func(t *testing.T) {
		svc := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: v1.ServiceSpec{
				ConfigurationSpec: v1.ConfigurationSpec{
					Template: v1.RevisionTemplateSpec{
						Spec: v1.RevisionSpec{
							PodSpec: corev1.PodSpec{
								Containers: []corev1.Container{{Image: "gcr.io/knative-samples/helloworld-go"}},
							},
						},
					},
				},
				RouteSpec: v1.RouteSpec{
					Traffic: []v1.TrafficTarget{{
						LatestRevision: ptr.Bool(true),
						Percent:        ptr.Int64(100),
					}},
				},
			},
		}

		initialState := []runtime.Object{svc}
		ctx := logtesting.TestContextWithLogger(t)

		strategy, err := NewKnativeStrategy(servicereconciler.NewController, &FakeRecorder{})
		if err != nil {
			t.Fatalf("NewKnativeStrategy() error = %v", err)
		}
		ctx, cancel, err := strategy.PrepareState(ctx, initialState)
		if err != nil {
			t.Fatalf("SetupClientState() error = %v", err)
		}
		t.Cleanup(cancel)
		if _, err := strategy.ReconcileAtState(ctx, key); err != nil {
			t.Fatalf("ReconcileAtState() error = %v", err)
		}

		assertCreates(t, ctx, strategy, "Configuration", "Route")
	})

	t.Run("Revision Reconciler", func(t *testing.T) {
		initialState := []runtime.Object{
			&v1.Revision{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec: v1.RevisionSpec{
					PodSpec: corev1.PodSpec{
						Containers: []corev1.Container{{Image: "test"}},
					},
				},
				Status: v1.RevisionStatus{
					Status: duckv1.Status{
						Conditions: duckv1.Conditions{{
							Type:   "Ready",
							Status: "Unknown",
						}},
					},
					ContainerStatuses: []v1.ContainerStatus{{
						Name:        "user-container",
						ImageDigest: "test@sha256:abcd",
					}},
				},
			},
		}

		strategy, err := NewKnativeStrategy(revisionreconciler.NewController, &FakeRecorder{})
		if err != nil {
			t.Fatalf("NewKnativeStrategy() error = %v", err)
		}
		ctx, cancel, err := strategy.PrepareState(logtesting.TestContextWithLogger(t), initialState)
		if err != nil {
			t.Fatalf("SetupClientState() error = %v", err)
		}
		t.Cleanup(cancel)
		if _, err := strategy.ReconcileAtState(ctx, key); err != nil {
			t.Fatalf("ReconcileAtState() error = %v", err)
		}

		// this just updated a revision -- did not create anything new
		assertCreates(t, ctx, strategy, "PodAutoscaler")
	})

	t.Run("KPA Reconciler", func(t *testing.T) {
		// The KPA reconciler needs a Revision and a Deployment to exist
		// to know what to scale.
		rev := &v1.Revision{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: v1.RevisionSpec{
				PodSpec: corev1.PodSpec{Containers: []corev1.Container{{Image: "test"}}},
			},
		}
		// The PA's ScaleTargetRef points to this deployment.
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      names.Deployment(rev),
				Namespace: ns,
			},
		}

		pa := resources.MakePA(rev, nil)
		pa.Annotations[autoscaling.ClassAnnotationKey] = autoscaling.KPA

		initialState := []runtime.Object{rev, dep, pa}

		factory := func(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
			multiScaler := scaling.NewMultiScaler(ctx.Done(), nil, logging.FromContext(ctx))
			return kpareconciler.NewController(ctx, cmw, multiScaler)
		}
		strategy, err := NewKnativeStrategy(factory, &FakeRecorder{}, serving.RevisionUID)
		if err != nil {
			t.Fatalf("NewKnativeStrategy() error = %v", err)
		}

		ctx, cancel, err := strategy.PrepareState(logtesting.TestContextWithLogger(t), initialState)
		if err != nil {
			t.Fatalf("SetupClientState() error = %v", err)
		}

		t.Cleanup(cancel)
		if _, err := strategy.ReconcileAtState(ctx, types.NamespacedName{Namespace: ns, Name: pa.Name}); err != nil {
			t.Fatalf("ReconcileAtState() error = %v", err)
		}

		assertActions(t, ctx, strategy, "update", "PodAutoscaler")
	})

	t.Run("Configuration Reconciler", func(t *testing.T) {
		initialState := []runtime.Object{
			&v1.Configuration{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec: v1.ConfigurationSpec{Template: v1.RevisionTemplateSpec{
					Spec: v1.RevisionSpec{PodSpec: corev1.PodSpec{
						Containers: []corev1.Container{{Image: "test"}},
					}}}},
			},
		}
		strategy, err := NewKnativeStrategy(configurationreconciler.NewController, &FakeRecorder{})
		if err != nil {
			t.Fatalf("NewKnativeStrategy() error = %v", err)
		}
		ctx, cancel, err := strategy.PrepareState(logtesting.TestContextWithLogger(t), initialState)
		if err != nil {
			t.Fatalf("SetupClientState() error = %v", err)
		}
		t.Cleanup(cancel)
		if _, err := strategy.ReconcileAtState(ctx, key); err != nil {
			t.Fatalf("ReconcileAtState() error = %v", err)
		}

		assertCreates(t, ctx, strategy, "Revision")
	})
}

// assertCreates is a test helper to run a reconciliation and assert that certain resources were created.
func assertCreates(t *testing.T, ctx context.Context, strategy *KnativeStrategy, wantCreates ...string) {
	t.Helper()
	assertActions(t, ctx, strategy, "create", wantCreates...)
}

// assertActions is a test helper to run a reconciliation and assert that resources of specific types were acted upon with a given verb.
func assertActions(t *testing.T, ctx context.Context, strategy *KnativeStrategy, verb string, wantResources ...string) {
	t.Helper()

	recorder, ok := strategy.recorder.(*FakeRecorder)
	if !ok {
		t.Fatalf("KnativeStrategy.recorder is not of type *FakeRecorder")
	}
	actions := recorder.effects

	got := make(map[string]bool)
	for _, action := range actions {
		// The verb from the reactor is now an event.OperationType, so we compare it as a string.
		fmt.Printf("Found %s action on resource %T with actionType %q\n", verb, action.Object, action.OpType)
		if string(action.OpType) == strings.ToUpper(verb) {
			// Using reflection is more reliable than GetObjectKind() which may not be populated.
			// We get the type of the object, which will be a pointer (e.g., *v1.Configuration).
			// We then get the element type to get the struct name (e.g., v1.Configuration).
			// Finally, we take the name of that struct (e.g., "Configuration").
			kind := reflect.TypeOf(action.Object).Elem().Name()
			fmt.Printf("  -> matches %s action on resource of type %q\n", verb, kind)
			got[kind] = true
		}
	}

	for _, want := range wantResources {
		if !got[want] {
			t.Errorf("Expected a '%s' action on a resource of type %q, but it wasn't found.", verb, want)
		}
	}
}
