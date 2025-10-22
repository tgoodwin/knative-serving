package kamera

import (
	"context"
	"fmt"

	// Standard Kubernetes types

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	testing "k8s.io/client-go/testing"
	filteredinformerfactory "knative.dev/pkg/client/injection/kube/informers/factory/filtered"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/reconciler"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	// Knative Serving types and clients
	fakecachingclient "knative.dev/caching/pkg/client/injection/client/fake"
	fakekubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	v1 "knative.dev/serving/pkg/apis/serving/v1"
	autoscalercfg "knative.dev/serving/pkg/autoscaler/config"
	fakeservingclient "knative.dev/serving/pkg/client/injection/client/fake"
	"knative.dev/serving/pkg/gc"
	"knative.dev/serving/pkg/reconciler/route/config"

	"k8s.io/apimachinery/pkg/runtime/schema"
	netcfg "knative.dev/networking/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	// Knative pkg imports
	// Knative controller plumbing
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/system"

	// The actual reconciler implementations
	autoscalingv1alpha1 "knative.dev/serving/pkg/apis/autoscaling/v1alpha1"
	cfgmap "knative.dev/serving/pkg/apis/config"
)

// Ensure KnativeStrategy implements the Strategy interface
var _ tracecheck.Strategy = (*KnativeStrategy)(nil)

// ControllerFactory is a function that creates a new controller.
type ControllerFactory func(ctx context.Context, cmw configmap.Watcher) *controller.Impl

// KnativeStrategy implements the Strategy interface for Knative controllers.
type KnativeStrategy struct {
	factory   ControllerFactory
	recorder  replay.EffectRecorder
	selectors []string
}

// NewKnativeStrategy creates a new KnativeStrategy for a given controller factory.
func NewKnativeStrategy(factory ControllerFactory, recorder replay.EffectRecorder, selectors ...string) (*KnativeStrategy, error) {
	if factory == nil {
		return nil, fmt.Errorf("controller factory cannot be nil")
	}

	// The context passed in from the test should already have fakes injected.

	cmw := configmap.NewStaticWatcher(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfgmap.FeaturesConfigName,
			Namespace: system.Namespace(),
		},
		Data: map[string]string{},
	}, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfgmap.DefaultsConfigName,
			Namespace: system.Namespace(),
		},
		Data: map[string]string{},
	}, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      autoscalercfg.ConfigName,
			Namespace: system.Namespace(),
		},
		Data: map[string]string{},
	},
		// added the following for route reconciler
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      config.DomainConfigName,
				Namespace: system.Namespace(),
			},
			Data: map[string]string{
				"test-domain.dev": "",
				"prod-domain.com": "selector:\n  app: prod",
			},
		}, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      netcfg.ConfigMapName,
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		}, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      gc.ConfigName,
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		}, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cfgmap.FeaturesConfigName,
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		},
		// added for revision informer
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "config-observability",
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "config-deployment",
				Namespace: system.Namespace(),
			},
			Data: map[string]string{
				"queue-sidecar-image": "gcr.io/knative-releases/knative.dev/serving/cmd/queue@sha256:abc123",
			},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "config-logging",
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		},

		// added for certificate reconciler
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "config-certmanager",
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		},
	)

	partial := func(ctx context.Context, _ configmap.Watcher) *controller.Impl {
		return factory(ctx, cmw)
	}

	return &KnativeStrategy{
		factory:   partial,
		selectors: selectors,
		recorder:  recorder,
	}, nil
}

// PrepareState sets up the fake clients and informers for the reconciler under test.
func (ks *KnativeStrategy) PrepareState(ctx context.Context, state []runtime.Object) (context.Context, func(), error) {
	return SetupClientState(ctx, state, ks.selectors...)
}

// newReactor creates a new reactor function that intercepts client actions,
// records them as effects, and uses the provided trackers to fetch object states.
func newReactor(ctx context.Context, recorder replay.EffectRecorder, trackers ...testing.ObjectTracker) testing.ReactionFunc {
	return func(action testing.Action) (handled bool, ret runtime.Object, err error) {
		var obj runtime.Object
		var op event.OperationType

		// lookup iterates through all provided trackers to find the object.
		lookup := func(res schema.GroupVersionResource, ns, name string) (runtime.Object, error) {
			for _, tracker := range trackers {
				obj, err := tracker.Get(res, ns, name)
				if err == nil {
					return obj, nil
				}
			}
			// If we didn't find it in any tracker, return a generic error.
			return nil, fmt.Errorf("object %s/%s not found in any tracker for resource %v", ns, name, res)
		}

		switch action.GetVerb() {
		case "get":
			a := action.(testing.GetAction)
			obj, err = lookup(a.GetResource(), a.GetNamespace(), a.GetName())
			op = event.GET
		case "create":
			a := action.(testing.CreateAction)
			obj = a.GetObject()
			op = event.CREATE
		case "update":
			a := action.(testing.UpdateAction)
			obj = a.GetObject()
			op = event.UPDATE
		case "delete":
			a := action.(testing.DeleteAction)
			obj, err = lookup(a.GetResource(), a.GetNamespace(), a.GetName())
			op = event.MARK_FOR_DELETION
		case "patch":
			a := action.(testing.PatchAction)
			obj, err = lookup(a.GetResource(), a.GetNamespace(), a.GetName())
			op = event.PATCH
		default:
			fmt.Println("WARNING Unhandled action type:", action.GetVerb(), action.GetResource().Resource)
			return false, nil, nil
		}

		if err == nil && obj != nil {
			recorder.RecordEffect(ctx, obj.(client.Object), op, nil)
		}

		// Return false to let the default reactor handle the action.
		return false, nil, err
	}
}

// ReconcileAtState invokes the reconciler for a given state.
func (ks *KnativeStrategy) ReconcileAtState(ctx context.Context, nsName types.NamespacedName) (reconcile.Result, error) {
	servingClient := fakeservingclient.Get(ctx)
	kubeClient := fakekubeclient.Get(ctx)
	cachingClient := fakecachingclient.Get(ctx)

	// Create a reactor and attach it to both clients to intercept and record actions.
	reactor := newReactor(ctx, ks.recorder,
		servingClient.Tracker(),
		kubeClient.Tracker(),
		cachingClient.Tracker())

	// Add the reactor to both clients.
	servingClient.PrependReactor("*", "*", reactor)
	kubeClient.PrependReactor("*", "*", reactor)
	cachingClient.PrependReactor("*", "*", reactor)

	// must re-initialize the controller each time to reset its informer state
	ctrl := ks.factory(ctx, nil)
	if la, ok := ctrl.Reconciler.(reconciler.LeaderAware); ok {
		la.Promote(reconciler.UniversalBucket(), func(reconciler.Bucket, types.NamespacedName) {})
	} else {
		return reconcile.Result{}, fmt.Errorf("Reconciler is not leader-aware")
	}

	key := nsName.String()
	err := ctrl.Reconciler.Reconcile(ctx, key)

	requeue, requeueAfter := controller.IsRequeueKey(err)
	if err != nil && !requeue {
		return reconcile.Result{}, err // Return actual error if it's not a requeue request
	}

	return reconcile.Result{
		Requeue:      requeue,
		RequeueAfter: requeueAfter,
	}, nil
}

// RetrieveEffects extracts the results of a reconciliation.
// func (ks *KnativeStrategy) RetrieveEffects(ctx context.Context) (tracecheck.Changes, error) {
// 	client := fakeservingclient.Get(ctx)
// 	if client == nil {
// 		return tracecheck.Changes{}, fmt.Errorf("fake serving client not found in context")
// 	}
// 	return client.Actions(), nil
// }

func SetupClientState(ctx context.Context, state []runtime.Object, selectors ...string) (context.Context, func(), error) {
	ctx, cancel := context.WithCancel(ctx)
	ctx = filteredinformerfactory.WithSelectors(ctx, selectors...)
	ctx = injection.WithConfig(ctx, &rest.Config{})
	ctx, informers := injection.Fake.SetupInformers(ctx, &rest.Config{})

	if err := insertObjects(ctx, state); err != nil {
		return nil, cancel, err
	}

	waitInformers, err := reconcilertesting.RunAndSyncInformers(ctx, informers...)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("failed to sync informers: %w", err)
	}

	return ctx, func() {
		cancel()
		waitInformers()
	}, nil
}

func insertObjects(ctx context.Context, objs []runtime.Object) error {
	servingclient := fakeservingclient.Get(ctx)
	kubeclient := fakekubeclient.Get(ctx)

	// i am sorry for the following code
	for _, obj := range objs {
		switch o := obj.(type) {
		case *v1.Service:
			if _, err := servingclient.ServingV1().Services(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create service: %w", err)
			}
		case *v1.Route:
			if _, err := servingclient.ServingV1().Routes(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create route: %w", err)
			}
		case *v1.Configuration:
			if _, err := servingclient.ServingV1().Configurations(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create configuration: %w", err)
			}
		case *v1.Revision:
			if _, err := servingclient.ServingV1().Revisions(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create revision: %w", err)
			}
		case *autoscalingv1alpha1.PodAutoscaler:
			if _, err := servingclient.AutoscalingV1alpha1().PodAutoscalers(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create podautoscaler: %w", err)
			}
		case *autoscalingv1alpha1.Metric:
			if _, err := servingclient.AutoscalingV1alpha1().Metrics(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create metric: %w", err)
			}
		case *corev1.ConfigMap:
			if _, err := kubeclient.CoreV1().ConfigMaps(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create configmap: %w", err)
			}
		case *corev1.Secret:
			if _, err := kubeclient.CoreV1().Secrets(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create secret: %w", err)
			}
		case *corev1.ServiceAccount:
			if _, err := kubeclient.CoreV1().ServiceAccounts(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create serviceaccount: %w", err)
			}
		case *corev1.Pod:
			if _, err := kubeclient.CoreV1().Pods(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create pod: %w", err)
			}
		case *corev1.Endpoints:
			if _, err := kubeclient.CoreV1().Endpoints(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create endpoints: %w", err)
			}
		case *corev1.Service:
			if _, err := kubeclient.CoreV1().Services(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create service: %w", err)
			}
		case *appsv1.Deployment:
			if _, err := kubeclient.AppsV1().Deployments(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create deployment: %w", err)
			}
		default:
			panic(fmt.Sprintf("unsupported type %T -- need to add a case for it", o))
		}
	}
	return nil

}
