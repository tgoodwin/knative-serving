package kamera

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	kamerascheme "knative.dev/serving/kamera/scheme"

	// Knative Serving types and clients
	cachingv1alpha1 "knative.dev/caching/pkg/apis/caching/v1alpha1"
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
	log "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	// Knative pkg imports
	// Knative controller plumbing
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/system"

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
	logger    logr.Logger
}

// NewKnativeStrategy creates a new KnativeStrategy for a given controller factory.
func NewKnativeStrategy(factory ControllerFactory, recorder replay.EffectRecorder, selectors ...string) (*KnativeStrategy, error) {
	if factory == nil {
		return nil, fmt.Errorf("controller factory cannot be nil")
	}

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
		logger:    log.Log.WithName("knative-strategy"),
	}, nil
}

// SetLogger overrides the default logger used by the strategy. Call this before PrepareState.
func (ks *KnativeStrategy) SetLogger(logger logr.Logger) {
	if logger.GetSink() == nil {
		return
	}
	ks.logger = logger
}

// PrepareState sets up the fake clients and informers for the reconciler under test.
func (ks *KnativeStrategy) PrepareState(ctx context.Context, state []runtime.Object) (context.Context, func(), error) {
	ctx = log.IntoContext(ctx, ks.logger)
	fmt.Println("setting up client state")
	ctx, cancel, err := SetupClientState(ctx, state, ks.selectors...)
	if err != nil {
		return nil, cancel, err
	}
	ctx = log.IntoContext(ctx, ks.logger)
	return ctx, cancel, nil
}

// newReactor creates a new reactor function that intercepts client actions,
// records them as effects, and uses the provided trackers to fetch object states.
func newReactor(ctx context.Context, recorder replay.EffectRecorder, trackers ...testing.ObjectTracker) testing.ReactionFunc {
	baseLogger := log.FromContext(ctx).WithName("fake-reactor")

	return func(action testing.Action) (handled bool, ret runtime.Object, err error) {
		var obj runtime.Object
		var op event.OperationType
		logger := baseLogger.WithValues(
			"verb", action.GetVerb(),
			"resource", action.GetResource().Resource,
			"namespace", action.GetNamespace(),
		)

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
		case "list":
			a := action.(testing.ListAction)
			gvr := a.GetResource()
			gvk := schema.GroupVersionKind{
				Group:   gvr.Group,
				Version: gvr.Version,
				Kind:    listKindForResource(gvr.Resource),
			}
			ul := &unstructured.Unstructured{}
			ul.SetGroupVersionKind(gvk)
			obj = ul
			op = event.LIST
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
			logger.V(1).Info("unhandled action type")
			return false, nil, nil
		}

		if err == nil && obj != nil {
			if co, ok := obj.(client.Object); ok {
				ensureGVK(co)
				logger.V(1).Info("recording effect",
					"operation", op,
					"name", co.GetName(),
					"kind", co.GetObjectKind().GroupVersionKind().Kind,
				)
				recorder.RecordEffect(ctx, co, op, nil)
			} else {
				logger.V(1).Info("object does not implement client.Object", "operation", op, "type", fmt.Sprintf("%T", obj))
			}
		} else if err != nil {
			logger.V(1).Info("failed to resolve object for action", "error", err)
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

	logger := log.FromContext(ctx).WithName("reconcile").WithValues("key", nsName.String())
	fmt.Println("reconciling at state for", nsName.String())

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
	logger = logger.WithValues("reconciler", ctrl.Name)
	if la, ok := ctrl.Reconciler.(reconciler.LeaderAware); ok {
		la.Promote(reconciler.UniversalBucket(), func(reconciler.Bucket, types.NamespacedName) {})
	} else {
		logger.Error(fmt.Errorf("not leader-aware"), "reconcile aborted")
		return reconcile.Result{}, fmt.Errorf("Reconciler is not leader-aware")
	}

	key := nsName.String()
	err := ctrl.Reconciler.Reconcile(ctx, key)

	requeue, requeueAfter := controller.IsRequeueKey(err)
	if err != nil && !requeue {
		logger.Error(err, "reconcile failed")
		return reconcile.Result{}, err // Return actual error if it's not a requeue request
	}

	result := reconcile.Result{
		Requeue:      requeue,
		RequeueAfter: requeueAfter,
	}

	logger.Info("reconcile completed", "requeue", result.Requeue, "requeueAfter", result.RequeueAfter)
	return result, nil
}

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
	cachingclient := fakecachingclient.Get(ctx)

	// i am sorry for the following code
	for _, obj := range objs {
		if u, ok := obj.(*unstructured.Unstructured); ok {
			typed, err := convertUnstructured(u)
			if err != nil {
				return fmt.Errorf("failed to convert unstructured %s: %w", u.GroupVersionKind().String(), err)
			}
			obj = typed
		}

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
		case *cachingv1alpha1.Image:
			if _, err := cachingclient.CachingV1alpha1().Images(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create image: %w", err)
			}
		default:
			return fmt.Errorf("unsupported type %T", o)
		}
	}
	return nil

}

var kindToGVK = map[string]schema.GroupVersionKind{
	"Deployment":    appsv1.SchemeGroupVersion.WithKind("Deployment"),
	"Image":         cachingv1alpha1.SchemeGroupVersion.WithKind("Image"),
	"PodAutoscaler": autoscalingv1alpha1.SchemeGroupVersion.WithKind("PodAutoscaler"),
	"Metric":        autoscalingv1alpha1.SchemeGroupVersion.WithKind("Metric"),
	"Configuration": v1.SchemeGroupVersion.WithKind("Configuration"),
	"Revision":      v1.SchemeGroupVersion.WithKind("Revision"),
	"Route":         v1.SchemeGroupVersion.WithKind("Route"),
	"Service":       v1.SchemeGroupVersion.WithKind("Service"),
}

var resourceToListKind = map[string]string{
	"deployments":     "DeploymentList",
	"images":          "ImageList",
	"podautoscalers":  "PodAutoscalerList",
	"metrics":         "MetricList",
	"configurations":  "ConfigurationList",
	"revisions":       "RevisionList",
	"routes":          "RouteList",
	"services":        "ServiceList",
	"pods":            "PodList",
	"endpoints":       "EndpointsList",
	"configmaps":      "ConfigMapList",
	"secrets":         "SecretList",
	"serviceaccounts": "ServiceAccountList",
}

func ensureGVK(obj client.Object) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind != "" && gvk.Version != "" {
		return
	}
	switch o := obj.(type) {
	case *corev1.ConfigMap:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ConfigMap"))
	case *corev1.Secret:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	case *corev1.ServiceAccount:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ServiceAccount"))
	case *corev1.Pod:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
	case *corev1.Endpoints:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Endpoints"))
	case *corev1.Service:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	case *appsv1.Deployment:
		o.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
	case *v1.Service:
		o.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Service"))
	case *v1.Route:
		o.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Route"))
	case *v1.Configuration:
		o.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Configuration"))
	case *v1.Revision:
		o.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Revision"))
	case *autoscalingv1alpha1.PodAutoscaler:
		o.SetGroupVersionKind(autoscalingv1alpha1.SchemeGroupVersion.WithKind("PodAutoscaler"))
	case *autoscalingv1alpha1.Metric:
		o.SetGroupVersionKind(autoscalingv1alpha1.SchemeGroupVersion.WithKind("Metric"))
	case *cachingv1alpha1.Image:
		o.SetGroupVersionKind(cachingv1alpha1.SchemeGroupVersion.WithKind("Image"))
	}
}

func convertUnstructured(u *unstructured.Unstructured) (runtime.Object, error) {
	gvk := u.GroupVersionKind()
	if (gvk.Group == "" || gvk.Kind == "") && u.GetKind() != "" {
		if mapped, ok := kindToGVK[u.GetKind()]; ok {
			gvk = mapped
		}
	}
	if gvk.Empty() {
		return nil, fmt.Errorf("object has no GroupVersionKind")
	}

	obj, err := kamerascheme.Default.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("creating typed object for %s: %w", gvk.String(), err)
	}

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, obj); err != nil {
		return nil, fmt.Errorf("converting from unstructured: %w", err)
	}

	if accessor, err := meta.Accessor(obj); err == nil {
		accessor.SetNamespace(u.GetNamespace())
		accessor.SetName(u.GetName())
		accessor.SetResourceVersion(u.GetResourceVersion())
	}
	obj.GetObjectKind().SetGroupVersionKind(gvk)

	return obj, nil
}

func listKindForResource(resource string) string {
	if kind, ok := resourceToListKind[resource]; ok {
		return kind
	}
	if resource == "" {
		return "List"
	}
	return strings.ToUpper(resource[:1]) + resource[1:] + "List"
}
