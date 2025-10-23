package main

import (
	"context"
	"fmt"

	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/serving/kamera"
	"knative.dev/serving/pkg/apis/serving"
	v1 "knative.dev/serving/pkg/apis/serving/v1"
	"knative.dev/serving/pkg/autoscaler/scaling"
	kpareconciler "knative.dev/serving/pkg/reconciler/autoscaling/kpa"
	revisionreconciler "knative.dev/serving/pkg/reconciler/revision"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var scheme = runtime.NewScheme()

func main() {
	logf.SetLogger(ctrlzap.New(
		ctrlzap.UseDevMode(true),
		ctrlzap.Level(zapcore.InfoLevel),
	))

	eb := tracecheck.NewExplorerBuilder(scheme)
	// instantiate reconcilers
	eb.WithCustomStrategy("RevisionReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		strategy, err := kamera.NewKnativeStrategy(revisionreconciler.NewController, r)
		if err != nil {
			panic(err)
		}
		strategy.SetLogger(logf.Log.WithName("RevisionReconciler"))
		return strategy
	})
	eb.WithCustomStrategy("KPA", func(r replay.EffectRecorder) tracecheck.Strategy {
		factory := func(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
			multiScaler := scaling.NewMultiScaler(ctx.Done(), nil, logging.FromContext(ctx))
			return kpareconciler.NewController(ctx, cmw, multiScaler)
		}
		strategy, err := kamera.NewKnativeStrategy(factory, r, serving.RevisionUID)
		if err != nil {
			panic(fmt.Sprintf("NewKnativeStrategy() error = %v", err))
		}
		strategy.SetLogger(logf.Log.WithName("KPAReconciler"))
		return strategy
	})

	eb.AssignReconcilerToKind("RevisionReconciler", "Revision")
	eb.AssignReconcilerToKind("KPA", "PodAutoscaler")
	eb.WithResourceDep("Revision", "RevisionReconciler", "KPA")
	explorer, err := eb.Build("standalone")
	if err != nil {
		panic(fmt.Sprintf("Build() error = %v", err))
	}
	stateBuilder := eb.NewStateEventBuilder()
	rev := &v1.Revision{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
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
	}
	tag.AddSleeveObjectID(rev)
	initialState := stateBuilder.AddTopLevelObject(rev, "RevisionReconciler")
	res := explorer.Explore(context.Background(), initialState)
	fmt.Println("Exploration result:", res)
}
