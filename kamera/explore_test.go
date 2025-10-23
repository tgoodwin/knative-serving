package kamera

import (
	"context"
	"fmt"
	"testing"

	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	v1 "knative.dev/serving/pkg/apis/serving/v1"
	revisionreconciler "knative.dev/serving/pkg/reconciler/revision"
)

var scheme = runtime.NewScheme()

func TestExplore(t *testing.T) {
	eb := tracecheck.NewExplorerBuilder(scheme)
	// instantiate reconcilers
	eb.WithCustomStrategy("RevisionReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		strategy, err := NewKnativeStrategy(revisionreconciler.NewController, r)
		if err != nil {
			panic(err)
		}
		return strategy
	})

	eb.AssignReconcilerToKind("RevisionReconciler", "Revision")
	eb.WithResourceDep("Revision", "RevisionReconciler")
	explorer, err := eb.Build("standalone")
	if err != nil {
		t.Fatal(err)
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
	initialState := stateBuilder.AddTopLevelObject(rev, "RevisionReconciler")
	res := explorer.Explore(context.Background(), initialState)
	fmt.Println("Exploration result:", res)
}
