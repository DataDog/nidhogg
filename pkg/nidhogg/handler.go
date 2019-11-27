package nidhogg

import (
	"context"
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
)

const taintKey = "nidhogg.uswitch.com"

// Handler performs the main business logic of the Wave controller
type Handler struct {
	client.Client
	recorder record.EventRecorder
	config   HandlerConfig

	nodeSelector labels.Selector
	nodeRejecter labels.Selector
}

//HandlerConfig contains the options for Nidhogg
type HandlerConfig struct {
	Daemonsets   []Daemonset       `json:"daemonsets" yaml:"daemonsets"`
	NodeSelector map[string]string `json:"nodeSelector" yaml:"nodeSelector"`
	NodeRejecter map[string]string `json:"nodeRejecter" yaml:"nodeRejecter"`
}

//Daemonset contains the name and namespace of a Daemonset
type Daemonset struct {
	Name      string `json:"name" yaml:"name"`
	Namespace string `json:"namespace" yaml:"namespace"`
}

type taintChanges struct {
	taintsAdded   []string
	taintsRemoved []string
}

// NewHandler constructs a new instance of Handler
func NewHandler(c client.Client, r record.EventRecorder, conf HandlerConfig) *Handler {
	return &Handler{
		Client:       c,
		recorder:     r,
		config:       conf,
		nodeSelector: labels.SelectorFromSet(conf.NodeSelector),
		nodeRejecter: labels.SelectorFromSet(conf.NodeRejecter),
	}
}

// HandleNode works out what taints need to be applied to the node
func (h *Handler) HandleNode(instance *corev1.Node) (reconcile.Result, error) {
	log := logf.Log.WithName("nidhogg")

	// check whether node matches the selectors
	var daemonsets []Daemonset
	labelSet := labels.Set(instance.Labels)
	if h.nodeSelector.Matches(labelSet) && !h.nodeRejecter.Matches(labelSet) {
		daemonsets = h.config.Daemonsets
	}
	nodeCopy, taintChanges, err := h.calculateTaints(instance, daemonsets)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("error caluclating taints for node %s: %v", instance.Name, err)
	}

	if !reflect.DeepEqual(nodeCopy, instance) {
		instance = nodeCopy
		log.Info("Updating Node taints", "instance", instance.Name, "taints added", taintChanges.taintsAdded, "taints removed", taintChanges.taintsRemoved)
		err := h.Update(context.TODO(), instance)
		if err != nil {
			return reconcile.Result{}, err
		}

		// this is a hack to make the event work on a non-namespaced object
		nodeCopy.UID = types.UID(nodeCopy.Name)

		h.recorder.Eventf(nodeCopy, corev1.EventTypeNormal, "TaintsChanged", "Taints added: %s, Taints removed: %s", taintChanges.taintsAdded, taintChanges.taintsRemoved)
	}

	return reconcile.Result{}, nil
}

func (h *Handler) calculateTaints(instance *corev1.Node, daemonsets []Daemonset) (*corev1.Node, taintChanges, error) {

	nodeCopy := instance.DeepCopy()

	var changes taintChanges

	for _, daemonset := range daemonsets {

		taint := fmt.Sprintf("%s/%s.%s", taintKey, daemonset.Namespace, daemonset.Name)
		// Get Pod for node
		pod, err := h.getDaemonsetPod(instance.Name, daemonset)
		if err != nil {
			return nil, taintChanges{}, fmt.Errorf("error fetching pods: %v", err)
		}

		if pod == nil || podNotReady(pod) {
			if !taintPresent(nodeCopy, taint) {
				nodeCopy.Spec.Taints = addTaint(nodeCopy.Spec.Taints, taint)
				changes.taintsAdded = append(changes.taintsAdded, taint)
			}
		} else if taintPresent(nodeCopy, taint) {
			nodeCopy.Spec.Taints = removeTaint(nodeCopy.Spec.Taints, taint)
			changes.taintsRemoved = append(changes.taintsRemoved, taint)
		}

	}
	return nodeCopy, changes, nil
}

func (h *Handler) getDaemonsetPod(nodeName string, ds Daemonset) (*corev1.Pod, error) {
	opts := client.InNamespace(ds.Namespace)
	pods := &corev1.PodList{}
	err := h.List(context.TODO(), opts, pods)
	if err != nil {
		return nil, err
	}

	for _, pod := range pods.Items {
		for _, owner := range pod.OwnerReferences {
			if owner.Name == ds.Name {
				if pod.Spec.NodeName == nodeName {
					return &pod, nil
				}
			}
		}
	}

	return nil, nil
}

func podNotReady(pod *corev1.Pod) bool {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Ready == false {
			return true
		}
	}
	return false
}

func taintPresent(node *corev1.Node, taintName string) bool {

	for _, taint := range node.Spec.Taints {
		if taint.Key == taintName {
			return true
		}
	}
	return false
}

func addTaint(taints []corev1.Taint, taintName string) []corev1.Taint {
	return append(taints, corev1.Taint{Key: taintName, Effect: corev1.TaintEffectNoSchedule})
}

func removeTaint(taints []corev1.Taint, taintName string) []corev1.Taint {
	newTaints := []corev1.Taint{}

	for _, taint := range taints {
		if taint.Key == taintName {
			continue
		}
		newTaints = append(newTaints, taint)
	}

	//empty array != nil
	if len(newTaints) == 0 {
		return nil
	}

	return newTaints
}
