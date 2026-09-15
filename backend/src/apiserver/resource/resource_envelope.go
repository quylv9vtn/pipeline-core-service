// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resource

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/kubeflow/pipelines/api/v2alpha1/go/pipelinespec"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	k8sresource "k8s.io/apimachinery/pkg/api/resource"
)

const maxExactEnvelopeTasks = 15

// ResourceEnvelope is the element-wise peak of Kubernetes resource requests
// and limits for a pipeline version. Independent tasks are assumed to be able
// to run concurrently. The peak for each resource may come from a different
// valid concurrent task set.
type ResourceEnvelope struct {
	Requests          map[string]string `json:"requests,omitempty"`
	Limits            map[string]string `json:"limits,omitempty"`
	GPUs              []GPUEnvelope     `json:"gpus,omitempty"`
	Scope             string            `json:"scope"`
	Calculation       string            `json:"calculation"`
	CalculatorVersion string            `json:"calculator_version"`
	Warnings          []string          `json:"warnings,omitempty"`
}

// GPUEnvelope is the peak number of GPUs required for one model/type. The
// model is the static accelerator type declared in the Pipeline IR.
type GPUEnvelope struct {
	GPUModel string `json:"gpu_model"`
	GPUCount string `json:"gpu_count"`
}

type envelopeQuantities struct {
	requests map[corev1.ResourceName]k8sresource.Quantity
	limits   map[corev1.ResourceName]k8sresource.Quantity
	gpus     map[string]k8sresource.Quantity
}

type envelopeCalculator struct {
	spec        *pipelinespec.PipelineSpec
	platform    *pipelinespec.PlatformSpec
	deployment  *pipelinespec.PipelineDeploymentConfig
	warnings    []string
	unknown     bool
	upperBound  bool
	componentIn map[string]bool
}

// CalculateResourceEnvelope calculates an upload-time envelope from a V2 IR.
// Static DAGs with up to maxExactEnvelopeTasks tasks per component are exact.
// Larger DAGs fall back to summing every task, which is a safe upper bound.
func CalculateResourceEnvelope(spec *pipelinespec.PipelineSpec, platformSpecs ...*pipelinespec.PlatformSpec) (*ResourceEnvelope, error) {
	if spec == nil || spec.GetRoot() == nil {
		return nil, fmt.Errorf("pipeline spec or root component is empty")
	}
	deployment := &pipelinespec.PipelineDeploymentConfig{}
	if spec.GetDeploymentSpec() != nil {
		data, err := protojson.Marshal(spec.GetDeploymentSpec())
		if err != nil {
			return nil, fmt.Errorf("marshal deployment spec: %w", err)
		}
		if err := protojson.Unmarshal(data, deployment); err != nil {
			return nil, fmt.Errorf("parse deployment spec: %w", err)
		}
	}
	var platformSpec *pipelinespec.PlatformSpec
	if len(platformSpecs) > 0 {
		platformSpec = platformSpecs[0]
	}
	c := &envelopeCalculator{spec: spec, platform: platformSpec, deployment: deployment, componentIn: map[string]bool{}}
	quantities, err := c.componentEnvelope("<root>", spec.GetRoot())
	if err != nil {
		return nil, err
	}
	calculation := "EXACT"
	if c.unknown {
		calculation = "UNKNOWN"
	} else if c.upperBound {
		calculation = "UPPER_BOUND"
	}
	return &ResourceEnvelope{
		Requests:          stringifyQuantities(quantities.requests),
		Limits:            stringifyQuantities(quantities.limits),
		GPUs:              stringifyGPUs(quantities.gpus),
		Scope:             "DECLARED_CONTAINER_RESOURCES",
		Calculation:       calculation,
		CalculatorVersion: "v2",
		Warnings:          c.warnings,
	}, nil
}

func (c *envelopeCalculator) componentEnvelope(name string, component *pipelinespec.ComponentSpec) (envelopeQuantities, error) {
	if component == nil {
		return newEnvelopeQuantities(), fmt.Errorf("component %q is missing", name)
	}
	if component.GetDag() != nil {
		if c.componentIn[name] {
			return newEnvelopeQuantities(), fmt.Errorf("component cycle detected at %q", name)
		}
		c.componentIn[name] = true
		defer delete(c.componentIn, name)
		return c.dagEnvelope(component.GetDag())
	}
	return c.executorEnvelope(component.GetExecutorLabel()), nil
}

func (c *envelopeCalculator) executorEnvelope(label string) envelopeQuantities {
	result := newEnvelopeQuantities()
	executor := c.deployment.GetExecutors()[label]
	if executor == nil || executor.GetContainer() == nil || executor.GetContainer().GetResources() == nil {
		return result
	}
	resources := executor.GetContainer().GetResources()
	c.addQuantity(result.requests, corev1.ResourceCPU, resources.GetResourceCpuRequest(), deprecatedCPU(resources.GetCpuRequest()), label+" cpu request")
	c.addQuantity(result.limits, corev1.ResourceCPU, resources.GetResourceCpuLimit(), deprecatedCPU(resources.GetCpuLimit()), label+" cpu limit")
	c.addQuantity(result.requests, corev1.ResourceMemory, resources.GetResourceMemoryRequest(), deprecatedMemory(resources.GetMemoryRequest()), label+" memory request")
	c.addQuantity(result.limits, corev1.ResourceMemory, resources.GetResourceMemoryLimit(), deprecatedMemory(resources.GetMemoryLimit()), label+" memory limit")
	if accelerator := resources.GetAccelerator(); accelerator != nil {
		resourceType := accelerator.GetResourceType()
		if resourceType == "" {
			resourceType = accelerator.GetType()
		}
		count := accelerator.GetResourceCount()
		if count == "" && accelerator.GetCount() > 0 {
			count = strconv.FormatInt(accelerator.GetCount(), 10)
		}
		if resourceType != "" && count != "" {
			resourceName := corev1.ResourceName(resourceType)
			c.addQuantity(result.requests, resourceName, count, "", label+" accelerator request")
			c.addQuantity(result.limits, resourceName, count, "", label+" accelerator limit")
			if isGPUType(resourceType) {
				c.addGPUQuantity(result.gpus, c.gpuModel(label, resourceType), count, label+" GPU count")
			}
		} else if resourceType != "" || count != "" {
			c.markUnknown(fmt.Sprintf("executor %q has an unresolved accelerator type or count", label))
		}
	}
	return result
}

func (c *envelopeCalculator) gpuModel(executorLabel, fallback string) string {
	if c.platform == nil {
		return fallback
	}
	kubernetesSpec := c.platform.GetPlatforms()["kubernetes"]
	if kubernetesSpec == nil || kubernetesSpec.GetDeploymentSpec() == nil {
		return fallback
	}
	executor := kubernetesSpec.GetDeploymentSpec().GetExecutors()[executorLabel]
	if executor == nil {
		return fallback
	}
	nodeSelectorValue := executor.GetFields()["nodeSelector"]
	if nodeSelectorValue == nil || nodeSelectorValue.GetStructValue() == nil {
		return fallback
	}
	labelsValue := nodeSelectorValue.GetStructValue().GetFields()["labels"]
	if labelsValue == nil || labelsValue.GetStructValue() == nil {
		return fallback
	}
	labels := labelsValue.GetStructValue().GetFields()
	preferredKeys := []string{
		"nvidia.com/gpu.product",
		"cloud.google.com/gke-accelerator",
		"karpenter.k8s.aws/instance-gpu-name",
		"node.kubernetes.io/accelerator",
	}
	for _, key := range preferredKeys {
		if value := labels[key].GetStringValue(); value != "" {
			return value
		}
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		normalized := strings.ToLower(key)
		if strings.Contains(normalized, "accelerator") ||
			(strings.Contains(normalized, "gpu") &&
				(strings.Contains(normalized, "model") || strings.Contains(normalized, "product") || strings.Contains(normalized, "type"))) {
			if value := labels[key].GetStringValue(); value != "" {
				return value
			}
		}
	}
	return fallback
}

func (c *envelopeCalculator) dagEnvelope(dag *pipelinespec.DagSpec) (envelopeQuantities, error) {
	names := make([]string, 0, len(dag.GetTasks()))
	for name := range dag.GetTasks() {
		names = append(names, name)
	}
	sort.Strings(names)
	taskEnvelopes := make([]envelopeQuantities, len(names))
	index := make(map[string]int, len(names))
	for i, name := range names {
		index[name] = i
		task := dag.GetTasks()[name]
		if task.GetTriggerPolicy().GetCondition() != "" {
			c.upperBound = true
			c.warnings = append(c.warnings, fmt.Sprintf("task %q is conditional; the envelope assumes its resources may be needed", name))
		}
		componentName := task.GetComponentRef().GetName()
		component := c.spec.GetComponents()[componentName]
		envelope, err := c.componentEnvelope(componentName, component)
		if err != nil {
			return newEnvelopeQuantities(), fmt.Errorf("calculate task %q: %w", name, err)
		}
		factor := c.iteratorFactor(name, task)
		taskEnvelopes[i] = multiplyEnvelope(envelope, factor)
	}

	if len(names) > maxExactEnvelopeTasks {
		c.upperBound = true
		c.warnings = append(c.warnings, fmt.Sprintf("DAG has %d tasks; envelope uses the sum of all tasks as a safe upper bound", len(names)))
		return sumEnvelopes(taskEnvelopes), nil
	}

	reachable := make([][]bool, len(names))
	for i := range reachable {
		reachable[i] = make([]bool, len(names))
	}
	for taskName, task := range dag.GetTasks() {
		to := index[taskName]
		for _, dependency := range task.GetDependentTasks() {
			from, ok := index[dependency]
			if !ok {
				return newEnvelopeQuantities(), fmt.Errorf("task %q depends on missing task %q", taskName, dependency)
			}
			reachable[from][to] = true
		}
	}
	for k := range names {
		for i := range names {
			for j := range names {
				reachable[i][j] = reachable[i][j] || (reachable[i][k] && reachable[k][j])
			}
		}
	}
	for i := range names {
		if reachable[i][i] {
			return newEnvelopeQuantities(), fmt.Errorf("DAG contains a dependency cycle at task %q", names[i])
		}
	}

	peak := newEnvelopeQuantities()
	for subset := uint64(1); subset < uint64(1)<<len(names); subset++ {
		valid := true
		selected := make([]envelopeQuantities, 0, len(names))
		for i := range names {
			if subset&(uint64(1)<<i) == 0 {
				continue
			}
			for j := i + 1; j < len(names); j++ {
				if subset&(uint64(1)<<j) != 0 && (reachable[i][j] || reachable[j][i]) {
					valid = false
					break
				}
			}
			if !valid {
				break
			}
			selected = append(selected, taskEnvelopes[i])
		}
		if valid {
			maximizeEnvelope(&peak, sumEnvelopes(selected))
		}
	}
	return peak, nil
}

func (c *envelopeCalculator) iteratorFactor(taskName string, task *pipelinespec.PipelineTaskSpec) int64 {
	parallelismLimit := int64(task.GetIteratorPolicy().GetParallelismLimit())
	if iterator := task.GetParameterIterator(); iterator != nil {
		if raw := iterator.GetItems().GetRaw(); raw != "" {
			var items []interface{}
			if err := json.Unmarshal([]byte(raw), &items); err == nil {
				factor := int64(len(items))
				if parallelismLimit > 0 && factor > parallelismLimit {
					factor = parallelismLimit
				}
				return factor
			}
		}
		if parallelismLimit > 0 {
			c.upperBound = true
			c.warnings = append(c.warnings, fmt.Sprintf("task %q has a runtime-resolved parameter iterator; its parallelism limit is used as an upper bound", taskName))
			return parallelismLimit
		}
		c.markUnknown(fmt.Sprintf("task %q has a runtime-resolved parameter iterator", taskName))
		return 1
	}
	if task.GetArtifactIterator() != nil {
		if parallelismLimit > 0 {
			c.upperBound = true
			c.warnings = append(c.warnings, fmt.Sprintf("task %q has a runtime-resolved artifact iterator; its parallelism limit is used as an upper bound", taskName))
			return parallelismLimit
		}
		c.markUnknown(fmt.Sprintf("task %q has a runtime-resolved artifact iterator", taskName))
	}
	return 1
}

func (c *envelopeCalculator) addQuantity(target map[corev1.ResourceName]k8sresource.Quantity, name corev1.ResourceName, primary, fallback, field string) {
	value := primary
	if value == "" {
		value = fallback
	}
	if value == "" {
		return
	}
	quantity, err := k8sresource.ParseQuantity(value)
	if err != nil {
		c.markUnknown(fmt.Sprintf("%s %q cannot be resolved at pipeline-version creation", field, value))
		return
	}
	target[name] = quantity
}

func (c *envelopeCalculator) addGPUQuantity(target map[string]k8sresource.Quantity, model, count, field string) {
	quantity, err := k8sresource.ParseQuantity(count)
	if err != nil {
		c.markUnknown(fmt.Sprintf("%s %q cannot be resolved at pipeline-version creation", field, count))
		return
	}
	target[model] = quantity
}

func (c *envelopeCalculator) markUnknown(warning string) {
	c.unknown = true
	c.warnings = append(c.warnings, warning)
}

func newEnvelopeQuantities() envelopeQuantities {
	return envelopeQuantities{
		requests: map[corev1.ResourceName]k8sresource.Quantity{},
		limits:   map[corev1.ResourceName]k8sresource.Quantity{},
		gpus:     map[string]k8sresource.Quantity{},
	}
}

func sumEnvelopes(envelopes []envelopeQuantities) envelopeQuantities {
	result := newEnvelopeQuantities()
	for _, envelope := range envelopes {
		addQuantityMaps(result.requests, envelope.requests)
		addQuantityMaps(result.limits, envelope.limits)
		addGPUQuantityMaps(result.gpus, envelope.gpus)
	}
	return result
}

func addGPUQuantityMaps(target, source map[string]k8sresource.Quantity) {
	for model, quantity := range source {
		current := target[model]
		current.Add(quantity)
		target[model] = current
	}
}

func addQuantityMaps(target, source map[corev1.ResourceName]k8sresource.Quantity) {
	for name, quantity := range source {
		current := target[name]
		current.Add(quantity)
		target[name] = current
	}
}

func multiplyEnvelope(envelope envelopeQuantities, factor int64) envelopeQuantities {
	result := newEnvelopeQuantities()
	for i := int64(0); i < factor; i++ {
		addQuantityMaps(result.requests, envelope.requests)
		addQuantityMaps(result.limits, envelope.limits)
		addGPUQuantityMaps(result.gpus, envelope.gpus)
	}
	return result
}

func maximizeEnvelope(target *envelopeQuantities, candidate envelopeQuantities) {
	maximizeQuantityMap(target.requests, candidate.requests)
	maximizeQuantityMap(target.limits, candidate.limits)
	maximizeGPUQuantityMap(target.gpus, candidate.gpus)
}

func maximizeQuantityMap(target, candidate map[corev1.ResourceName]k8sresource.Quantity) {
	for name, quantity := range candidate {
		current, ok := target[name]
		if !ok || quantity.Cmp(current) > 0 {
			target[name] = quantity
		}
	}
}

func maximizeGPUQuantityMap(target, candidate map[string]k8sresource.Quantity) {
	for model, quantity := range candidate {
		current, ok := target[model]
		if !ok || quantity.Cmp(current) > 0 {
			target[model] = quantity
		}
	}
}

func stringifyQuantities(values map[corev1.ResourceName]k8sresource.Quantity) map[string]string {
	result := make(map[string]string, len(values))
	for name, quantity := range values {
		if !quantity.IsZero() {
			result[string(name)] = quantity.String()
		}
	}
	return result
}

func stringifyGPUs(values map[string]k8sresource.Quantity) []GPUEnvelope {
	models := make([]string, 0, len(values))
	for model, quantity := range values {
		if !quantity.IsZero() {
			models = append(models, model)
		}
	}
	sort.Strings(models)
	result := make([]GPUEnvelope, 0, len(models))
	for _, model := range models {
		quantity := values[model]
		result = append(result, GPUEnvelope{GPUModel: model, GPUCount: quantity.String()})
	}
	return result
}

func isGPUType(resourceType string) bool {
	normalized := strings.ToLower(resourceType)
	return strings.Contains(normalized, "gpu") ||
		strings.Contains(normalized, "nvidia") ||
		strings.Contains(normalized, "amd") ||
		strings.Contains(normalized, "radeon")
}

func deprecatedCPU(value float64) string {
	if value == 0 {
		return ""
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func deprecatedMemory(value float64) string {
	if value == 0 {
		return ""
	}
	return strconv.FormatFloat(value, 'f', -1, 64) + "G"
}
