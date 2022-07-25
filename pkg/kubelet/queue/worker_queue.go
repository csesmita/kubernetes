/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// This file contains structures that implement scheduling queue types.
// Scheduling queues hold pods waiting to be scheduled. This file implements a/
// priority queue which has two sub queues. One sub-queue holds pods that are
// being considered for scheduling. This is called workerQ. Another queue holds
// pods that are already tried and are determined to be unschedulable. The latter
// is called unschedulableQ.

package queue

import (
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	queueClosed = "scheduling queue is closed"
)

// WorkerQueue is an interface for a queue to store pods waiting to be sent to OS.
type WorkerQueue interface {
	Add(pod *v1.Pod) error
	// Pop removes the head of the queue and returns it. It blocks if the
	// queue is empty and waits until a new item is added to the queue.
	Pop() (*v1.Pod, error)
	Update(oldPod, newPod *v1.Pod) error
	Delete(pod *v1.Pod) error

	Len() int
	// Close closes the WorkerQueue so that the goroutine which is
	// waiting to pop items can exit gracefully.
	Close()
}

// NewWorkerQueue initializes a priority queue as a new scheduling queue.
func NewWorkerQueue() WorkerQueue {
	return NewPriorityQueue()
}

// PriorityQueue implements a worker queue.
// The head of PriorityQueue is the highest priority pending pod. This structure
// has one sub queues that holds pods that are being considered for
// scheduling. This is called workerQ and is a Heap.
type PriorityQueue struct {
	stop  chan struct{}

	lock sync.RWMutex
	cond sync.Cond

	// workerQ is heap structure that Kubelet actively looks at to find pods to
	// schedule next (send to Linux scheduler). Head of heap is the highest priority pod.
	workerQ *Heap

	// closed indicates that the queue is closed.
	// It is mainly used to let Pop() exit its control loop while waiting for an item.
	closed bool
}

// Making sure that PriorityQueue implements WorkerQueue.
var _ WorkerQueue = &PriorityQueue{}

// NewPriorityQueue creates a PriorityQueue object.
func NewPriorityQueue() *PriorityQueue {
	comp := func(pod1, pod2 interface{}) bool {
		p1 := pod1.(*v1.Pod)
		p2 := pod2.(*v1.Pod)
		//return p1.PodStatus.startTime.Before(p2.PodStatus)
		return p1.CreationTimestamp.Before(&p2.CreationTimestamp)
	}

	pq := &PriorityQueue{
		stop:                      make(chan struct{}),
		workerQ:                   New(podKeyFunc, comp),
	}
	pq.cond.L = &pq.lock
	return pq
}

// Add adds a pod to the worker queue. It should be called only when a new pod
// is added so there is no chance the pod is already in the queue.
func (p *PriorityQueue) Add(pod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if err := p.workerQ.Add(pod); err != nil {
		klog.ErrorS(err, "Error adding pod to the worker queue", "pod", klog.KObj(pod))
		return err
	}
	p.cond.Broadcast()
	return nil
}

func(p *PriorityQueue) Len() int {
	return p.workerQ.Len()
}

// Pop removes the head of the worker queue and returns it. It blocks if the
// workerQ is empty and waits until a new item is added to the queue.
func (p *PriorityQueue) Pop() (*v1.Pod, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for p.workerQ.Len() == 0 {
		// When the queue is empty, invocation of Pop() is blocked until new item is enqueued.
		// When Close() is called, the p.closed is set and the condition is broadcast,
		// which causes this loop to continue and return from the Pop().
		if p.closed {
			return nil, fmt.Errorf(queueClosed)
		}
		p.cond.Wait()
	}
	obj, err := p.workerQ.Pop()
	if err != nil {
		return nil, err
	}
	pInfo := obj.(*v1.Pod)
	return pInfo, err
}

// Update updates a pod in the active or backoff queue if present. Otherwise, it removes
// the item from the unschedulable queue if pod is updated in a way that it may
// become schedulable and adds the updated one to the active queue.
// If pod is not present in any of the queues, it is added to the active queue.
func (p *PriorityQueue) Update(oldPod, newPod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if oldPod != nil {
		// If the pod is already in the worker queue, just update it there.
		if oldPod, exists, _ := p.workerQ.Get(oldPod); exists {
			p.workerQ.Delete(oldPod)
			return p.workerQ.Add(newPod)
		}
	}

	// If pod is not in any of the queues, we put it in the worker queue.
	if err := p.workerQ.Add(newPod); err != nil {
		return err
	}
	p.cond.Broadcast()
	return nil
}

// Delete deletes the item from either of the two queues. It assumes the pod is
// only in one queue.
func (p *PriorityQueue) Delete(pod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.workerQ.Delete(pod)
	return nil
}

// Close closes the priority queue.
func (p *PriorityQueue) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	close(p.stop)
	p.closed = true
	p.cond.Broadcast()
}

func podKeyFunc(obj interface{}) (string, error) {
	return cache.MetaNamespaceKeyFunc(obj.(*v1.Pod))
}

func MakeNextPodFunc(queue WorkerQueue) func() *v1.Pod {
	return func() *v1.Pod {
		pod, err := queue.Pop()
		if err == nil {
			// TODO(smita) check for resource fit here
			klog.InfoS("Scheduling pod from worker queue", "pod", klog.KObj(pod))
			return pod
		}
		klog.ErrorS(err, "Error while retrieving next pod from scheduling queue")
		return nil
	}
}
