package service

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/swarm"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/progress"
	"github.com/docker/docker/pkg/streamformatter"
	"github.com/docker/docker/pkg/stringid"
	"golang.org/x/net/context"
)

var (
	stateToProgress = map[swarm.TaskState]int64{
		swarm.TaskStateNew:       1,
		swarm.TaskStateAllocated: 2,
		swarm.TaskStatePending:   3,
		swarm.TaskStateAssigned:  4,
		swarm.TaskStateAccepted:  5,
		swarm.TaskStatePreparing: 6,
		swarm.TaskStateReady:     7,
		swarm.TaskStateStarting:  8,
		swarm.TaskStateRunning:   9,
	}

	longestState = func() int {
		longest := 0
		for state := range stateToProgress {
			if len(state) > longest {
				longest = len(state)
			}
		}
		return longest
	}()
)

const (
	maxProgress     = 9
	maxProgressBars = 20
)

type progressUpdater interface {
	update(ctx context.Context, service swarm.Service, tasks []swarm.Task, activeNodes map[string]swarm.Node) (bool, error)
}

func serviceProgress(ctx context.Context, client client.APIClient, serviceID string, progressWriter io.WriteCloser) error {
	defer progressWriter.Close()

	progressOut := streamformatter.NewJSONStreamFormatter().NewProgressOutput(progressWriter, false)

	taskFilter := filters.NewArgs()
	taskFilter.Add("service", serviceID)

	var (
		updater     progressUpdater
		converged   bool
		convergedAt time.Time
		monitor     = 5 * time.Second
	)

	for {
		service, _, err := client.ServiceInspectWithRaw(ctx, serviceID)
		if err != nil {
			return err
		}

		if service.Spec.UpdateConfig != nil && service.Spec.UpdateConfig.Monitor != 0 {
			monitor = service.Spec.UpdateConfig.Monitor
		}

		if updater == nil {
			if service.Spec.Mode.Replicated != nil && service.Spec.Mode.Replicated.Replicas != nil {
				updater = &replicatedProgressUpdater{
					progressOut: progressOut,
				}
			} else if service.Spec.Mode.Global != nil {
				updater = &globalProgressUpdater{
					progressOut: progressOut,
				}
			} else {
				return errors.New("unrecognized service mode")
			}
		}

		if service.UpdateStatus != nil {
			// FIXME(aaronl): Handle rollback cases specially
			if service.UpdateStatus.State == swarm.UpdateStateCompleted {
				return nil
			} else if service.UpdateStatus.State == swarm.UpdateStatePaused {
				return fmt.Errorf("service update paused: %s", service.UpdateStatus.Message)
			}
		} else if converged && time.Since(convergedAt) >= monitor {
			return nil
		}

		tasks, err := client.TaskList(ctx, types.TaskListOptions{Filters: taskFilter})
		if err != nil {
			return err
		}

		nodes, err := client.NodeList(ctx, types.NodeListOptions{})
		if err != nil {
			return err
		}

		activeNodes := make(map[string]swarm.Node)
		for _, n := range nodes {
			if n.Status.State != swarm.NodeStateDown {
				activeNodes[n.ID] = n
			}
		}

		converged, err = updater.update(ctx, service, tasks, activeNodes)
		if err != nil {
			return err
		}
		if converged {
			if convergedAt.IsZero() {
				convergedAt = time.Now()
			}
			wait := monitor - time.Since(convergedAt)
			if wait >= time.Second {
				progressOut.WriteProgress(progress.Progress{
					Action: fmt.Sprintf("Waiting %d seconds to verify that tasks are stable...", wait/time.Second),
				})
			}
		} else {
			convergedAt = time.Time{}
		}

		time.Sleep(200 * time.Millisecond)
	}
}

func writeOverallProgress(progressOut progress.Output, numerator, denominator int) {
	progressOut.WriteProgress(progress.Progress{
		ID:     "overall progress",
		Action: fmt.Sprintf("%d out of %d tasks", numerator, denominator),
	})
}

type replicatedProgressUpdater struct {
	progressOut progress.Output

	// used for maping slots to a contiguous space
	// this also causes progress bars to appear in order
	slotMap map[int]int

	initialized bool
	done        bool
}

func (u *replicatedProgressUpdater) update(ctx context.Context, service swarm.Service, tasks []swarm.Task, activeNodes map[string]swarm.Node) (bool, error) {
	running := uint64(0)

	if service.Spec.Mode.Replicated == nil || service.Spec.Mode.Replicated.Replicas == nil {
		return false, errors.New("no replica count")
	}
	replicas := *service.Spec.Mode.Replicated.Replicas

	if !u.initialized {
		u.slotMap = make(map[int]int)

		// Draw progress bars in order
		writeOverallProgress(u.progressOut, 0, int(replicas))

		if replicas <= maxProgressBars {
			for i := uint64(1); i <= replicas; i++ {
				progress.Update(u.progressOut, fmt.Sprintf("%d/%d", i, replicas), " ")
			}
		}
		u.initialized = true
	}

	for _, task := range tasks {
		if _, nodeActive := activeNodes[task.NodeID]; nodeActive && taskUpToDate(service, task) {
			mappedSlot := u.slotMap[task.Slot]
			if mappedSlot == 0 {
				mappedSlot = len(u.slotMap) + 1
				u.slotMap[task.Slot] = mappedSlot
			}
			if !u.done && replicas <= maxProgressBars && uint64(mappedSlot) <= replicas {
				u.progressOut.WriteProgress(progress.Progress{
					ID:         fmt.Sprintf("%d/%d", mappedSlot, replicas),
					Action:     fmt.Sprintf("%-[1]*s", longestState, task.Status.State),
					Current:    stateToProgress[task.Status.State],
					Total:      maxProgress,
					HideCounts: true,
				})
			}
			if task.Status.State == swarm.TaskStateRunning {
				running++
			}
		}
	}

	if !u.done {
		writeOverallProgress(u.progressOut, int(running), int(replicas))

		if running == replicas {
			u.done = true
		}
	}

	return running == replicas, nil
}

type globalProgressUpdater struct {
	progressOut progress.Output

	initialized bool
	done        bool
}

func (u *globalProgressUpdater) update(ctx context.Context, service swarm.Service, tasks []swarm.Task, activeNodes map[string]swarm.Node) (bool, error) {
	// FIXME(aaronl): Checking constraints on the client side is bad. We
	// should provide a way to find out whether a global service is
	// converged without having all this logic duplicated on the client
	// side.
	if service.Spec.TaskTemplate.Placement != nil {
		for nodeID, node := range activeNodes {
			constraints, _ := parseConstraints(service.Spec.TaskTemplate.Placement.Constraints)
			if !nodeMatches(constraints, node) {
				delete(activeNodes, nodeID)
			}
		}
	}

	running := 0
	nodeCount := len(activeNodes)

	if !u.initialized {
		writeOverallProgress(u.progressOut, 0, nodeCount)
		u.initialized = true
	}

	for _, task := range tasks {
		if node, nodeActive := activeNodes[task.NodeID]; nodeActive && taskUpToDate(service, task) {
			if !u.done && nodeCount <= maxProgressBars {
				u.progressOut.WriteProgress(progress.Progress{
					ID:         stringid.TruncateID(node.ID),
					Action:     fmt.Sprintf("%-[1]*s", longestState, task.Status.State),
					Current:    stateToProgress[task.Status.State],
					Total:      maxProgress,
					HideCounts: true,
				})
			}
			if task.Status.State == swarm.TaskStateRunning {
				running++
			}
		}
	}

	if !u.done {
		writeOverallProgress(u.progressOut, running, nodeCount)

		if running == nodeCount {
			u.done = true
		}
	}

	return running == nodeCount, nil
}

func taskUpToDate(s swarm.Service, t swarm.Task) bool {
	// FIXME(aaronl): This ignores t.Endpoint, which doesn't seem to be
	// present in engine's representation of the task. It must either be
	// nil, or t.Endpoint.Spec should match s.Spec.EndpointSpec.
	return reflect.DeepEqual(s.Spec.TaskTemplate, t.Spec)
}
