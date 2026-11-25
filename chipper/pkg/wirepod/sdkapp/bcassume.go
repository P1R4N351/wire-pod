package sdkapp

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/fforchino/vector-go-sdk/pkg/vectorpb"
	"github.com/kercre123/wire-pod/chipper/pkg/logger"
)

const (
	bcGrantWaitTimeout       = 6 * time.Second
	bcPriorityHigh           = "high"
	bcPriorityOverride       = "override_behaviors"
	bcPriorityReserve        = "reserve"
	bcPriorityReserveControl = "reserve_control"
)

type bcControlStream interface {
	Send(*vectorpb.BehaviorControlRequest) error
	Recv() (*vectorpb.BehaviorControlResponse, error)
}

type bcStreamOpener func(context.Context) (bcControlStream, error)

func bcPriority(priority string) vectorpb.ControlRequest_Priority {
	trimmed := strings.TrimSpace(priority)
	if trimmed == bcPriorityHigh || strings.EqualFold(trimmed, bcPriorityOverride) {
		return vectorpb.ControlRequest_OVERRIDE_BEHAVIORS
	}
	if strings.EqualFold(trimmed, bcPriorityReserve) || strings.EqualFold(trimmed, bcPriorityReserveControl) {
		return vectorpb.ControlRequest_RESERVE_CONTROL
	}
	return vectorpb.ControlRequest_DEFAULT
}

func assumeBehaviorControl(robot *Robot, priority string) {
	if robot == nil || robot.Vector == nil {
		logger.Println("Behavior control error: no active robot connection")
		return
	}
	release, done, _ := robot.bc.start(robot.Ctx)
	go holdBehaviorControl(robot, bcPriority(priority), release, done)
}

func assumeBehaviorControlWait(reqCtx context.Context, robot *Robot, priority string, timeout time.Duration) error {
	if robot == nil || robot.Vector == nil {
		return errors.New("behavior control: no active robot connection")
	}
	return assumeControlWithOpener(reqCtx, &robot.bc, robot.Ctx, priority, timeout,
		func(ctx context.Context) (bcControlStream, error) {
			return robot.Vector.Conn.BehaviorControl(ctx)
		})
}

func assumeControlWithOpener(reqCtx context.Context, task *robotTask, parent context.Context,
	priority string, timeout time.Duration, open bcStreamOpener) error {
	if task == nil || open == nil {
		return errors.New("behavior control: invalid task or stream opener")
	}
	if timeout <= 0 {
		timeout = bcGrantWaitTimeout
	}
	if reqCtx == nil {
		reqCtx = context.Background()
	}
	if parent == nil {
		parent = context.Background()
	}
	grantCh := make(chan error, 1)
	notify := func(err error) {
		select {
		case grantCh <- err:
		default:
		}
	}
	release, done, _ := task.start(parent)
	go holdBehaviorControlWithNotify(parent, release, open, bcPriority(priority), done, notify)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-grantCh:
		if err != nil {
			task.stop()
			return err
		}
		return nil
	case <-reqCtx.Done():
		task.stop()
		return reqCtx.Err()
	case <-timer.C:
		task.stop()
		return errors.New("timed out waiting for behavior control grant")
	}
}

func releaseBehaviorControl(robot *Robot) {
	if robot != nil {
		robot.bc.stop()
	}
}

func holdBehaviorControl(robot *Robot, prio vectorpb.ControlRequest_Priority, release context.Context, done func()) {
	holdBehaviorControlWithNotify(robot.Ctx, release,
		func(ctx context.Context) (bcControlStream, error) {
			return robot.Vector.Conn.BehaviorControl(ctx)
		}, prio, done, nil)
}

func sendControlRelease(stream bcControlStream, streamCtx context.Context) {
	err := stream.Send(&vectorpb.BehaviorControlRequest{
		RequestType: &vectorpb.BehaviorControlRequest_ControlRelease{
			ControlRelease: &vectorpb.ControlRelease{},
		},
	})
	if err != nil && streamCtx.Err() == nil {
		logger.Println("Behavior control release error: ", err)
	}
}

func holdBehaviorControlWithNotify(parent, release context.Context, open bcStreamOpener,
	prio vectorpb.ControlRequest_Priority, done func(), onGrant func(error)) {
	defer done()
	notified := false
	notify := func(err error) {
		if !notified && onGrant != nil {
			notified = true
			onGrant(err)
		}
	}
	if parent == nil {
		parent = context.Background()
	}
	streamCtx, cancel := context.WithCancel(parent)
	defer cancel()
	stream, err := open(streamCtx)
	if err != nil {
		notify(err)
		logger.Println("Behavior control error: ", err)
		return
	}
	err = stream.Send(&vectorpb.BehaviorControlRequest{
		RequestType: &vectorpb.BehaviorControlRequest_ControlRequest{
			ControlRequest: &vectorpb.ControlRequest{Priority: prio},
		},
	})
	if err != nil {
		notify(err)
		logger.Println("Behavior control error: ", err)
		return
	}
	recvErr := make(chan error, 1)
	go pumpControlResponses(stream, recvErr, notify)
	select {
	case <-release.Done():
		notify(errors.New("behavior control released before grant"))
		sendControlRelease(stream, streamCtx)
	case err := <-recvErr:
		notify(err)
	}
}

func pumpControlResponses(stream bcControlStream, recvErr chan<- error, notify func(error)) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			recvErr <- err
			return
		}
		if resp != nil && resp.GetControlGrantedResponse() != nil {
			notify(nil)
		}
	}
}
