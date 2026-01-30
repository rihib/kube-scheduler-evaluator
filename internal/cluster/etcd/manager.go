package etcd

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/consts"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type Manager interface {
	Delete(context.Context) error
	Seed() map[string]string
	SetSeed(context.Context) error
}

func NewManager(c consts.ConstsExternalMode) (Manager, error) {
	etcdClient, err := clientv3.New(
		clientv3.Config{
			Endpoints:   []string{c.EtcdURL().String()},
			DialTimeout: 2 * time.Second,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("etcd.NewManager: %w", err)
	}

	result, err := etcdClient.Get(context.Background(), c.EtcdPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("etcd.NewManager: %w", err)
	}
	seed := make(map[string]string)
	for _, v := range result.Kvs {
		seed[string(v.Key)] = string(v.Value)
	}

	return &manager{
		consts:     c,
		etcdClient: etcdClient,
		seed:       seed,
	}, nil
}

type manager struct {
	consts     consts.ConstsExternalMode
	etcdClient *clientv3.Client
	seed       map[string]string
}

func (m *manager) Delete(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, consts.DefaultShortTimeout)
	defer cancel()
	if _, err := m.etcdClient.Delete(ctx, m.consts.EtcdPrefix(), clientv3.WithPrefix()); err != nil {
		return fmt.Errorf("etcd.Delete: %w", err)
	}
	return nil
}

func (m *manager) Seed() map[string]string {
	return m.seed
}

func (m *manager) SetSeed(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	sem := semaphore.NewWeighted(int64(runtime.GOMAXPROCS(0)))
	for k, v := range m.seed {
		k, v := k, v
		g.Go(func() error {
			if err := sem.Acquire(ctx, 1); err != nil {
				return fmt.Errorf("etcd.SetSeed: %w", err)
			}
			defer sem.Release(1)
			if _, err := m.etcdClient.Put(ctx, k, v); err != nil {
				return fmt.Errorf("etcd.SetSeed: key: %s; value: %s; error: %w", k, v, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}
