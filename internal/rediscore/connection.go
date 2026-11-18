package rediscore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/redisclient"
)

type ConnectionConfig struct {
	Redis   redisclient.Options
	Profile ProfileConfig
}

// Connection owns only a lazy client and immutable compatibility metadata.
// It retains no identity key or local authoritative protocol state.
type Connection struct {
	client                                          *redis.Client
	root                                            string
	profile                                         []byte
	authority                                       *pb.StateAuthority
	recordBudget, namespaceContexts, namespaceBytes int64
	timeout                                         time.Duration
}

func NewConnection(cfg ConnectionConfig) (*Connection, error) {
	options, err := redisclient.Normalize(cfg.Redis)
	if err != nil {
		return nil, err
	}
	binding, err := BindProfile(cfg.Profile)
	if err != nil {
		return nil, err
	}
	contexts := cfg.Profile.NamespaceMaxContexts
	if contexts == 0 {
		contexts = DefaultNamespaceMaxContexts
	}
	namespaceBytes := cfg.Profile.NamespaceMaxBytes
	if namespaceBytes == 0 {
		namespaceBytes = DefaultNamespaceMaxBytes
	}
	return &Connection{client: redisclient.New(options, options.PoolSize, nil), root: cfg.Profile.Root, profile: bytes.Clone(binding.Value), authority: proto.Clone(binding.Authority).(*pb.StateAuthority), recordBudget: cfg.Profile.RecordBudget, namespaceContexts: contexts, namespaceBytes: namespaceBytes, timeout: options.IOTimeout}, nil
}

// AdmissionLimits are immutable profile bounds. The sole process-local phone
// owner uses the same numeric limits for its independent resident accounting.
func (c *Connection) AdmissionLimits() (int64, int64) { return c.namespaceContexts, c.namespaceBytes }

func (c *Connection) Authority() *pb.StateAuthority {
	if c == nil || c.authority == nil {
		return nil
	}
	return proto.Clone(c.authority).(*pb.StateAuthority)
}
func (c *Connection) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}

// Refresh probes one actual primary and compatibility profile atomically. An
// uncertain reply may have initialized the profile and must not be called a
// definite failure; subsequent observation may reconcile the same profile.
func (c *Connection) Refresh(ctx context.Context) (*Store, error) {
	if c == nil || c.client == nil {
		return nil, ErrAuthority
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	start := time.Now()
	deadline := start.Add(c.timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	callCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	low := start.UnixMilli() - workerClockTolerance.Milliseconds()
	high := start.UnixMilli() + max(int64(0), deadline.Sub(start).Milliseconds()) + workerClockTolerance.Milliseconds()
	keys := []string{c.root + "core:profile", c.root + "core:capacity", c.root + "core:selectors", c.root + "core:applications", c.root + "core:owner-expiry", c.root + "core:coordinator"}
	result, err := connectionProbeScript.Run(callCtx, c.client, keys, c.profile, low, high).Slice()
	finish := time.Now()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if err = status(result); err != nil {
		return nil, err
	}
	if len(result) != 3 {
		return nil, errors.Join(ErrUnknown, ErrCorrupt)
	}
	primary, ok := result[1].(string)
	now, e := responseInt(result[2])
	elapsed := finish.Sub(start)
	skew := finish.UnixMilli() - start.UnixMilli() - elapsed.Milliseconds()
	if !ok || !validPrimary(primary) || e != nil || elapsed < 0 || skew < -workerClockTolerance.Milliseconds() || skew > workerClockTolerance.Milliseconds() || now < low || now > finish.UnixMilli()+workerClockTolerance.Milliseconds() || ctx.Err() != nil || callCtx.Err() != nil {
		return nil, errors.Join(ErrUnknown, ErrClock)
	}
	return New(c.client, Config{Root: c.root, Profile: c.profile, RecordBudget: c.recordBudget, NamespaceMaxContexts: c.namespaceContexts, NamespaceMaxBytes: c.namespaceBytes, PrimaryRunID: primary})
}

var connectionProbeScript = redis.NewScript(`
local function fail(s) return {s} end
local function uint(s) if type(s)~='string' or #s==0 or #s>16 or not string.match(s,'^[0-9]+$') or (#s>1 and string.sub(s,1,1)=='0') then return nil end;local n=tonumber(s);if not n or n>9007199254740991 then return nil end;return n end
local server=redis.call('INFO','server');local id=string.match(server,'\r\nrun_id:([a-f0-9]+)\r\n');local replication=redis.call('INFO','replication')
if not id or #id~=40 or not string.find(replication,'\r\nrole:master\r\n',1,true) then return fail('AUTHORITY') end
local lo,hi=uint(ARGV[2]),uint(ARGV[3]);if not lo or not hi or hi<lo then return fail('CORRUPT') end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000);if now<lo or now>hi then return fail('EXPIRED') end
local typ=redis.call('TYPE',KEYS[1]).ok;if typ~='none' and typ~='string' then return fail('AUTHORITY') end
local profile=redis.call('GET',KEYS[1]);if profile and profile~=ARGV[1] then return fail('AUTHORITY') end
if not profile then
 for i=2,#KEYS do if redis.call('EXISTS',KEYS[i])~=0 then return fail('AUTHORITY') end end
 if not redis.acl_check_cmd('SET',KEYS[1],ARGV[1]) then return fail('AUTHORITY') end
 redis.call('SET',KEYS[1],ARGV[1])
end
return {'OK',id,string.format('%.0f',now)}
`)
