package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"sipbridge/internal/buildinfo"
	"sipbridge/internal/capture"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/engine"
	"sipbridge/internal/state"
	"sipbridge/internal/storagecodec"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	version := flag.Bool("version", false, "print build version and exit")
	stateSchema := flag.Bool("state-schema", false, "print private retained storage receipt and exit")
	private := flag.String("control-listen", ":8443", "private control listener")
	controlAuth := flag.String("control-auth", "mtls", "control authentication: mtls or bearer")
	controlTransport := flag.String("control-transport", "tls", "control transport: tls or explicit loopback-only plain")
	controlToken := flag.String("control-bearer-token-file", "", "bearer control token file")
	controlPrincipal := flag.String("control-principal", "", "stable bearer controller principal")
	plain := flag.String("sip-plain-listen", ":5060", "public SIP UDP/TCP listener; empty disables")
	public := flag.String("sip-listen", ":5061", "public SIP TLS listener")
	captureURL := flag.String("sip-capture-url", "", "optional edge-local SIP capture collector URL")
	sipInterface := flag.String("sip-interface", "", "optional IPv4 interface for public SIP listeners and network-target source")
	admin := flag.String("admin-listen", "127.0.0.1:8800", "local health listener")
	cert := flag.String("control-cert", "", "control server certificate")
	key := flag.String("control-key", "", "control key")
	ca := flag.String("client-ca", "", "controller client trust")
	allowed := flag.String("allowed-client-uri", "", "exact comma-separated controller URI identities")
	sipCert := flag.String("sip-cert", "", "public SIP certificate")
	sipKey := flag.String("sip-key", "", "public SIP key")
	initialController := flag.String("initial-controller", "", "optional preferred first-hop controller instance; owner redirects still apply")
	instance := flag.String("instance", "", "inventory instance ID")
	advertise := flag.String("advertise", "", "public SIP host:port")
	advertisePlain := flag.String("advertise-plain", "", "native public UDP/TCP host:port; default advertised host and plain listener port")
	contact := flag.String("contact", "", "public Contact URI")
	allowRegister := flag.Bool("allow-register", true, "allow inbound REGISTER requests")
	allowSubscribe := flag.Bool("allow-subscribe", true, "allow inbound SUBSCRIBE requests")
	requiredAuth := flag.String("require-auth-methods", "", "locally challenge initial REGISTER, INVITE and/or SUBSCRIBE (comma separated)")
	flag.Parse()
	if *stateSchema {
		_, err := os.Stdout.Write(storagecodec.Receipt())
		return err
	}
	if *version {
		fmt.Printf("sipbridge %s\n", buildinfo.Version)
		return nil
	}
	methodPolicy := engine.MethodPolicy{DisableRegister: !*allowRegister, DisableSubscribe: !*allowSubscribe}
	authPolicy, err := engine.ParseRequiredAuthMethods(*requiredAuth, methodPolicy)
	if err != nil {
		return err
	}
	stateConfig, err := loadStateConfiguration()
	if err != nil {
		return err
	}
	stateStore, err := state.OpenBackend(*stateConfig)
	if err != nil {
		return err
	}
	defer stateStore.Close()

	var sourceIP net.IP
	if *sipInterface != "" {
		iface, err := net.InterfaceByName(*sipInterface)
		if err != nil {
			return err
		}
		addresses, err := iface.Addrs()
		if err != nil {
			return err
		}
		sourceIP, err = interfaceIPv4(addresses)
		if err != nil {
			return err
		}
		*public, err = bindSIPListener(*public, sourceIP)
		if err != nil {
			return err
		}
		if *plain != "" {
			*plain, err = bindSIPListener(*plain, sourceIP)
			if err != nil {
				return err
			}
		}
		if *advertise == "" {
			*advertise = *public
		}
	}
	if *instance == "" || *advertise == "" {
		return fmt.Errorf("instance and public advertisement are required")
	}
	if *contact == "" {
		*contact = "sip:sipbridge@" + *advertise + ";transport=tls"
	}
	plainAdvertisement, err := derivePlainAdvertisement(*advertise, *plain, *advertisePlain)
	if err != nil {
		return err
	}
	if *captureURL != "" && *plain != "" {
		host, _, splitErr := net.SplitHostPort(*plain)
		ip := net.ParseIP(host)
		if splitErr != nil || ip == nil || ip.IsUnspecified() {
			return fmt.Errorf("SIP capture requires UDP listener bound to a specific IP address")
		}
	}
	controlSecurity, err := loadControlSecurity(*controlAuth, *controlTransport, *controlToken, *controlPrincipal, *allowed, control.TLSFiles{Certificate: *cert, Key: *key, Trust: *ca})
	if err != nil {
		return err
	}
	publicTLS, err := control.NewPublicTLSContext(control.TLSFiles{Certificate: *sipCert, Key: *sipKey})
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	process := &pb.ProcessRef{InstanceId: *instance, Epoch: uuid.NewString()}
	var sipCapture *capture.Exporter
	if *captureURL != "" {
		sipCapture, err = capture.New(*captureURL, process.InstanceId, process.Epoch)
		if err != nil {
			return err
		}
		captureDone := make(chan struct{})
		go func() { defer close(captureDone); sipCapture.Run(ctx) }()
		defer func() {
			cancel()
			select {
			case <-captureDone:
			case <-time.After(3 * time.Second):
				fmt.Fprintln(os.Stderr, "SIP capture shutdown timed out")
			}
		}()
	}
	var stateNative *engine.StateEngine
	var ingress *engine.Ingress
	capabilities := []string{engine.Capability, control.CommandBudgetCapability, engine.DialogContinuityCapability,
		control.StateCapability, control.NativeCompletionCapability, control.CommandAdmissionCapability, control.ControllerReattachCapability}
	if authPolicy != nil {
		capabilities = append(capabilities, engine.LocalChallengeCapability)
	}
	a := stateStore.Authority()
	authority := &pb.StateAuthority{Service: stateStore.Namespace(), Generation: a.Generation, PrimaryId: a.RunID, SchemaVersion: 1}
	server, err := control.NewServer(control.Config{Process: process, TLS: controlSecurity.TLS, Authenticator: controlSecurity.Authenticator, AllowPlain: controlSecurity.AllowPlain, AllowedURIs: controlSecurity.AllowedURIs, Capabilities: capabilities, StateAuthority: authority, InboundAuthPolicy: authPolicy, OnMessage: func(peer *control.Peer, envelope *pb.Envelope) error {
		if envelope.GetWrite() != nil {
			return ingress.Write(peer, envelope)
		}
		if envelope.GetInspectEffect() != nil {
			return ingress.InspectEffect(peer, envelope)
		}
		return stateNative.Handle(peer, envelope)
	}, OnApplicationLease: func(ctx context.Context, application *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
		if stateNative == nil {
			return nil, errors.New("native application authority unavailable")
		}
		return stateNative.ApplicationLease(ctx, application, term)
	}, OnControllerSession: func(ctx context.Context, peer *control.Peer, hello *pb.SessionHello) (*pb.ApplicationLease, error) {
		if stateNative == nil {
			return nil, errors.New("native application authority unavailable")
		}
		return stateNative.ControllerSession(ctx, peer, hello)
	}, OnControllerRenew: func(ctx context.Context, peer *control.Peer) (*pb.ApplicationLease, error) {
		if stateNative == nil {
			return nil, errors.New("native application authority unavailable")
		}
		return stateNative.ControllerRenew(ctx, peer)
	}, OnStatus: func(peer *control.Peer, status *pb.SessionStatus) error {
		if status.DigestChallengePolicy != nil {
			if !peer.HasCapability(engine.LocalChallengeCapability) {
				return errors.New("local challenge policy not negotiated")
			}
			if err := ingress.UpdateChallengePolicy(status.DigestChallengePolicy); err != nil {
				return err
			}
		}
		if status.RetainedReady && status.BindingsReady {
			return stateNative.ControllerReady(peer.Context(), peer)
		}
		return nil
	},
		OnDiagnostic:  func(diagnostic control.Diagnostic) { _ = json.NewEncoder(os.Stdout).Encode(diagnostic) },
		OnReloadError: func(error) { fmt.Fprintln(os.Stderr, "private credentials invalid; retaining previous context") }})
	if err != nil {
		return err
	}
	ingress = engine.NewIngress(process, server, *advertise, 4096, engine.IngressOptions{PreferredController: *initialController, SourceIP: sourceIP, Capture: sipCapture,
		MethodPolicy: methodPolicy, AuthPolicy: authPolicy})
	engineConfig := engine.Config{Process: process, AdvertiseAddress: *advertise, PlainAdvertiseAddress: plainAdvertisement, ContactURI: *contact, OpenNetwork: ingress.OpenNetwork, LiveFlow: ingress.LiveFlow, StateIdentityKey: stateConfig.NamespaceKey}
	engineConfig.WriteCommitted, engineConfig.InspectCommitted = ingress.WriteCommitted, ingress.InspectCommitted
	stateNative, err = engine.NewState(ctx, engineConfig, stateStore, server)
	if err != nil {
		return err
	}
	defer stateNative.Close()
	if err := ingress.ConfigureStateRuntime(stateNative, stateNative.LocateRoute, stateNative.Reject); err != nil {
		return err
	}
	ingress.ConfigureNativeCompletion(stateNative.TryNativeCompletion)
	controlListener, err := net.Listen("tcp", *private)
	if err != nil {
		return err
	}
	defer controlListener.Close()
	sipListener, err := net.Listen("tcp", *public)
	if err != nil {
		return err
	}
	defer sipListener.Close()
	var serving atomic.Bool
	adminServer := &http.Server{Addr: *admin, ReadHeaderTimeout: 2 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			storagecodec.WriteMetrics(w)
			writeRuntimeMetrics(w)
			stats := ingress.Statistics()
			fmt.Fprintf(w, "zt_sipbridge_ingress_rejected_total %d\nzt_sipbridge_tls_handshake_failures_total %d\nzt_sipbridge_invalid_messages_total %d\n", stats.FlowRejected, stats.TLSFailed, stats.InvalidMessages)
			fmt.Fprintf(w, "zt_sipbridge_method_rejected_total %d\n", stats.MethodRejected)
			scheduler := stateNative.SchedulerStatistics()
			leader := 0
			if scheduler.Leader {
				leader = 1
			}
			fmt.Fprintf(w, "zt_sipbridge_scheduler_leader %d\nzt_sipbridge_scheduler_acquisitions_total %d\nzt_sipbridge_scheduler_head_reads_total %d\nzt_sipbridge_scheduler_hint_batches_total %d\nzt_sipbridge_scheduler_wakeups_total %d\nzt_sipbridge_scheduler_repair_pages_total %d\n",
				leader, scheduler.Acquisitions, scheduler.HeadReads, scheduler.HintBatches, scheduler.Wakeups, scheduler.RepairPages)
			for _, failure := range scheduler.Failures {
				fmt.Fprintf(w, "zt_sipbridge_scheduler_failures_total{stage=%q,class=%q} %d\n", failure.Stage, failure.Class, failure.Count)
			}
			fmt.Fprintf(w, "zt_sipbridge_worker_head_reads_total %d\nzt_sipbridge_worker_hint_batches_total %d\nzt_sipbridge_worker_wakeups_total %d\nzt_sipbridge_worker_owned_groups_total %d\n",
				scheduler.WorkerHeadReads, scheduler.WorkerBatches, scheduler.WorkerWakeups, scheduler.OwnedGroups)
			for _, statistic := range stateNative.ExecutionStatistics() {
				fmt.Fprintf(w, "zt_sipbridge_execution_total{input=%q,event=%q} %d\n", statistic.Kind, statistic.Event, statistic.Count)
			}
			if sipCapture != nil {
				dropped, failures := sipCapture.Stats()
				fmt.Fprintf(w, "zt_sipbridge_capture_dropped_total %d\nzt_sipbridge_capture_export_failures_total %d\n", dropped, failures)
			}
			return
		}
		if r.URL.Path == "/healthz" {
			w.WriteHeader(200)
			return
		}
		if r.URL.Path == "/sip-servingz" {
			if !serving.Load() || ctx.Err() != nil || !publicTLS.Valid() {
				http.Error(w, "SIP listeners unavailable", 503)
				return
			}
			w.WriteHeader(200)
			return
		}
		if r.URL.Path != "/sip-readyz" {
			http.NotFound(w, r)
			return
		}
		if len(server.ReadyPeers()) == 0 || !server.CredentialsValid() || !publicTLS.Valid() || stateNative.Ready() != nil {
			http.Error(w, "signaling unavailable", 503)
			return
		}
		if authPolicy != nil && !ingress.ChallengeReady() {
			http.Error(w, "authentication policy unavailable", 503)
			return
		}
		w.WriteHeader(200)
	})}
	defer adminServer.Close()
	go func() {
		if err := adminServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			cancel()
		}
	}()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if publicTLS.Reload() != nil {
					fmt.Fprintln(os.Stderr, "public credentials invalid; retaining previous context")
				}
			}
		}
	}()
	errors := make(chan error, 4)
	if *plain != "" {
		udp, err := net.ListenPacket("udp", *plain)
		if err != nil {
			return err
		}
		defer udp.Close()
		tcp, err := net.Listen("tcp", *plain)
		if err != nil {
			return err
		}
		defer tcp.Close()
		go func() { errors <- ingress.ServeUDP(ctx, udp) }()
		go func() { errors <- ingress.Serve(ctx, tcp, nil) }()
	}
	go func() { errors <- server.Serve(ctx, controlListener) }()
	go func() { errors <- ingress.Serve(ctx, sipListener, publicTLS.Config()) }()
	serving.Store(true)
	defer serving.Store(false)
	_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"event": "sipbridge-listening", "instance": process.InstanceId, "epoch": process.Epoch, "control": controlListener.Addr().String(), "sip": sipListener.Addr().String(), "profile": control.StateCapability, "stateBackend": stateConfig.Backend, "version": buildinfo.Version, "revision": buildinfo.Revision})
	select {
	case err := <-errors:
		cancel()
		return err
	case <-ctx.Done():
		return nil
	}
}
