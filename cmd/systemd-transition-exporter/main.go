package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Yuri666/systemd-transition-exporter/internal/config"
	"github.com/Yuri666/systemd-transition-exporter/internal/engine"
	"github.com/Yuri666/systemd-transition-exporter/internal/metrics"
	"github.com/Yuri666/systemd-transition-exporter/internal/model"
	"github.com/Yuri666/systemd-transition-exporter/internal/systemd"
	"github.com/Yuri666/systemd-transition-exporter/internal/wal"
)

func main() {
	configPath:=flag.String("config","/etc/systemd-transition-exporter/config.yaml","configuration file");flag.Parse()
	cfg,err:=config.Load(*configPath);if err!=nil{log.Fatal(err)}
	ctx,stop:=signal.NotifyContext(context.Background(),os.Interrupt,syscall.SIGTERM);defer stop()
	eng:=engine.New();reg:=metrics.New()
	var eventLog *wal.WAL
	if cfg.WAL.Enabled{eventLog,err=wal.Open(cfg.WAL.Directory,cfg.WAL.Fsync);if err!=nil{log.Fatal(err)};defer eventLog.Close()}

	var mu sync.Mutex
	currentBootID:=""
	if currentBootID,err=systemd.BootID();err!=nil{log.Fatal(err)}
	bootTime,_:=systemd.BootTime()

	emit:=func(e model.Event) error{
		if eventLog!=nil{if err:=eventLog.Append(e);err!=nil{return err}}
		reg.Event(e)
		log.Printf("transition seq=%d service=%s state=%s timestamp_ms=%d source=%d",e.Sequence,e.Service,e.State,e.EventTimeUnixMS,e.Source)
		return nil
	}

	onSnapshot:=func(s model.UnitSnapshot)error{
		mu.Lock();defer mu.Unlock()
		if currentBootID!=""&&s.BootID!=currentBootID{
			// The host reboot itself is the beginning of downtime. Generate DOWN
			// for services that were UP before accepting the new boot snapshots.
			for _,e:=range eng.ApplyReboot(s.BootID,bootTime){if err:=emit(e);err!=nil{return err}}
			currentBootID=s.BootID
			bootTime,_=systemd.BootTime()
		}
		for _,e:=range eng.Apply(s){if err:=emit(e);err!=nil{return err}}
		reg.SetState(s.Service,currentState(s.ActiveState))
		return nil
	}

	mux:=http.NewServeMux();mux.HandleFunc("/metrics",reg.Handler);mux.HandleFunc("/health",func(w http.ResponseWriter,_ *http.Request){w.WriteHeader(http.StatusOK)});mux.HandleFunc("/ready",func(w http.ResponseWriter,_ *http.Request){w.WriteHeader(http.StatusOK)})
	server:=&http.Server{Addr:cfg.Server.Listen,Handler:mux}
	go func(){log.Printf("listening on %s",cfg.Server.Listen);if err:=server.ListenAndServe();err!=nil&&err!=http.ErrServerClosed{log.Printf("HTTP server: %v",err)}}()

	go func(){
		err:=systemd.RunResilient(ctx,cfg.Services,cfg.Systemd.ReconnectInterval,onSnapshot)
		if err!=nil&&err!=context.Canceled{log.Printf("systemd monitor stopped: %v",err);stop()}
	}()
	<-ctx.Done();_ = server.Shutdown(context.Background())
}

func currentState(active string)model.AvailabilityState{switch active{case "active","activating":return model.StateUp;case "inactive","failed","deactivating":return model.StateDown;default:return model.StateUnknown}}

type _ = time.Time
