package systemd

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/godbus/dbus/v5"
	"github.com/Yuri666/systemd-transition-exporter/internal/model"
)

const (
	busName      = "org.freedesktop.systemd1"
	managerPath  = dbus.ObjectPath("/org/freedesktop/systemd1")
	managerIface = "org.freedesktop.systemd1.Manager"
	unitIface    = "org.freedesktop.systemd1.Unit"
	propertiesIF = "org.freedesktop.DBus.Properties"
	dbusIF       = "org.freedesktop.DBus"
	dbusPath     = dbus.ObjectPath("/org/freedesktop/DBus")
)

type DBus struct { conn *dbus.Conn }
func Connect(context.Context) (*DBus,error) { conn,err:=dbus.ConnectSystemBus(); if err!=nil{return nil,fmt.Errorf("connect system bus: %w",err)}; return &DBus{conn:conn},nil }
func (d *DBus) Close() error { if d!=nil&&d.conn!=nil{return d.conn.Close()}; return nil }
func (d *DBus) Conn()*dbus.Conn{return d.conn}

func BootID()(string,error){b,err:=os.ReadFile("/proc/sys/kernel/random/boot_id");if err!=nil{return "",fmt.Errorf("read boot_id: %w",err)};return strings.TrimSpace(string(b)),nil}

// BootTime returns the host boot time using CLOCK_BOOTTIME's /proc uptime
// together with wall-clock time. It is used to timestamp reboot downtime.
func BootTime()(time.Time,error){
b,err:=os.ReadFile("/proc/uptime");if err!=nil{return time.Time{},fmt.Errorf("read uptime: %w",err)}
	fields:=strings.Fields(string(b));if len(fields)==0{return time.Time{},fmt.Errorf("invalid /proc/uptime")}
	sec,err:=strconv.ParseFloat(fields[0],64);if err!=nil{return time.Time{},fmt.Errorf("parse uptime: %w",err)}
	return time.Now().Add(-time.Duration(sec*float64(time.Second))),nil
}

type Unit struct{conn *dbus.Conn;path dbus.ObjectPath;service string}
func(u *Unit)Path()dbus.ObjectPath{return u.path}
func(u *Unit)Service()string{return u.service}
func(u *Unit)Object()dbus.BusObject{return u.conn.Object(busName,u.path)}
func(d *DBus)LoadUnit(service string)(*Unit,error){call:=d.conn.Object(busName,managerPath).Call(managerIface+".LoadUnit",0,service);if call.Err!=nil{return nil,fmt.Errorf("LoadUnit(%s): %w",service,call.Err)};var path dbus.ObjectPath;if err:=call.Store(&path);err!=nil{return nil,fmt.Errorf("decode unit path: %w",err)};return &Unit{conn:d.conn,path:path,service:service},nil}

func(u *Unit)Snapshot(bootID string)(model.UnitSnapshot,error){obj:=u.Object();get:=func(name string)(interface{},error){v,err:=obj.GetProperty(unitIface+"."+name);if err!=nil{return nil,fmt.Errorf("get %s: %w",name,err)};return v.Value(),nil};active,err:=get("ActiveState");if err!=nil{return model.UnitSnapshot{},err};sub,err:=get("SubState");if err!=nil{return model.UnitSnapshot{},err};enter,err:=get("ActiveEnterTimestamp");if err!=nil{return model.UnitSnapshot{},err};exit,err:=get("ActiveExitTimestamp");if err!=nil{return model.UnitSnapshot{},err};enterMono,err:=get("ActiveEnterTimestampMonotonic");if err!=nil{return model.UnitSnapshot{},err};exitMono,err:=get("ActiveExitTimestampMonotonic");if err!=nil{return model.UnitSnapshot{},err};as,ok:=active.(string);if !ok{return model.UnitSnapshot{},fmt.Errorf("ActiveState has type %T",active)};ss,ok:=sub.(string);if !ok{return model.UnitSnapshot{},fmt.Errorf("SubState has type %T",sub)};return model.UnitSnapshot{Service:u.service,ActiveState:as,SubState:ss,ActiveEnterTimestampUS:asUint64(enter),ActiveExitTimestampUS:asUint64(exit),ActiveEnterTimestampMonotonicUS:asUint64(enterMono),ActiveExitTimestampMonotonicUS:asUint64(exitMono),BootID:bootID,ObservedAt:time.Now()},nil}
func asUint64(v interface{})uint64{switch x:=v.(type){case uint64:return x;case int64:if x>=0{return uint64(x)};case uint32:return uint64(x)};return 0}

type Monitor struct{dbus *DBus;byPath map[dbus.ObjectPath]*Unit}
func NewMonitor(d *DBus)*Monitor{return &Monitor{dbus:d,byPath:make(map[dbus.ObjectPath]*Unit)}}
func(m *Monitor)AddUnit(u *Unit){m.byPath[u.Path()]=u}
func(m *Monitor)Subscribe()error{args:="type='signal',interface='org.freedesktop.DBus.Properties',member='PropertiesChanged',path_namespace='/org/freedesktop/systemd1/unit'";if err:=m.dbus.conn.Object(dbusIF,dbusPath).Call(dbusIF+".AddMatch",0,args).Err;err!=nil{return fmt.Errorf("AddMatch: %w",err)};return nil}
func(m *Monitor)Run(ctx context.Context,handler func(*Unit)error)error{signals:=make(chan *dbus.Signal,256);m.dbus.conn.Signal(signals);defer m.dbus.conn.RemoveSignal(signals);for{select{case<-ctx.Done():return ctx.Err();case sig:=<-signals:if sig==nil{return fmt.Errorf("D-Bus signal channel closed")};if sig.Name!=propertiesIF+".PropertiesChanged"||len(sig.Body)<2{continue};u,ok:=m.byPath[sig.Path];if !ok{continue};iface,ok:=sig.Body[0].(string);if !ok||iface!=unitIface{continue};props,ok:=sig.Body[1].(map[string]dbus.Variant);if !ok||!interesting(props){continue};if err:=handler(u);err!=nil{return err}}}}
func interesting(props map[string]dbus.Variant)bool{for name:=range props{switch name{case "ActiveState","SubState","ActiveEnterTimestamp","ActiveExitTimestamp","ActiveEnterTimestampMonotonic","ActiveExitTimestampMonotonic":return true}};return false}
