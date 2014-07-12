package main

import (
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/alexzorin/libvirt-go"
	"github.com/dotcloud/docker/daemon/networkdriver/ipallocator"
	"github.com/flynn/flynn-host/containerinit"
	lt "github.com/flynn/flynn-host/libvirt"
	"github.com/flynn/flynn-host/logbuf"
	"github.com/flynn/flynn-host/pinkerton"
	"github.com/flynn/flynn-host/ports"
	"github.com/flynn/flynn-host/types"
	"github.com/flynn/go-iptables"
	"github.com/flynn/lumberjack"
	"github.com/technoweenie/grohl"
)

func NewLibvirtLXCBackend(state *State, logPath, initPath string) (Backend, error) {
	libvirtc, err := libvirt.NewVirConnection("lxc:///")
	if err != nil {
		return nil, err
	}

	iptables.RemoveExistingChain("FLYNN")
	chain, err := iptables.NewChain("FLYNN", "virbr0")
	if err != nil {
		return nil, err
	}

	return &LibvirtLXCBackend{
		LogPath:    logPath,
		InitPath:   initPath,
		libvirt:    libvirtc,
		state:      state,
		ports:      ports.NewAllocator(55000, 65535),
		forwarder:  ports.NewForwarder(net.ParseIP("0.0.0.0"), chain),
		logs:       make(map[string]*logbuf.Log),
		containers: make(map[string]*libvirtContainer),
	}, nil
}

type LibvirtLXCBackend struct {
	LogPath   string
	InitPath  string
	libvirt   libvirt.VirConnection
	state     *State
	ports     *ports.Allocator
	forwarder *ports.Forwarder

	logsMtx sync.Mutex
	logs    map[string]*logbuf.Log

	containersMtx sync.RWMutex
	containers    map[string]*libvirtContainer
}

type libvirtContainer struct {
	RootPath string
	IP       net.IP
	job      *host.Job
	l        *LibvirtLXCBackend
	done     chan struct{}
	*containerinit.Client
}

// TODO: read these from a configurable libvirt network
var defaultGW, defaultNet, _ = net.ParseCIDR("192.168.122.1/24")

const dockerBase = "/var/lib/docker"

type dockerImageConfig struct {
	User       string
	Env        []string
	Cmd        []string
	Entrypoint []string
	WorkingDir string
	Volumes    map[string]struct{}
}

func writeContainerEnv(path string, envs ...map[string]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var length int
	for _, e := range envs {
		length += len(e)
	}
	data := make([]string, 0, length)

	for _, e := range envs {
		for k, v := range e {
			data = append(data, k+"="+v)
		}
	}

	return json.NewEncoder(f).Encode(data)
}

func readDockerImageConfig(id string) (*dockerImageConfig, error) {
	res := &struct{ Config dockerImageConfig }{}
	f, err := os.Open(filepath.Join(dockerBase, "graph", id, "json"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(res); err != nil {
		return nil, err
	}
	return &res.Config, nil
}

func (l *LibvirtLXCBackend) Run(job *host.Job) (err error) {
	g := grohl.NewContext(grohl.Data{"backend": "libvirt-lxc", "fn": "run", "job.id": job.ID})
	g.Log(grohl.Data{"at": "start", "job.artifact.url": job.Artifact.URL, "job.cmd": job.Config.Cmd})

	ip, err := ipallocator.RequestIP(defaultNet, nil)
	if err != nil {
		g.Log(grohl.Data{"at": "request_ip", "status": "error", "err": err})
		return err
	}
	container := &libvirtContainer{
		l:    l,
		job:  job,
		IP:   *ip,
		done: make(chan struct{}),
	}
	defer func() {
		if err != nil {
			go container.cleanup()
		}
	}()

	g.Log(grohl.Data{"at": "pull_image"})
	layers, err := pinkerton.Pull(job.Artifact.URL)
	if err != nil {
		g.Log(grohl.Data{"at": "pull_image", "status": "error", "err": err})
		return err
	}
	imageID, err := pinkerton.ImageID(job.Artifact.URL)
	if err == pinkerton.ErrNoImageID && len(layers) > 0 {
		imageID = layers[len(layers)-1].ID
	} else if err != nil {
		g.Log(grohl.Data{"at": "image_id", "status": "error", "err": err})
		return err
	}

	g.Log(grohl.Data{"at": "read_config"})
	imageConfig, err := readDockerImageConfig(imageID)
	if err != nil {
		g.Log(grohl.Data{"at": "read_config", "status": "error", "err": err})
		return err
	}

	g.Log(grohl.Data{"at": "checkout"})
	rootPath, err := pinkerton.Checkout(job.ID, imageID)
	if err != nil {
		g.Log(grohl.Data{"at": "checkout", "status": "error", "err": err})
		return err
	}
	container.RootPath = rootPath

	g.Log(grohl.Data{"at": "mount"})
	if err := bindMount(l.InitPath, filepath.Join(rootPath, ".containerinit"), false, true); err != nil {
		g.Log(grohl.Data{"at": "mount", "status": "error", "err": err})
		return err
	}
	if err := os.MkdirAll(filepath.Join(rootPath, ".container-shared"), 0700); err != nil {
		g.Log(grohl.Data{"at": "mkdir", "dir": ".container-shared", "status": "error", "err": err})
		return err
	}

	if job.Config.Env == nil {
		job.Config.Env = make(map[string]string)
	}
	var i int
	for _, p := range job.Config.Ports {
		if p.Proto != "tcp" {
			continue
		}

		var port uint16
		if p.Port <= 0 {
			port, err = l.ports.Get()
		} else {
			port, err = l.ports.GetPort(uint16(p.Port))
		}
		if err != nil {
			g.Log(grohl.Data{"at": "alloc_port", "status": "error", "err": err})
			return err
		}
		p.Port = int(port)

		if i == 0 {
			job.Config.Env["PORT"] = strconv.Itoa(int(port))
		}
		job.Config.Env[fmt.Sprintf("PORT_%d", i)] = strconv.Itoa(int(port))
		i++
	}

	g.Log(grohl.Data{"at": "write_env"})
	if err := writeContainerEnv(filepath.Join(rootPath, ".containerenv"), job.Config.Env, map[string]string{"HOSTNAME": job.ID}); err != nil {
		g.Log(grohl.Data{"at": "write_env", "status": "error", "err": err})
		return err
	}

	args := []string{
		"-i", ip.String() + "/24",
		"-g", defaultGW.String(),
	}
	if job.Config.TTY {
		args = append(args, "-tty")
	}
	if job.Config.Stdin {
		args = append(args, "-stdin")
	}
	if job.Config.WorkingDir != "" {
		args = append(args, "-w", job.Config.WorkingDir)
	} else if imageConfig.WorkingDir != "" {
		args = append(args, "-w", imageConfig.WorkingDir)
	}
	if job.Config.Uid > 0 {
		args = append(args, "-u", strconv.Itoa(job.Config.Uid))
	} else if imageConfig.User != "" {
		// TODO: check and lookup user from image config
	}
	if len(job.Config.Entrypoint) > 0 {
		args = append(args, job.Config.Entrypoint...)
	} else {
		args = append(args, imageConfig.Entrypoint...)
	}
	if len(job.Config.Cmd) > 0 {
		args = append(args, job.Config.Cmd...)
	} else {
		args = append(args, imageConfig.Cmd...)
	}

	l.state.AddJob(job)
	l.state.SetInternalIP(job.ID, ip.String())
	domain := &lt.Domain{
		Type:   "lxc",
		Name:   job.ID,
		Memory: lt.UnitInt{Value: 1, Unit: "GiB"},
		VCPU:   1,
		OS: lt.OS{
			Type:     lt.OSType{Value: "exe"},
			Init:     "/.containerinit",
			InitArgs: args,
		},
		Devices: lt.Devices{
			Filesystems: []lt.Filesystem{{
				Type:   "mount",
				Source: lt.FSRef{Dir: rootPath},
				Target: lt.FSRef{Dir: "/"},
			}},
			Interfaces: []lt.Interface{{
				Type:   "network",
				Source: lt.InterfaceSrc{Network: "default"},
			}},
			Consoles: []lt.Console{{Type: "pty"}},
		},
		OnPoweroff: "preserve",
		OnCrash:    "preserve",
	}

	g.Log(grohl.Data{"at": "define_domain"})
	vd, err := l.libvirt.DomainDefineXML(string(domain.XML()))
	if err != nil {
		g.Log(grohl.Data{"at": "define_domain", "status": "error", "err": err})
		return err
	}

	g.Log(grohl.Data{"at": "create_domain"})
	if err := vd.Create(); err != nil {
		g.Log(grohl.Data{"at": "create_domain", "status": "error", "err": err})
		return err
	}
	uuid, err := vd.GetUUIDString()
	if err != nil {
		g.Log(grohl.Data{"at": "get_domain_uuid", "status": "error", "err": err})
		return err
	}
	g.Log(grohl.Data{"at": "get_uuid", "uuid": uuid})
	l.state.SetContainerID(job.ID, uuid)

	domainXML, err := vd.GetXMLDesc(0)
	if err != nil {
		g.Log(grohl.Data{"at": "get_domain_xml", "status": "error", "err": err})
		return err
	}
	domain = &lt.Domain{}
	if err := xml.Unmarshal([]byte(domainXML), domain); err != nil {
		g.Log(grohl.Data{"at": "unmarshal_domain_xml", "status": "error", "err": err})
		return err
	}

	if len(domain.Devices.Interfaces) == 0 || domain.Devices.Interfaces[0].Target == nil ||
		domain.Devices.Interfaces[0].Target.Dev == "" {
		err = errors.New("domain config missing interface")
		g.Log(grohl.Data{"at": "enable_hairpin", "status": "error", "err": err})
		return err
	}
	iface := domain.Devices.Interfaces[0].Target.Dev
	if err := enableHairpinMode(iface); err != nil {
		g.Log(grohl.Data{"at": "enable_hairpin", "status": "error", "err": err})
		return err
	}

	for _, p := range job.Config.Ports {
		if p.Proto != "tcp" {
			continue
		}
		if err := l.forwarder.Add(&net.TCPAddr{IP: *ip, Port: p.Port}); err != nil {
			g.Log(grohl.Data{"at": "forward_port", "port": p.Port, "status": "error", "err": err})
			return err
		}
	}

	go container.watch(nil)

	g.Log(grohl.Data{"at": "finish"})
	return nil
}

func enableHairpinMode(iface string) error {
	return ioutil.WriteFile(filepath.Join("/sys/class/net", iface, "brport/hairpin_mode"), []byte("1"), 0666)
}

func (l *LibvirtLXCBackend) openLog(id string) *logbuf.Log {
	l.logsMtx.Lock()
	defer l.logsMtx.Unlock()
	if _, ok := l.logs[id]; !ok {
		// TODO: configure retention and log size
		l.logs[id] = logbuf.NewLog(&lumberjack.Logger{Dir: filepath.Join(l.LogPath, id)})
	}
	// TODO: do reference counting and remove logs that are not in use from memory
	return l.logs[id]
}

func (c *libvirtContainer) watch(ready chan<- error) error {
	g := grohl.NewContext(grohl.Data{"backend": "libvirt-lxc", "fn": "watch_container", "job.id": c.job.ID})
	g.Log(grohl.Data{"at": "start", "job.id": c.job.ID})

	// We can't connect to the socket file directly because
	// the path to it is longer than 108 characters (UNIX_PATH_MAX).
	// Create a temporary symlink to connect to.
	symlink := "/tmp/containerinit-rpc." + c.job.ID
	os.Symlink(path.Join(c.RootPath, containerinit.SocketPath), symlink)
	defer os.Remove(symlink)

	var err error
	for startTime := time.Now(); time.Since(startTime) < time.Second; time.Sleep(time.Millisecond) {
		c.Client, err = containerinit.NewClient(symlink)
		if err == nil {
			break
		}
	}
	if ready != nil {
		ready <- err
	}
	if err != nil {
		g.Log(grohl.Data{"at": "connect", "status": "error", "err": err})
		return err
	}
	defer c.Client.Close()

	c.l.containersMtx.Lock()
	c.l.containers[c.job.ID] = c
	c.l.containersMtx.Unlock()
	defer func() {
		c.l.containersMtx.Lock()
		delete(c.l.containers, c.job.ID)
		c.l.containersMtx.Unlock()
		c.cleanup()
		close(c.done)
	}()

	if !c.job.Config.TTY {
		g.Log(grohl.Data{"at": "get_stdout"})
		stdout, stderr, err := c.Client.GetStdout()
		if err != nil {
			g.Log(grohl.Data{"at": "get_stdout", "status": "error", "err": err.Error()})
			return err
		}
		log := c.l.openLog(c.job.ID)
		defer log.Close()
		// TODO: log errors from these
		go log.ReadFrom(1, stdout)
		go log.ReadFrom(2, stderr)
	}

	g.Log(grohl.Data{"at": "watch_changes"})
	for change := range c.Client.StreamState() {
		g.Log(grohl.Data{"at": "change", "state": change.State.String()})
		if change.Error != "" {
			err := errors.New(change.Error)
			g.Log(grohl.Data{"at": "change", "status": "error", "err": err})
			c.l.state.SetStatusFailed(c.job.ID, err)
			return err
		}
		switch change.State {
		case containerinit.StateInitial:
			g.Log(grohl.Data{"at": "wait_attach"})
			c.l.state.WaitAttach(c.job.ID)
			g.Log(grohl.Data{"at": "resume"})
			c.Client.Resume()
		case containerinit.StateRunning:
			g.Log(grohl.Data{"at": "running"})
			c.l.state.SetStatusRunning(c.job.ID)
		case containerinit.StateExited:
			g.Log(grohl.Data{"at": "exited", "status": change.ExitStatus})
			c.Client.Resume()
			c.l.state.SetStatusDone(c.job.ID, change.ExitStatus)
			return nil
		case containerinit.StateFailed:
			g.Log(grohl.Data{"at": "failed"})
			c.Client.Resume()
			c.l.state.SetStatusFailed(c.job.ID, errors.New("container failed to start"))
			return nil
		}
	}
	g.Log(grohl.Data{"at": "unknown_failure"})
	c.l.state.SetStatusFailed(c.job.ID, errors.New("unknown failure"))

	return nil
}

func (c *libvirtContainer) cleanup() error {
	g := grohl.NewContext(grohl.Data{"backend": "libvirt-lxc", "fn": "cleanup", "job.id": c.job.ID})
	g.Log(grohl.Data{"at": "start"})

	if err := syscall.Unmount(filepath.Join(c.RootPath, ".containerinit"), 0); err != nil {
		g.Log(grohl.Data{"at": "unmount", "status": "error", "err": err})
	}
	if err := pinkerton.Cleanup(c.job.ID); err != nil {
		g.Log(grohl.Data{"at": "pinkerton", "status": "error", "err": err})
	}
	for _, p := range c.job.Config.Ports {
		if p.Proto != "tcp" {
			continue
		}
		if err := c.l.forwarder.Remove(&net.TCPAddr{IP: c.IP, Port: p.Port}); err != nil {
			g.Log(grohl.Data{"at": "iptables", "status": "error", "err": err, "port": p.Port})
		}
		c.l.ports.Put(uint16(p.Port))
	}
	ipallocator.ReleaseIP(defaultNet, &c.IP)
	g.Log(grohl.Data{"at": "finish"})
	return nil
}

func (l *LibvirtLXCBackend) Stop(id string) error {
	client := l.getContainer(id)
	if client == nil {
		return errors.New("unknown container")
	}
	// TODO: follow up with sigkill
	return client.Signal(int(syscall.SIGTERM))
}

func (l *LibvirtLXCBackend) getContainer(id string) *libvirtContainer {
	l.containersMtx.RLock()
	defer l.containersMtx.RUnlock()
	return l.containers[id]
}

func (l *LibvirtLXCBackend) Attach(req *AttachRequest) error {
	var stdout, stderr, stdin bool
	for _, s := range req.Streams {
		switch s {
		case "stdout":
			stdout = true
		case "stderr":
			stderr = true
		case "stdin":
			stdin = true
		}
	}

	client := l.getContainer(req.Job.Job.ID)
	if client == nil {
		return fmt.Errorf("missing job client (is the job running?)")
	}
	var stdoutDone chan struct{}
	if stdin {
		stdin, err := client.GetStdin()
		if err != nil {
			return err
		}
		stdoutDone = make(chan struct{})
		go func() {
			io.Copy(stdin, req)
			stdin.Close()
			close(stdoutDone)
		}()
	}
	if req.Job.Job.Config.TTY && (stdout || stderr) {
		stdout, _, err := client.GetStdout()
		if err != nil {
			return err
		}
		io.Copy(req, stdout)
		stdout.Close()
		<-stdoutDone
		return nil
	}

	log := l.openLog(req.Job.Job.ID)
	r := log.NewReader()
	defer r.Close()
	if !req.Logs {
		if err := r.SeekToEnd(); err != nil {
			return err
		}
	}

	if req.Attached != nil {
		close(req.Attached)
	}

	var header [8]byte
	for {
		line, err := r.ReadLine(req.Stream)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
		if !stdout && line.Stream == 1 || !stderr && line.Stream == 2 {
			continue
		}
		// TODO: get rid of docker framing
		header[0] = byte(line.Stream)
		binary.BigEndian.PutUint32(header[4:], uint32(len(line.Message)+1))
		if _, err := req.Write(header[:]); err != nil {
			return err
		}
		if _, err := req.Write([]byte(line.Message)); err != nil {
			return err
		}
		if _, err := req.Write([]byte{'\n'}); err != nil {
			return err
		}
	}
	<-stdoutDone
	return nil
}

func (l *LibvirtLXCBackend) Cleanup() error {
	l.containersMtx.Lock()
	ids := make([]string, 0, len(l.containers))
	chans := make([]chan struct{}, 0, len(l.containers))
	for id, c := range l.containers {
		ids = append(ids, id)
		chans = append(chans, c.done)
	}
	l.containersMtx.Unlock()
	for _, id := range ids {
		if err := l.Stop(id); err != nil {
			return err
		}
	}
	for _, done := range chans {
		<-done
	}
	return nil
}

func (l *LibvirtLXCBackend) RestoreState(jobs map[string]*host.ActiveJob, dec *json.Decoder) error {
	containers := make(map[string]*libvirtContainer)
	if err := dec.Decode(&containers); err != nil {
		return err
	}
	for _, j := range jobs {
		container, ok := containers[j.Job.ID]
		if !ok {
			continue
		}
		container.l = l
		container.job = j.Job
		container.done = make(chan struct{})
		status := make(chan error)
		go container.watch(status)
		if err := <-status; err != nil {
			// log error
			l.state.RemoveJob(j.Job.ID)
			container.cleanup()
			continue
		}
		l.containers[j.Job.ID] = container

		for _, p := range j.Job.Config.Ports {
			if p.Proto != "tcp" {
				continue
			}
			l.ports.GetPort(uint16(p.Port))
		}

	}
	return nil
}

func (l *LibvirtLXCBackend) SaveState(e *json.Encoder) error {
	l.containersMtx.RLock()
	defer l.containersMtx.RUnlock()
	return e.Encode(l.containers)
}

func bindMount(src, dest string, writeable, private bool) error {
	srcStat, err := os.Stat(src)
	if err != nil {
		return err
	}
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if srcStat.IsDir() {
			if err := os.MkdirAll(dest, 0755); err != nil {
				return err
			}
		} else {
			if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
				return err
			}
			f, err := os.OpenFile(dest, os.O_CREATE, 0755)
			if err != nil {
				return err
			}
			f.Close()
		}
	} else if err != nil {
		return err
	}

	flags := syscall.MS_BIND | syscall.MS_REC
	if !writeable {
		flags |= syscall.MS_RDONLY
	}

	if err := syscall.Mount(src, dest, "bind", uintptr(flags), ""); err != nil {
		return err
	}
	if private {
		if err := syscall.Mount("", dest, "none", uintptr(syscall.MS_PRIVATE), ""); err != nil {
			return err
		}
	}
	return nil
}
