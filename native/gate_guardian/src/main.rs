//! External process ownership for Orris. The line protocol is defined in
//! docs/contracts/gate-guardian-protocol.org. This is a single-threaded port
//! program: the forked child only prepares descriptors, waits for GO, and execs.
use libc::{c_int, pid_t};
use std::{
    ffi::CString,
    io,
    os::unix::ffi::OsStrExt,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
mod facts;

static TERMINATED: AtomicBool = AtomicBool::new(false);
extern "C" fn on_term(_: c_int) {
    TERMINATED.store(true, Ordering::Relaxed);
}
fn fault(name: &str) -> bool {
    cfg!(feature = "testing") && std::env::var("GATE_GUARDIAN_FAULT").is_ok_and(|v| v == name)
}
fn sleep(ms: u64) {
    std::thread::sleep(Duration::from_millis(ms));
}
fn errno() -> i32 {
    io::Error::last_os_error().raw_os_error().unwrap_or(0)
}
fn close(fd: c_int) {
    unsafe {
        libc::close(fd);
    }
}
fn write_all(fd: c_int, mut bytes: &[u8]) -> bool {
    let end = Instant::now() + Duration::from_secs(2);
    while !bytes.is_empty() {
        // SAFETY: bytes remains valid for its length throughout this call.
        let n = unsafe { libc::write(fd, bytes.as_ptr().cast(), bytes.len()) };
        if n > 0 {
            bytes = &bytes[n as usize..];
        } else if n < 0 && errno() == libc::EINTR {
            continue;
        } else if n < 0
            && [libc::EAGAIN, libc::EWOULDBLOCK].contains(&errno())
            && Instant::now() < end
        {
            sleep(5);
        } else {
            return false;
        }
    }
    true
}
fn read(fd: c_int, buf: &mut [u8]) -> isize {
    // SAFETY: the mutable buffer is valid for its supplied length.
    unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) }
}
fn poll(fd: c_int, timeout: c_int) -> c_int {
    let mut p = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    unsafe { libc::poll(&mut p, 1, timeout) }
}
fn signal_fact(pid: pid_t) -> &'static str {
    if unsafe { libc::kill(pid, 0) } == 0 {
        "alive"
    } else if errno() == libc::ESRCH {
        "gone"
    } else {
        "unknown"
    }
}
fn decimal(s: &str, max_len: usize) -> Option<u64> {
    (!s.is_empty() && s.len() <= max_len && s.bytes().all(|b| b.is_ascii_digit()))
        .then(|| s.parse().ok())
        .flatten()
}
fn pid(s: &str) -> Option<pid_t> {
    decimal(s, 10)
        .filter(|&n| n > 0 && n <= pid_t::MAX as u64)
        .map(|n| n as pid_t)
}
fn bounded(s: &str, min: u64, max: u64) -> Option<u64> {
    decimal(s, 9).filter(|&n| n >= min && n <= max)
}

#[derive(Default)]
struct Guardian {
    worker: pid_t,
    group_proven: bool,
    exit: Option<(&'static str, i32)>,
    broken: bool,
    created: Vec<CString>,
}
struct Settlement {
    settled: bool,
    leftovers: String,
    proof: &'static str,
}
impl Guardian {
    fn say(&mut self, line: &str) {
        if !self.broken {
            self.broken = !write_all(1, format!("{line}\n").as_bytes());
        }
    }
    fn observe(&mut self) {
        if self.exit.is_some() {
            return;
        }
        // SAFETY: zeroed siginfo_t is valid output storage. WNOWAIT retains our
        // exact child identity until settlement; no other code reaps this child.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let rc = unsafe {
            libc::waitid(
                libc::P_PID,
                self.worker as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if rc == 0 && unsafe { info.si_pid() } == self.worker {
            self.exit = Some((
                if info.si_code == libc::CLD_EXITED {
                    "exited"
                } else {
                    "signaled"
                },
                unsafe { info.si_status() },
            ));
        }
    }
    fn members(&self) -> Option<usize> {
        if self.group_proven {
            facts::members(self.worker)
        } else {
            Some(if self.exit.is_some() { 1 } else { 2 })
        }
    }
    fn settle(&mut self, force: bool, ms: u64, rounds: u64) -> Settlement {
        for r in 0..rounds {
            if force || r > 0 {
                let target = if self.group_proven {
                    -self.worker
                } else {
                    self.worker
                };
                unsafe {
                    libc::kill(target, if r == 0 { libc::SIGTERM } else { libc::SIGKILL });
                }
            }
            let end = Instant::now() + Duration::from_millis(ms);
            loop {
                self.observe();
                if self.exit.is_some() && self.members().is_some_and(|m| m <= 1) {
                    break;
                }
                if Instant::now() > end {
                    break;
                }
                sleep(20);
            }
            self.observe();
            if self.exit.is_some() && self.members().is_some_and(|m| m <= 1) {
                break;
            }
        }
        self.observe();
        if self.exit.is_none() {
            return Settlement {
                settled: false,
                leftovers: "unknown".into(),
                proof: "alive",
            };
        }
        // Only an observed exited child is reaped. EINTR does not end ownership.
        loop {
            let rc = unsafe { libc::waitpid(self.worker, std::ptr::null_mut(), 0) };
            if rc == self.worker {
                break;
            }
            if errno() != libc::EINTR {
                return Settlement {
                    settled: false,
                    leftovers: "unknown".into(),
                    proof: "unknown",
                };
            }
        }
        if !self.group_proven {
            return Settlement {
                settled: true,
                leftovers: "0".into(),
                proof: "gone",
            };
        }
        let members = facts::members(self.worker);
        let proof = signal_fact(-self.worker);
        Settlement {
            settled: proof == "gone" && members == Some(0),
            leftovers: members.map_or("unknown".into(), |n| n.to_string()),
            proof,
        }
    }
    fn finish(&mut self, head: &str, force: bool, opts: &Options) -> i32 {
        let s = self.settle(force, opts.settle_ms, opts.rounds);
        let kind = match self.exit {
            Some(("exited", n)) => format!("kind=exited status={n}"),
            Some((_, n)) => format!("kind=signaled signal={n}"),
            None => "kind=unknown".into(),
        };
        self.say(&format!(
            "{head} {kind} settled={} leftovers={} proof={} escaped=unknown",
            u8::from(s.settled),
            s.leftovers,
            s.proof
        ));
        if s.settled {
            0
        } else {
            3
        }
    }
    fn fail(&mut self, class: &str, ms: u64) -> ! {
        let s = if self.worker > 0 {
            Some(self.settle(true, ms, 2))
        } else {
            None
        };
        let mut removed = true;
        for path in &self.created {
            if unsafe { libc::unlink(path.as_ptr()) } != 0 && errno() != libc::ENOENT {
                removed = false;
            }
        }
        let cleanup = if self.created.is_empty() {
            "none"
        } else if removed {
            "removed"
        } else {
            "left"
        };
        if let Some(s) = s {
            self.say(&format!(
                "SETUP_FAILED class={class} settled={} leftovers={} proof={} cleanup={cleanup}",
                u8::from(s.settled),
                s.leftovers,
                s.proof
            ));
        } else {
            self.say(&format!("SETUP_FAILED class={class} cleanup={cleanup}"));
        }
        std::process::exit(1);
    }
}
struct Options {
    out: CString,
    err: CString,
    cwd: CString,
    ready_ms: u64,
    settle_ms: u64,
    rounds: u64,
    timeout: u64,
    command: Vec<CString>,
}
impl Options {
    fn parse(args: &[std::ffi::OsString]) -> Option<Self> {
        let (mut out, mut err, mut cwd) = (None, None, None);
        let (mut ready_ms, mut settle_ms, mut rounds, mut timeout) = (5000, 3000, 2, 0);
        let mut i = 1;
        while i < args.len() {
            if args[i] == "--" {
                i += 1;
                break;
            }
            let key = args[i].to_str()?;
            let val = args.get(i + 1)?;
            match key {
                "--stdout" => out = Some(CString::new(val.as_bytes()).ok()?),
                "--stderr" => err = Some(CString::new(val.as_bytes()).ok()?),
                "--cwd" => cwd = Some(CString::new(val.as_bytes()).ok()?),
                "--ready-ms" => ready_ms = bounded(val.to_str()?, 100, 600000)?,
                "--settle-ms" => settle_ms = bounded(val.to_str()?, 100, 600000)?,
                "--rounds" => rounds = bounded(val.to_str()?, 2, 10)?,
                "--timeout-ms" => timeout = bounded(val.to_str()?, 100, 86400000)?,
                _ => return None,
            }
            i += 2;
        }
        if i >= args.len() {
            return None;
        }
        let command = args[i..]
            .iter()
            .map(|s| CString::new(s.as_bytes()).ok())
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            out: out?,
            err: err?,
            cwd: cwd?,
            ready_ms,
            settle_ms,
            rounds,
            timeout,
            command,
        })
    }
}
fn pipe() -> Option<[c_int; 2]> {
    let mut fds = [-1; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return None;
    }
    for fd in fds {
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            close(fds[0]);
            close(fds[1]);
            return None;
        }
    }
    Some(fds)
}
// SAFETY: called only in the fork child of this single-threaded program. CStrings
// and argv pointers were prepared before fork and outlive execvp. No other thread
// can hold a Rust allocator lock. _exit prevents parent destructors from running.
unsafe fn child(
    opts: &Options,
    argv: &[*const libc::c_char],
    token: [c_int; 2],
    status: [c_int; 2],
    out: c_int,
    err: c_int,
    delay: bool,
    delay_path: Option<&CString>,
) -> ! {
    close(token[1]);
    close(status[0]);
    for sig in [libc::SIGPIPE, libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
        libc::signal(sig, libc::SIG_DFL);
    }
    if let Some(path) = delay_path {
        let fd = libc::open(
            path.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
            0o600,
        );
        if fd >= 0 {
            let text = format!("{}\n", libc::getpid());
            libc::write(fd, text.as_ptr().cast(), text.len());
            close(fd);
        }
    }
    if delay {
        libc::sleep(5);
    }
    if libc::setsid() < 0 {
        libc::write(status[1], b"F setsid\n".as_ptr().cast(), 9);
        libc::_exit(111);
    }
    if libc::chdir(opts.cwd.as_ptr()) != 0 {
        libc::write(status[1], b"F chdir\n".as_ptr().cast(), 8);
        libc::_exit(111);
    }
    let null = libc::open(c"/dev/null".as_ptr(), libc::O_RDONLY);
    if null < 0 || libc::dup2(null, 0) < 0 || libc::dup2(out, 1) < 0 || libc::dup2(err, 2) < 0 {
        libc::write(status[1], b"F redirect\n".as_ptr().cast(), 11);
        libc::_exit(111);
    }
    close(null);
    close(out);
    close(err);
    libc::write(status[1], b"R\n".as_ptr().cast(), 2);
    let mut t = [0; 3];
    if read(token[0], &mut t) != 3 || t != *b"GO\n" {
        libc::_exit(112);
    }
    libc::execvp(argv[0], argv.as_ptr());
    libc::_exit(127);
}
fn probe(g: &mut Guardian, args: &[std::ffi::OsString]) -> i32 {
    let ids = if args.len() == 4 {
        args[2]
            .to_str()
            .and_then(pid)
            .zip(args[3].to_str().and_then(pid))
    } else {
        None
    };
    let Some((pid, pgid)) = ids else {
        g.say("SETUP_FAILED class=usage");
        return 2;
    };
    let mut leader = signal_fact(pid);
    let mut identity = "-".into();
    if leader == "alive" {
        if let Some(s) = facts::identity(pid) {
            identity = s;
        } else {
            leader = "unknown";
        }
    }
    let group = if fault("probe_group_eperm") {
        "unknown"
    } else {
        signal_fact(-pgid)
    };
    let members = facts::members(pgid).map_or("unknown".into(), |n| n.to_string());
    g.say(&format!(
        "PROBE leader={leader} start={identity} group={group} members={members}"
    ));
    0
}
fn run() -> i32 {
    let started = Instant::now();
    let mut g = Guardian::default();
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
        let flags = libc::fcntl(1, libc::F_GETFL);
        if flags >= 0 {
            libc::fcntl(1, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
    }
    let args: Vec<_> = std::env::args_os().collect();
    if args.get(1).is_some_and(|s| s == "--probe") {
        return probe(&mut g, &args);
    }
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = on_term as *const () as usize;
        libc::sigemptyset(&mut sa.sa_mask);
        for sig in [libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
            libc::sigaction(sig, &sa, std::ptr::null_mut());
        }
    }
    let Some(opts) = Options::parse(&args) else {
        g.say("SETUP_FAILED class=usage");
        return 2;
    };
    let out = unsafe {
        libc::open(
            opts.out.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )
    };
    if out < 0 {
        g.fail("open_stdout", opts.settle_ms);
    }
    g.created.push(opts.out.clone());
    let err = unsafe {
        libc::open(
            opts.err.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )
    };
    if err < 0 {
        g.fail("open_stderr", opts.settle_ms);
    }
    g.created.push(opts.err.clone());
    let token = pipe().unwrap_or_else(|| g.fail("pipe", opts.settle_ms));
    let status = pipe().unwrap_or_else(|| g.fail("pipe", opts.settle_ms));
    let mut argv: Vec<_> = opts.command.iter().map(|s| s.as_ptr()).collect();
    argv.push(std::ptr::null());
    let delay = fault("setsid_delay");
    let delay_path = if delay {
        std::env::var_os("GATE_GUARDIAN_FAULT_DIR").and_then(|p| {
            CString::new(
                std::path::PathBuf::from(p)
                    .join("fault-child-pid")
                    .as_os_str()
                    .as_bytes(),
            )
            .ok()
        })
    } else {
        None
    };
    g.worker = unsafe { libc::fork() };
    if g.worker < 0 {
        g.fail("fork", opts.settle_ms);
    }
    if g.worker == 0 {
        unsafe {
            child(
                &opts,
                &argv,
                token,
                status,
                out,
                err,
                delay,
                delay_path.as_ref(),
            )
        }
    }
    close(token[0]);
    close(status[1]);
    close(out);
    close(err);
    if poll(status[0], opts.ready_ms as i32) <= 0 {
        g.fail("ready_timeout", opts.settle_ms);
    }
    let mut reply = [0; 31];
    let n = read(status[0], &mut reply);
    close(status[0]);
    if n <= 0 || reply[0] != b'R' {
        let class = if n > 2 && reply[0] == b'F' {
            std::str::from_utf8(&reply[2..n as usize])
                .unwrap_or("ready")
                .trim_end_matches('\n')
        } else {
            "ready"
        };
        g.fail(class, opts.settle_ms);
    }
    g.group_proven = true;
    let identity = facts::identity(g.worker).unwrap_or_else(|| g.fail("identity", opts.settle_ms));
    g.created.clear();
    g.say(&format!(
        "READY guardian={} worker={} pgid={} start={identity}",
        unsafe { libc::getpid() },
        g.worker,
        g.worker
    ));
    let expired = || opts.timeout > 0 && started.elapsed().as_millis() >= opts.timeout as u128;
    let mut released = false;
    let mut acc = Vec::with_capacity(127);
    let mut overlong = false;
    loop {
        if TERMINATED.load(Ordering::Relaxed) {
            return g.finish("DEAD reason=guardian_signaled", true, &opts);
        }
        if g.broken {
            return g.finish("DEAD reason=parent_gone", true, &opts);
        }
        if expired() {
            return g.finish("DEAD reason=deadline", true, &opts);
        }
        let pr = poll(0, 50);
        if pr < 0 && errno() != libc::EINTR {
            g.broken = true;
        }
        if pr > 0 {
            let mut chunk = [0; 64];
            let n = read(0, &mut chunk);
            if n < 0 && errno() == libc::EINTR {
                continue;
            }
            if n <= 0 {
                return g.finish("DEAD reason=parent_gone", true, &opts);
            }
            for &b in &chunk[..n as usize] {
                if b != b'\n' {
                    if acc.len() < 127 {
                        acc.push(b);
                    } else {
                        overlong = true;
                    }
                    continue;
                }
                if overlong {
                    g.say("PROTOCOL_ERROR");
                } else if acc == b"GO" {
                    if expired() || fault("go_after_deadline") {
                        return g.finish("DEAD reason=deadline", true, &opts);
                    }
                    if !released {
                        if write_all(token[1], b"GO\n") {
                            close(token[1]);
                            released = true;
                            g.say("RELEASED");
                        } else {
                            g.finish("DEAD reason=release_failed", true, &opts);
                            return 3;
                        }
                    }
                } else if acc == b"TERM" {
                    return g.finish("DEAD reason=command", true, &opts);
                } else {
                    g.say("PROTOCOL_ERROR");
                }
                acc.clear();
                overlong = false;
            }
        }
        g.observe();
        if g.exit.is_some() {
            return g.finish("EXIT", false, &opts);
        }
    }
}
fn main() {
    std::process::exit(run());
}
