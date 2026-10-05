//! The one claims-root lock (NS-15.G.005, B3a G2): a kernel flock on `<root>/.root-lock`, held by this process for
//! its caller over an Erlang Port.
//!
//! Usage: `root_lock ROOT WAIT_MS`. Lines on stdout, each newline-terminated:
//!   `contended <pid>`  once, after the first attempt actually found the lock held (EWOULDBLOCK)
//!   `acquired <pid>`   the lock is held until stdin delivers anything or closes
//!   `busy`             WAIT_MS passed without the lock; the helper exits 0
//!   `error <name>`     any other failure; the helper exits nonzero
//!
//! Every attempt is non-blocking. The first attempt is always made; every later attempt (including a retry after
//! EINTR) is made only after a check that the deadline has not passed, and only after a stdin wait that did not see
//! stdin end. Between attempts the helper waits on stdin for at most 10 ms, so a caller that dies or closes the port
//! ends the wait within one slice. An abandoned waiter can still acquire in the gap between its last stdin check and
//! its next attempt; it then holds only until its next stdin check, which sees the close, and exits. The kernel
//! releases the lock when the process exits, on every path including SIGKILL.

use std::ffi::CString;
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::process::exit;
use std::time::{Duration, Instant};

const POLL_SLICE_MS: u128 = 10;

fn say(line: &str) {
    let mut out = io::stdout().lock();
    // A failed write means the caller is gone; the next stdin wait ends the helper.
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

fn errno() -> i32 {
    io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn usage() -> ! {
    say("error usage");
    exit(2)
}

fn fail(code: i32) -> ! {
    say(&format!("error errno_{code}"));
    exit(1)
}

/// Waits on stdin for at most `timeout_ms` (-1 waits without bound). True when stdin delivered anything, reached
/// end of file or hung up: in every one of those cases the caller has ended this helper's work.
fn stdin_ended(timeout_ms: i32) -> bool {
    loop {
        let mut fds = libc::pollfd {
            fd: 0,
            events: libc::POLLIN,
            revents: 0,
        };
        let ready = unsafe { libc::poll(&mut fds, 1, timeout_ms) };

        if ready >= 0 {
            return ready > 0;
        }

        let code = errno();
        if code != libc::EINTR {
            fail(code)
        }
    }
}

fn hold() -> ! {
    stdin_ended(-1);
    exit(0)
}

fn main() {
    let args: Vec<std::ffi::OsString> = std::env::args_os().collect();
    if args.len() != 3 {
        usage()
    }

    let wait_ms: u64 = match args[2].to_str().and_then(|text| text.parse().ok()) {
        Some(value) => value,
        None => usage(),
    };

    let path = Path::new(&args[1]).join(".root-lock");
    let c_path = match CString::new(path.as_os_str().as_bytes()) {
        Ok(c_path) => c_path,
        Err(_) => usage(),
    };

    let flags = libc::O_CREAT | libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW;
    let fd = unsafe { libc::open(c_path.as_ptr(), flags, 0o600 as libc::c_uint) };
    if fd < 0 {
        fail(errno())
    }

    let pid = std::process::id();
    let deadline = Instant::now() + Duration::from_millis(wait_ms);
    let mut contended = false;

    loop {
        if unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            say(&format!("acquired {pid}"));
            hold()
        }

        let code = errno();
        if code != libc::EINTR && code != libc::EWOULDBLOCK {
            fail(code)
        }
        if code == libc::EWOULDBLOCK && !contended {
            contended = true;
            say(&format!("contended {pid}"));
        }

        let now = Instant::now();
        if now >= deadline {
            say("busy");
            exit(0)
        }

        // An EINTR retry waits on stdin like any other retry; only EWOULDBLOCK reports contention.
        let slice = (deadline - now).as_millis().min(POLL_SLICE_MS) as i32;
        if stdin_ended(slice) {
            exit(0)
        }
        if Instant::now() >= deadline {
            say("busy");
            exit(0)
        }
    }
}
