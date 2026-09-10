//! Kernel-level package checks, run by cargo test on each native Nix builder.
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
};
static SERIAL: AtomicUsize = AtomicUsize::new(0);
struct Session {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
    dir: std::path::PathBuf,
}
impl Session {
    fn start(command: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "orris-rust-protocol-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_gate_guardian"))
            .args([
                "--stdout",
                dir.join("out").to_str().unwrap(),
                "--stderr",
                dir.join("err").to_str().unwrap(),
                "--cwd",
                dir.to_str().unwrap(),
                "--settle-ms",
                "100",
                "--rounds",
                "2",
                "--timeout-ms",
                "2000",
                "--",
                "sh",
                "-c",
                command,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let input = child.stdin.take();
        let output = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            input,
            output,
            dir,
        }
    }
    fn line(&mut self) -> String {
        let mut s = String::new();
        self.output.read_line(&mut s).unwrap();
        s.trim_end().to_owned()
    }
    fn send(&mut self, text: &[u8]) {
        self.input.as_mut().unwrap().write_all(text).unwrap();
    }
    fn ready(&mut self) {
        let line = self.line();
        assert!(line.starts_with("READY "), "{line}");
        assert!(line.contains(" start="), "{line}");
    }
    fn exited(&mut self) {
        assert!(self.child.wait().unwrap().success());
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        // Closing the original control channel asks the guardian to settle its
        // own group. Keep output open and reap it before deleting the fixture.
        self.input.take();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.dir);
    }
}
#[test]
fn prepared_command_never_runs_on_parent_eof() {
    let mut s = Session::start("echo ran > marker");
    s.ready();
    assert!(!s.dir.join("marker").exists());
    s.input.take();
    let line = s.line();
    assert!(line.starts_with("DEAD reason=parent_gone"), "{line}");
    assert!(line.contains("settled=1 leftovers=0 proof=gone"), "{line}");
    s.exited();
    assert!(!s.dir.join("marker").exists());
}
#[test]
fn go_preserves_real_exit_status_and_private_output() {
    let mut s = Session::start("echo command-output; exit 7");
    s.ready();
    s.send(b"GO\n");
    assert_eq!(s.line(), "RELEASED");
    let line = s.line();
    assert!(line.starts_with("EXIT kind=exited status=7"), "{line}");
    assert!(line.contains("settled=1 leftovers=0 proof=gone"), "{line}");
    s.exited();
    assert_eq!(
        fs::read_to_string(s.dir.join("out")).unwrap(),
        "command-output\n"
    );
}
#[test]
fn signal_is_not_successful_exit_status() {
    let mut s = Session::start("kill -TERM $$");
    s.ready();
    s.send(b"GO\n");
    assert_eq!(s.line(), "RELEASED");
    let line = s.line();
    assert!(line.starts_with("EXIT kind=signaled signal=15"), "{line}");
    s.exited();
}
#[test]
fn malformed_control_cannot_release_and_term_settles_the_owned_group() {
    let mut s = Session::start("echo ran > marker; sleep 30 & wait");
    s.ready();
    s.send(b"GO\0\n");
    assert_eq!(s.line(), "PROTOCOL_ERROR");
    assert!(!s.dir.join("marker").exists());
    s.send(b"TERM\n");
    let line = s.line();
    assert!(line.starts_with("DEAD reason=command"), "{line}");
    assert!(line.contains("settled=1 leftovers=0 proof=gone"), "{line}");
    s.exited();
}
