//! Kernel facts. Failure is unknown; only a documented disappearance is absence.
use libc::pid_t;

#[cfg(any(target_os = "linux", test))]
mod linux {
    use super::pid_t;
    use std::io::{self, Read};
    fn decimal(s: &str) -> Option<u64> {
        (!s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
            .then(|| s.parse().ok())
            .flatten()
    }
    pub fn entry_pid(s: &str) -> Option<pid_t> {
        (s.len() <= 10)
            .then(|| decimal(s))
            .flatten()
            .filter(|&n| n > 0 && n <= pid_t::MAX as u64)
            .map(|n| n as pid_t)
    }
    fn fields(line: &[u8], pid: pid_t) -> Option<Vec<&str>> {
        if line.is_empty() || line.len() >= 4095 {
            return None;
        }
        // comm is a kernel byte string, not UTF-8, and may itself contain ') '
        // or newlines. Only decode the pid and fields outside its delimiters.
        let open = line.windows(2).position(|w| w == b" (")?;
        let prefix = std::str::from_utf8(&line[..open]).ok()?;
        if decimal(prefix)? != pid as u64 {
            return None;
        }
        let rest = &line[open + 2..];
        let close = rest.windows(2).rposition(|w| w == b") ")?;
        let fields = std::str::from_utf8(&rest[close + 2..]).ok()?;
        Some(
            fields
                .split([' ', '\n'])
                .filter(|s| !s.is_empty())
                .collect(),
        )
    }
    pub fn identity(line: &[u8], pid: pid_t) -> Option<String> {
        Some(format!("ticks:{}", decimal(fields(line, pid)?.get(19)?)?))
    }
    fn group(line: &[u8], pid: pid_t) -> Option<pid_t> {
        let n = decimal(fields(line, pid)?.get(2)?)?;
        (n <= pid_t::MAX as u64).then_some(n as pid_t)
    }
    pub fn read_fact(reader: impl Read) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        reader.take(4095).read_to_end(&mut bytes)?;
        Ok(bytes)
    }
    // The same enumeration and read-error logic serves /proc and injected tests.
    pub fn count(
        entries: impl Iterator<Item = io::Result<String>>,
        mut read: impl FnMut(pid_t) -> io::Result<Vec<u8>>,
        pgid: pid_t,
    ) -> Option<usize> {
        let mut count = 0;
        for entry in entries {
            let name = entry.ok()?;
            let Some(pid) = entry_pid(&name) else {
                continue;
            };
            let bytes = match read(pid) {
                Ok(b) if b.is_empty() => continue,
                Ok(b) => b,
                Err(e) if matches!(e.raw_os_error(), Some(libc::ENOENT | libc::ESRCH)) => continue,
                Err(_) => return None,
            };
            if group(&bytes, pid)? == pgid {
                count += 1;
            }
        }
        Some(count)
    }
    #[cfg(target_os = "linux")]
    pub fn read_pid(pid: pid_t) -> io::Result<Vec<u8>> {
        read_fact(std::fs::File::open(format!("/proc/{pid}/stat"))?)
    }
    #[cfg(target_os = "linux")]
    pub fn members(pgid: pid_t) -> Option<usize> {
        let entries = std::fs::read_dir("/proc")
            .ok()?
            .map(|e| e.map(|e| e.file_name().to_string_lossy().into_owned()));
        count(entries, read_pid, pgid)
    }
    #[cfg(test)]
    mod tests {
        use super::*;
        fn stat(pid: &str, pgid: &str, ticks: &str) -> Vec<u8> {
            format!("{pid} (worker) S 1 {pgid}{} {ticks}\n", " 0".repeat(16)).into_bytes()
        }
        fn members(bytes: io::Result<Vec<u8>>) -> Option<usize> {
            let mut bytes = Some(bytes);
            count(
                [Ok("4242".into())].into_iter(),
                |_| bytes.take().unwrap(),
                4242,
            )
        }
        #[test]
        fn identity_rejects_wrong_pid_malformed_prefix_and_negative_ticks() {
            for bytes in [
                stat("999999", "4242", "-1"),
                stat("999999", "4242", "123"),
                stat("4242", "4242", "-1"),
                b"4242(worker) S 1 4242".to_vec(),
            ] {
                assert_eq!(identity(&bytes, 4242), None);
            }
            assert_eq!(
                identity(&stat("4242", "4242", "123"), 4242),
                Some("ticks:123".into())
            );
        }
        #[test]
        fn membership_rejects_signed_overflow_wrong_pid_and_malformed_facts() {
            for bytes in [
                stat("4242", "+4242", "123"),
                stat("4242", "99999999999", "123"),
                stat("999999", "4242", "123"),
                b"4242(worker) S 1 4242".to_vec(),
            ] {
                assert_eq!(members(Ok(bytes)), None);
            }
            assert_eq!(members(Ok(stat("4242", "4242", "123"))), Some(1));
            assert_eq!(members(Ok(stat("4242", "0", "123"))), Some(0));
        }
        #[test]
        fn arbitrary_comm_bytes_preserve_identity_and_membership() {
            for comm in [b"wo\xffrker".as_slice(), b"a) \xff\n(b".as_slice()] {
                let mut bytes = b"4242 (".to_vec();
                bytes.extend_from_slice(comm);
                bytes.extend_from_slice(format!(") S 1 4242{} 123\n", " 0".repeat(16)).as_bytes());
                assert_eq!(identity(&bytes, 4242), Some("ticks:123".into()));
                assert_eq!(members(Ok(bytes.clone())), Some(1));
                // An unrelated process with this name must not make the whole
                // /proc census unknown either.
                assert_eq!(
                    count([Ok("4242".into())].into_iter(), |_| Ok(bytes.clone()), 99),
                    Some(0)
                );
            }
        }
        #[test]
        fn read_and_enumeration_errors_are_unknown_but_disappearance_is_absence() {
            assert_eq!(
                members(Err(io::Error::from_raw_os_error(libc::EBADF))),
                None
            );
            assert_eq!(
                members(Err(io::Error::from_raw_os_error(libc::ENOENT))),
                Some(0)
            );
            assert_eq!(
                members(Err(io::Error::from_raw_os_error(libc::ESRCH))),
                Some(0)
            );
            assert_eq!(
                count(
                    [
                        Ok("4242".into()),
                        Err(io::Error::from_raw_os_error(libc::EIO))
                    ]
                    .into_iter(),
                    |_| Ok(stat("4242", "4242", "123")),
                    4242
                ),
                None
            );
            struct Broken;
            impl Read for Broken {
                fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                    Err(io::Error::from_raw_os_error(libc::EBADF))
                }
            }
            assert!(read_fact(Broken).is_err());
        }
        #[test]
        fn non_process_entries_are_ignored_and_reads_are_bounded() {
            for name in [
                "4242self",
                "99999999999999999999",
                "2147483648",
                "0",
                "4242 ",
            ] {
                assert_eq!(
                    count(
                        [Ok(name.into())].into_iter(),
                        |_| panic!("not a process entry"),
                        4242
                    ),
                    Some(0)
                );
            }
            let bytes = read_fact(&vec![b'x'; 5000][..]).unwrap();
            assert_eq!(bytes.len(), 4095);
            assert_eq!(identity(&bytes, 4242), None);
            assert_eq!(members(Ok(bytes)), None);
        }
    }
}

#[cfg(target_os = "macos")]
mod macos {
    use super::pid_t;
    // Darwin <sys/proc_info.h>, PROC_PIDTBSDINFO. These are fixed-width fields
    // from the public libproc ABI. Exact returned size and pid are verified.
    #[repr(C)]
    #[derive(Default)]
    struct BsdInfo {
        flags: u32,
        status: u32,
        xstatus: u32,
        pid: u32,
        ppid: u32,
        uid: u32,
        gid: u32,
        ruid: u32,
        rgid: u32,
        svuid: u32,
        svgid: u32,
        reserved: u32,
        comm: [u8; 16],
        name: [u8; 32],
        nfiles: u32,
        pgid: u32,
        jobc: u32,
        tdev: u32,
        tpgid: u32,
        nice: i32,
        start_sec: u64,
        start_usec: u64,
    }
    #[link(name = "proc")]
    unsafe extern "C" {
        fn proc_pidinfo(
            pid: i32,
            flavor: i32,
            arg: u64,
            buffer: *mut libc::c_void,
            size: i32,
        ) -> i32;
        fn proc_listpids(kind: u32, typeinfo: u32, buffer: *mut libc::c_void, size: i32) -> i32;
    }
    pub fn identity(pid: pid_t) -> Option<String> {
        let mut info = BsdInfo::default();
        let size = std::mem::size_of::<BsdInfo>() as i32;
        // SAFETY: properly aligned repr(C) storage with the exact public ABI size.
        let got = unsafe { proc_pidinfo(pid, 3, 0, (&mut info as *mut BsdInfo).cast(), size) };
        if got != size || info.pid != pid as u32 || info.start_usec > 999999 {
            return None;
        }
        Some(format!("{}.{:06}", info.start_sec, info.start_usec))
    }
    pub fn members(pgid: pid_t) -> Option<usize> {
        const PROC_PGRP_ONLY: u32 = 2;
        unsafe {
            *libc::__error() = 0;
        }
        let size = unsafe { proc_listpids(PROC_PGRP_ONLY, pgid as u32, std::ptr::null_mut(), 0) };
        if size < 0 || (size == 0 && super::super::errno() != 0) {
            return None;
        }
        // A completely filled buffer cannot prove enumeration was complete.
        let cap = usize::try_from(size).ok()?.checked_add(16)?;
        let mut pids = vec![0_i32; cap.div_ceil(4)];
        let bytes = i32::try_from(pids.len().checked_mul(4)?).ok()?;
        unsafe {
            *libc::__error() = 0;
        }
        let got =
            unsafe { proc_listpids(PROC_PGRP_ONLY, pgid as u32, pids.as_mut_ptr().cast(), bytes) };
        if got < 0 || (got == 0 && super::super::errno() != 0) || got >= bytes || got % 4 != 0 {
            return None;
        }
        // libproc uses zero for an empty result and for failure; errno distinguishes them.
        // The callers also require ESRCH independently before certifying settlement.
        Some(pids[..got as usize / 4].iter().filter(|&&p| p > 0).count())
    }
}
pub fn identity(pid: pid_t) -> Option<String> {
    if super::fault("identity_short") {
        return None;
    }
    #[cfg(target_os = "macos")]
    {
        macos::identity(pid)
    }
    #[cfg(target_os = "linux")]
    {
        linux::identity(&linux::read_pid(pid).ok()?, pid)
    }
}
pub fn members(pgid: pid_t) -> Option<usize> {
    if super::fault("members_fail") {
        return None;
    }
    #[cfg(target_os = "macos")]
    {
        macos::members(pgid)
    }
    #[cfg(target_os = "linux")]
    {
        linux::members(pgid)
    }
}
